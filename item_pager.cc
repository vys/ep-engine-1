/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/*
 *   Copyright 2013 Zynga inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "config.h"
#include <iostream>
#include <cstdlib>
#include <utility>
#include <list>

#include "common.hh"
#include "item_pager.hh"
#include "ep.hh"

static const double threshold = 75.0;
static const size_t MAX_PERSISTENCE_QUEUE_SIZE = 1000000;

/**
 * Handle expired items and eviction based on the policies.
 */
class ExpiryPagingVisitor : public VBucketVisitor {
public:

    /**
     * @param s the store that will handle the bulk removal
     * @param st the stats where we'll track what we've done
     * @param sfin pointer to a bool to be set to true after run completes
     * @param pause flag indicating if PagingVisitor can pause between vbucket visits
     */
    ExpiryPagingVisitor(EventuallyPersistentStore *s, EPStats &st,
                  bool *sfin, EvictionPolicy *ev = NULL)
        : store(s), stats(st), ejected(0), expiredCount(0),
          startTime(ep_real_time()), stateFinalizer(sfin),
          pauseMutations(false), evjob(ev) {
        if (evjob) {
            evjob->initRebuild();
        }
        stats.itemAgeHisto[1]->reset();
        stats.diskItemSizeHisto[1]->reset();
        stats.memItemSizeHisto[1]->reset();
        stats.expiryPagerTimeStats.reset();
    }

    void visit(StoredValue *v) {
        // Remember expired objects -- we're going to delete them. (disable collection
        // of expired items in slave)
        if (!CheckpointManager::isInconsistentSlaveCheckpoint() &&
                currentBucket->getState() == vbucket_state_active &&
                !pauseMutations &&
                v->isExpired(startTime) && !v->isDeleted()) {
            expired.push_back(std::make_pair(currentBucket->getId(), v->getKey()));
            return;
        } else if (evjob && !v->isDeleted() && v->isResident() && !v->isDirty() &&
                   v->valLength() >= EvictionManager::getMinBlobSize()) {
            if (evjob->evictAge() &&
                evjob->evictItemByAge(evjob->evictAge(), v, currentBucket)) {
            } else {
                evjob->addEvictItem(v, currentBucket);
            }
        }
        // Record item sizes and age
        if (v->isResident()) {
            stats.memItemSizeHisto[1]->add(v->valLength());
        } else {
            stats.diskItemSizeHisto[1]->add(v->valLength());
        }
        stats.itemAgeHisto[1]->add(ep_current_time() - v->getDataAge());
    }

    bool visitBucket(RCPtr<VBucket> vb) {
         update();
         return VBucketVisitor::visitBucket(vb);
    }

    void update() {
        size_t count = expired.size();
        expiredCount += count;
        stats.expired.incr(count);

        store->deleteExpiredItems(expired);

        if (numEjected() > 0) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "Paged out %d values\n", numEjected());
        }

        ejected = 0;
        expired.clear();
    }

    bool pauseVisitor() {
        size_t queueSize = stats.queue_size.get() + stats.flusher_todo_get();
        pauseMutations = queueSize >= MAX_PERSISTENCE_QUEUE_SIZE;
        return false;
    }

    bool shouldContinue() {
        update();
        if (evjob) {
            return evjob->storeEvictItem();
        }
        return !pauseMutations;
    }

    void complete() {
        update();
        if (stateFinalizer) {
            *stateFinalizer = true;
        }
        if (evjob) {
            evjob->completeRebuild();
        }

        scrub_memory();

        // Swap out the histograms
        stats.itemAgeHisto[1] = stats.itemAgeHisto[0].swap(stats.itemAgeHisto[1]);
        stats.memItemSizeHisto[1] = stats.memItemSizeHisto[0].swap(stats.memItemSizeHisto[1]);
        stats.diskItemSizeHisto[1] = stats.diskItemSizeHisto[0].swap(stats.diskItemSizeHisto[1]);

        endTime = ep_real_time();
        stats.expiryPagerTimeStats.startTime = startTime;
        stats.expiryPagerTimeStats.endTime = endTime;

        if (expiredCount) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "Purged %zu expired items\n", expiredCount);
        }
    }
    
    /**
     * Get the number of items ejected during the visit.
     */
    size_t numEjected() { return ejected; }

private:
    std::list<std::pair<uint16_t, std::string> > expired;

    EventuallyPersistentStore *store;
    EPStats                   &stats;
    size_t                     ejected;
    size_t                     expiredCount;
    time_t                     startTime;
    time_t                     endTime;
    bool                      *stateFinalizer;
    bool                       pauseMutations;
    EvictionPolicy            *evjob;
};

/*
 * This pager takes care of both evictions as well as expired items.
 * Runs if:
 * Eviction run is needed, or
 * Expired item frequency is hit
*/   

bool ExpiredItemPager::callback(Dispatcher &d, TaskId t) {
    if (available) {
        EvictionPolicy *policy = EvictionManager::getInstance()->evictionBGJob();
        bool expiryNeeded = pagerRunNeeded();
        bool evictionNeeded = policy && policy->evictionJobNeeded(lruSleepTime);
        if (expiryNeeded || evictionNeeded) {
            stats.expiryPagerRuns++;
            lastRun = ep_real_time();
            available = false;
            shared_ptr<ExpiryPagingVisitor> pv(new ExpiryPagingVisitor(store, stats,
                                                   &available, 
                                                   evictionNeeded ? policy : NULL
                                                   ));
            store->visit(pv, "Expired item remover", &d, Priority::ItemPagerPriority,
                         true, 10);
        }
    }
    d.snooze(t, static_cast<double>(callbackFreq()));
    return true;
}

void InvalidItemDbPager::addInvalidItem(Item *item, uint16_t vbucket_version) {
    uint16_t vbucket_id = item->getVBucketId();
    std::map<uint16_t, uint16_t>::iterator version_it = vb_versions.find(vbucket_id);
    if (version_it == vb_versions.end() || version_it->second < vbucket_version) {
        vb_versions[vbucket_id] = vbucket_version;
    }

    std::map<uint16_t, std::vector<int64_t>* >::iterator item_it = vb_items.find(vbucket_id);
    if (item_it != vb_items.end()) {
        item_it->second->push_back(item->getId());
    } else {
        std::vector<int64_t> *item_list = new std::vector<int64_t>(chunk_size * 5);
        item_list->push_back(item->getId());
        vb_items[vbucket_id] = item_list;
    }
}

void InvalidItemDbPager::createRangeList() {
    std::map<uint16_t, std::vector<int64_t>* >::iterator vbit;
    for (vbit = vb_items.begin(); vbit != vb_items.end(); vbit++) {
        std::sort(vbit->second->begin(), vbit->second->end());
        std::list<row_range> row_range_list;
        createChunkListFromArray<int64_t>(vbit->second, chunk_size, row_range_list);
        vb_row_ranges[vbit->first] = row_range_list;
        delete vbit->second;
    }
    vb_items.clear();
}

bool InvalidItemDbPager::callback(Dispatcher &d, TaskId t) {
    BlockTimer timer(&stats.diskInvaidItemDelHisto);
    std::map<uint16_t, std::list<row_range> >::iterator it = vb_row_ranges.begin();
    if (it == vb_row_ranges.end()) {
        stats.dbCleanerComplete.set(true);
        return false;
    }

    std::list<row_range>::iterator rit = it->second.begin();
    uint16_t vbid = it->first;
    uint16_t vb_version = vb_versions[vbid];
    if (store->getRWUnderlying(0)->delVBucket(vbid, vb_version, *rit)) {
        it->second.erase(rit);
        if (it->second.begin() == it->second.end()) {
            vb_row_ranges.erase(it);
        }
    }
    else {
        d.snooze(t, 10);
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "Reschedule to delete the old chunk of vbucket %d with",
                         " the version %d from disk\n",
                         vbid, vb_version);
    }
    return true;
}
