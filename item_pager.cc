/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

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

class ItemPagingVisitor : public VBucketVisitor {
public:

    /**
     * Construct a PagingVisitor that will attempt to evict the given
     * percentage of objects.
     *
     * @param s the store that will handle the bulk removal
     * @param st the stats where we'll track what we've done
     * @param pcnt percentage of objects to attempt to evict (0-1)
     * @param sfin pointer to a bool to be set to true after run completes
     * @param pause flag indicating if PagingVisitor can pause between vbucket visits
     */
    ItemPagingVisitor(EventuallyPersistentStore *s, EPStats &st, double pcnt,
                  bool *sfin, bool pause = false)
        : store(s), stats(st), percent(pcnt), ejected(0),
          startTime(ep_real_time()), stateFinalizer(sfin),
          canPause(pause) {}

    void visit(StoredValue *v) {
        // Remember expired objects -- we're going to delete them.
        if (v->isExpired(startTime) && !v->isDeleted()) {
            expired.push_back(std::make_pair(currentBucket->getId(), v->getKey()));
            return;
        }

        double r = static_cast<double>(std::rand()) / static_cast<double>(RAND_MAX);
        if (percent >= r) {
            if (!v->eligibleForEviction()) {
                ++stats.numFailedEjects;
                return;
            }
            // Check if the key with its CAS value exists in the open or closed referenced
            // checkpoints.
            bool foundInCheckpoints =
                currentBucket->checkpointManager.isKeyResidentInCheckpoints(v->getKey(),
                                                                            v->getCas());
            if (!foundInCheckpoints && v->ejectValue(stats, currentBucket->ht)) {
                if (currentBucket->getState() == vbucket_state_replica) {
                    ++stats.numReplicaEjects;
                }
                ++ejected;
            }
        }
    }

    bool visitBucket(RCPtr<VBucket> vb) {
         update();
         return VBucketVisitor::visitBucket(vb);
    }

    void update() {
        stats.expired.incr(expired.size());

        store->deleteExpiredItems(expired);

        if (numEjected() > 0) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "Paged out %d values\n", numEjected());
        }

        if (!expired.empty()) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "Purged %d expired items\n", expired.size());
        }
        ejected = 0;
        expired.clear();
    }

    bool pauseVisitor() {
        size_t queueSize = stats.queue_size.get() + stats.flusher_todo.get();
        return canPause && queueSize >= MAX_PERSISTENCE_QUEUE_SIZE;
    }

    void complete() {
        update();
        if (stateFinalizer) {
            *stateFinalizer = true;
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
    double                     percent;
    size_t                     ejected;
    time_t                     startTime;
    bool                      *stateFinalizer;
    bool                       canPause;
};

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
        : store(s), stats(st), ejected(0), startTime(ep_real_time()), stateFinalizer(sfin),
          pauseMutations(false), evjob(ev) {
        if (evjob) {
            stats.itemAgeStartTime = startTime;
            ageHisto.reset();
            sizeHisto.reset();
            evjob->initRebuild();
        }
    }

    void visit(StoredValue *v) {
        // Remember expired objects -- we're going to delete them.
        if (!pauseMutations && v->isExpired(startTime) && !v->isDeleted()) {
            expired.push_back(std::make_pair(currentBucket->getId(), v->getKey()));
            return;
        } else if (evjob && !v->isDeleted() && v->isResident() && !v->isDirty() &&
                   v->valLength() > EvictionManager::getMinBlobSize()) {
            if (evjob->evictAge() &&
                evjob->evictItemByAge(evjob->evictAge(), v, currentBucket) == false) {
            } else {
                time_t age = startTime - ep_abs_time(v->getDataAge());
                ageHisto.add(age);
                sizeHisto.add(v->valLength());
                evjob->addEvictItem(v, currentBucket);
            }
        }
    }

    bool visitBucket(RCPtr<VBucket> vb) {
         update();
         return VBucketVisitor::visitBucket(vb);
    }

    void update() {
        stats.expired.incr(expired.size());

        store->deleteExpiredItems(expired);

        if (numEjected() > 0) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "Paged out %d values\n", numEjected());
        }

        if (!expired.empty()) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "Purged %d expired items\n", expired.size());
        }
        ejected = 0;
        expired.clear();
    }

    bool pauseVisitor() {
        size_t queueSize = stats.queue_size.get() + stats.flusher_todo.get();
        pauseMutations = queueSize >= MAX_PERSISTENCE_QUEUE_SIZE;
        return (pauseMutations && !evjob);
    }

    bool shouldContinue() {
        if (evjob) {
            return evjob->storeEvictItem();
        }
        return true;
    }

    void complete() {
        update();
        if (stateFinalizer) {
            *stateFinalizer = true;
        }
        if (evjob) {
            evjob->completeRebuild();
            // Copy out the histograms
            stats.itemAgeHisto.reset();
            Histogram<int>::iterator it = ageHisto.begin();
            for (; it != ageHisto.end(); ++it) {
                stats.itemAgeHisto.add((*it)->start(), (*it)->count());
            }
            stats.itemSizeHisto.reset();
            Histogram<size_t>::iterator it1 = sizeHisto.begin();
            for (; it1 != sizeHisto.end(); ++it1) {
                stats.itemSizeHisto.add((*it1)->start(), (*it1)->count());
            }

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
    time_t                     startTime;
    bool                      *stateFinalizer;
    bool                       pauseMutations;
    EvictionPolicy *evjob;
    Histogram<int> ageHisto;
    Histogram<size_t> sizeHisto;
};

bool ItemPager::callback(Dispatcher &d, TaskId t) {
    double current = static_cast<double>(StoredValue::getCurrentSize(stats));
    double upper = static_cast<double>(stats.mem_high_wat);
    double lower = static_cast<double>(stats.mem_low_wat);

    if (available && current > upper && 0) {

        ++stats.pagerRuns;

        double toKill = (current - static_cast<double>(lower)) / current;

        std::stringstream ss;
        ss << "Using " << StoredValue::getCurrentSize(stats)
           << " bytes of memory, paging out %0f%% of items." << std::endl;
        getLogger()->log(EXTENSION_LOG_INFO, NULL, ss.str().c_str(),
                         (toKill*100.0));

        available = false;
        shared_ptr<ItemPagingVisitor> pv(new ItemPagingVisitor(store, stats,
                                                       toKill, &available, false));
        store->visit(pv, "Item pager", &d, Priority::ItemPagerPriority);
        getLogger()->log(EXTENSION_LOG_INFO, NULL, "XXX: item_pager: Got called!!!"); 
    }

    d.snooze(t, 10);
    return true;
}

/*
 * This pager takes care of both evictions as well as expired items.
 * Runs if:
 * Eviction run is needed, or
 * Expired item frequency is hit
*/   

bool ExpiredItemPager::callback(Dispatcher &d, TaskId t) {
    if (available) {
        EvictionPolicy *policy = store->evictionBGJob();
        bool expiryNeeded = pagerRunNeeded();
        bool evictionNeeded = policy && policy->evictionRunNeeded(expiryNeeded);
        if (expiryNeeded || evictionNeeded) {
            lastRun = ep_real_time();
            available = false;
            shared_ptr<ExpiryPagingVisitor> pv(new ExpiryPagingVisitor(store, stats,
                                                   &available, 
                                                   evictionNeeded ? policy : NULL));
            store->visit(pv, "Expired item remover", &d, Priority::ItemPagerPriority,
                         true, 10);
        }
    }
    d.snooze(t, callbackFreq());
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
    if (store->getRWUnderlying()->delVBucket(vbid, vb_version, *rit)) {
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
