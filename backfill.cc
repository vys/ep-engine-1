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
#include "vbucket.hh"
#include "ep_engine.h"
#include "ep.hh"
#include "backfill.hh"
#include "kvstore-mapper.hh"

double BackFillVisitor::backfillResidentThreshold = DEFAULT_BACKFILL_RESIDENT_THRESHOLD;
size_t BackFillVisitor::backfillNumKeysThreshold = DEFAULT_BACKFILL_NUM_KEYS_THRESHOLD;
size_t BackfillDiskLoad::backfillMaxListSize = BACKFILL_MAX_LIST_SIZE;
size_t BackfillDiskLoad::kvSleepTime = 0;
std::set<int> BackfillDiskLoad::kvSleepEnable;

CallbackResult BackfillDiskLoad::callback(GetValue &gv) {
    // If a vbucket version of a bg fetched item is different from the current version,
    // skip this item.
    if (vbucket_version != gv.getVBucketVersion()) {
        delete gv.getValue();
        return CB_SUCCESS;
    }

    int nItems = connMap.numBackfilledItems(name, sessionID);
    if (nItems == -1) { // The connection is gone?
        delete gv.getValue();
        return CB_ABORT;
    } else if (nItems >= (int)backfillMaxListSize ||
               !EvictionManager::getInstance()->evictHeadroom()) {
        // Should the diskload be throttled?
        return CB_RETRY;
    } else {
        ReceivedItemTapOperation tapop(true);
        // if the tap connection is closed, then free an Item instance
        if (!connMap.performTapOp(name, sessionID, tapop, gv.getValue())) {
            delete gv.getValue();
            return CB_ABORT;
        }
    }

    NotifyPausedTapOperation notifyOp;
    connMap.performTapOp(name, sessionID, notifyOp, engine);
    return CB_SUCCESS;
}

bool BackfillDiskLoad::callback(Dispatcher &d, TaskId t) {
    bool valid = false;

    if (!EvictionManager::getInstance()->evictHeadroom()) {
         d.snooze(t, 1);
         return true;
    }

    std::list<queued_item> flushItems;
    engine->getEpStore()->getFlushItems(flushItems, kvId);
    engine->getEpStats().backfillFlushItems[kvId].incr(flushItems.size());

    // Walk the disk
    if (connMap.checkConnectivity(name) && !engine->getEpStore()->isFlushAllScheduled()) {
        store->dump(vbucket, *this, forceVBDump);
        valid = true;
    }

    // Must not sort
    size_t queueLength = 0;
    size_t queueMemSize = 0;
    std::list<queued_item>::iterator it;
    for (it = flushItems.begin(); it != flushItems.end(); ++it) {
        queueLength++;
        queueMemSize += (*it)->size();
    }
    engine->getEpStats().backfillFlushItems[kvId].decr(flushItems.size());
    connMap.setEvents(name, &flushItems, queueLength, queueMemSize);

    if (BackfillDiskLoad::kvSleepTime > 0) {
        if (BackfillDiskLoad::isKvSleepEnabled(kvId)) {
            sleep(BackfillDiskLoad::kvSleepTime);
        }
    }

    // Should decr the disk backfill counter regardless of the connectivity status
    CompleteDiskBackfillTapOperation op;
    valid = connMap.performTapOp(name, sessionID, op, static_cast<void*>(NULL));

    if (valid && connMap.checkBackfillCompletion(name)) {
        engine->notifyTapNotificationThread();
    }

    return false;
}

std::string BackfillDiskLoad::description() {
    std::stringstream rv;
    rv << "Loading TAP backfill from disk for vb " << vbucket
       << " on kvstore " << kvId
       << " under connection " << name;
    return rv.str();
}

bool BackFillVisitor::visitBucket(RCPtr<VBucket> vb) {
    apply();

    if (vBucketFilter(vb->getId())) {
        VBucketVisitor::visitBucket(vb);
        // If the current resident ratio for a given vbucket is below the resident threshold
        // for memory backfill only, schedule the disk backfill for more efficient bg fetches.
        double numItems = static_cast<double>(vb->ht.getNumItems());
        double numNonResident = static_cast<double>(vb->ht.getNumNonResidentItems());
        if (numItems == 0) {
            return true;
        }
        bool res = true;
        bool residentRatioBelowThreshold = (((numItems - numNonResident) / numItems) < backfillResidentThreshold);
        bool numKeysAboveThreshold = (numItems > backfillNumKeysThreshold);
        if (residentRatioBelowThreshold || numKeysAboveThreshold) {
            // Perform check on the relevant KVStores to see if each one
            // has a separate dispatcher for disk backfill (WAL mode)
            int beginId, endId;
            KVStoreMapper::getVBucketToKVId(vb->getId(), beginId, endId);
            bool vb0 = engine->getConfiguration().isVb0();
            bool useDiskBackfill = true;
            for (int kvId = beginId; kvId < endId; kvId++) {
                if ((!efficientVBDump[kvId] && !vb0) || !engine->getEpStore()->hasSeparateRODispatcher(kvId)) {
                    useDiskBackfill = false;
                    break;
                }
            }
            if (useDiskBackfill) {
                uint32_t flags;
                if (engine->tapConnMap.getConnectionFlags(name, flags)) {
                    if (flags & TAP_CONNECT_REQUEST_KEYS_ONLY) {
                        // Perform the regular hashtable walk since that will defer
                        // disk reads due to this flag anyway, hence running fast.
                        useDiskBackfill = false;
                    }
                } else {
                    res = false; // Connection lost
                }
            }
            if (useDiskBackfill) {
                vbuckets.push_back(vb->getId());
                ScheduleDiskBackfillTapOperation tapop(endId - beginId);
                engine->tapConnMap.performTapOp(name, sessionID, tapop, static_cast<void*>(NULL));
                res = false; // Don't need a hashtable walk for this vbucket
            }
        }
        // When the backfill is scheduled for a given vbucket, set the TAP cursor to
        // the beginning of the open checkpoint.
        engine->tapConnMap.SetCursorToOpenCheckpoint(name, vb->getId());
        return res;
    }
    return false;
}

void BackFillVisitor::visit(StoredValue *v) {
    std::string k = v->getKey();
    queued_item qi(new QueuedItem(k, currentBucket->getId(), queue_op_set, -1, v->getId()));
    int kvid = KVStoreMapper::getKVStoreId(k, currentBucket);
    uint16_t shardId = engine->kvstore[kvid]->getShardId(qi->getKey(), qi->getVBucketId());
    found.push_back(std::make_pair(shardId, qi));
}

void BackFillVisitor::apply(void) {
    std::vector<uint16_t>::iterator it = vbuckets.begin();
    for (; it != vbuckets.end(); it++) {
        int beginId, endId;
        KVStoreMapper::getVBucketToKVId(*it, beginId, endId);
        for (int kvid = beginId; kvid < endId; kvid++) {
            Dispatcher *d(engine->epstore->getROBackfillDispatcher(kvid));
            KVStore *underlying(engine->epstore->getROBackfillUnderlying(kvid));
            assert(d);
            shared_ptr<DispatcherCallback> cb(new BackfillDiskLoad(name,
                        engine,
                        engine->tapConnMap,
                        underlying,
                        *it,
                        validityToken,
                        sessionID,
                        kvid,
                        !efficientVBDump[kvid]));
            d->schedule(cb, NULL, Priority::TapBgFetcherPriority);
        }
    }
    vbuckets.clear();

    setEvents();
}

void BackFillVisitor::setResidentItemThreshold(double residentThreshold) {
    backfillResidentThreshold = residentThreshold;
}

double BackFillVisitor::getResidentItemThreshold() {
    return backfillResidentThreshold;
}

void BackFillVisitor::setNumKeysThreshold(size_t numKeysThreshold) {
    backfillNumKeysThreshold = numKeysThreshold;
}

size_t BackFillVisitor::getNumKeysThreshold() {
    return backfillNumKeysThreshold;
}

void BackfillDiskLoad::setMaxListSize(size_t maxListSize) {
    backfillMaxListSize = maxListSize;
}

size_t BackfillDiskLoad::getMaxListSize() {
    return backfillMaxListSize;
}

void BackfillDiskLoad::setKvSleepTime(size_t sleepTime) {
    kvSleepTime = sleepTime;
}

size_t BackfillDiskLoad::getKvSleepTime() {
    return kvSleepTime;
}

void BackfillDiskLoad::setKvSleepEnable(int kvId, bool enable) {
    if (enable) {
        kvSleepEnable.insert(kvId);
    } else {
        kvSleepEnable.erase(kvId);
    }
}

bool BackfillDiskLoad::isKvSleepEnabled(int kvId) {
    return (kvSleepEnable.find(kvId) != kvSleepEnable.end());
}

void BackFillVisitor::setEvents() {
    if (checkValidity()) {
        if (!found.empty()) {
            // Don't notify unless we've got some data..
            TaggedQueuedItemComparator<uint16_t> comparator;
            std::sort(found.begin(), found.end(), comparator);

            std::vector<std::pair<uint16_t, queued_item> >::iterator it(found.begin());
            for (; it != found.end(); ++it) {
                queue->push_back(it->second);
                queueLength++;
                queueMemSize += it->second->size();
            }
            found.clear();
            engine->tapConnMap.setEvents(name, queue, queueLength, queueMemSize);
        }
    }
}

bool BackFillVisitor::pauseVisitor() {
    bool pause(true);

    ssize_t theSize(engine->tapConnMap.backfillQueueDepth(name));
    if (!checkValidity() || theSize < 0) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "TapProducer %s went away.  Stopping backfill.\n",
                         name.c_str());
        valid = false;
        return false;
    }

    pause = theSize > maxBackfillSize || !EvictionManager::getInstance()->evictHeadroom();

    if (pause) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "Tap queue depth too big for %s or memory usage too high, sleeping\n",
                         name.c_str());
    }
    return pause;
}

void BackFillVisitor::complete() {
    apply();
    CompleteBackfillTapOperation tapop;
    engine->tapConnMap.performTapOp(name, sessionID, tapop, static_cast<void*>(NULL));
    if (engine->tapConnMap.checkBackfillCompletion(name)) {
        engine->notifyTapNotificationThread();
    }
    releaseEngineResources();
}

bool BackFillVisitor::checkValidity() {
    if (valid) {
        valid = engine->tapConnMap.checkConnectivity(name);
        if (!valid) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Backfilling connectivity for %s went invalid. Stopping backfill.\n",
                             name.c_str());
        }
    }
    return valid;
}

bool BackfillTask::callback(Dispatcher &d, TaskId t) {
    (void) t;
    epstore->visit(bfv, "Backfill task", &d, Priority::BackfillTaskPriority, true, 1);
    return false;
}
