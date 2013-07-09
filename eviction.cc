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
#include "eviction.hh"
#include "rss.hh"

double LRUPolicy::rebuildPercent = 0.5;
double LRUPolicy::memThresholdPercent = 0.5;
Atomic<size_t> EvictionManager::minBlobSize = 5;
EvictionManager *EvictionManager::managerInstance = NULL;

// Periodic check to set policy and queue size due to config change
// Return policy if it needs to run as a background job.
EvictionPolicy *EvictionManager::evictionBGJob(void) {
    if (pauseJob) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL, "Eviction: Job has been stopped");
        return NULL;
    }

    if (policyName != evpolicy->description()) {
        EvictionPolicy *p = EvictionPolicyFactory::getInstance(policyName, store, stats, maxSize);
        if (p) {
            delete evpolicy;
            evpolicy = p;
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Eviction: Switching policy to %s", evpolicy->description().c_str());
        }
    }
    evpolicy->setSize(maxSize);
    if (evpolicy->backgroundJob) {
        evpolicy->evictAge(pruneAge);
        if (pruneAge != 0) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Eviction: Pruning keys that are older than %llu", pruneAge);
            stats.pruneStats.numPruneRuns++;
        }
        pruneAge = 0;
        return evpolicy;
    } else {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL, "Eviction: No background job");
        return NULL;
    }
}

/**
 * Checks if process RSS is above RSS threshold and if so, tries to evict
 * keys to bring the RSS down.
 *
 * This function should be called by all frontend threads before any operation
 * to decide if it should let the operation proceed or fail temporarily.
 *
 * Returns: true - caller can proceed with current operation as memory is within limit.
 *          false - caller should not proceed with current operation as we are temporarily out of memory.
 */
bool EvictionManager::evictHeadroom()
{
    size_t RSSThreshold = stats.maxDataSize - headroom;
    size_t currentRSS = GetSelfRSS();

    if (currentRSS < RSSThreshold) {
        return true;
    }

    if (disableInlineEviction || !evpolicy->supportsInlineEviction) {
        return allowOps(currentRSS);
    }

    size_t quantumSize = getEvictionQuantumSize();
    size_t quantumCount = getEvictionQuantumMaxCount();



    // When the allocated memory is over maxSize less a quantum padding, we return false
    // to wait till the allocator releases memory to the system and RSS comes down.
    do {
        if (pauseEvict) {
            if (ep_current_time() > (lastEvictTime + getEvictionQuietPeriod())) {
                getLogger()->log(EXTENSION_LOG_INFO, NULL, "pauseEvict timed out. lastEvictTime=%zu", lastEvictTime);
                continue;
            }
            if (currentRSS < lastRSSTarget) {
                getLogger()->log(EXTENSION_LOG_INFO, NULL, "pauseEvict can be reset. currentRSS=%zu < lastRSSTarget=%zu", currentRSS, lastRSSTarget);
                continue;
            }
            stats.evictionStats.failedTotal.evictionStopped++;
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "pauseEvict=true. currentRSS=%zu > lastRSSTarget=%zu. lastEvictTime=%zu. Denying request", currentRSS, lastRSSTarget, lastEvictTime);
            return false;
        }
    } while (0); // Using do-while trickery to be able to use continue inside if-condition above.

    // Attempt eviction only when the lock is available, otherwise return immediately
    bool lock;
    LockHolder lhe(evictionLock, &lock);
    if (!lock) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Unable to get eviction lock, returning");
        return allowOps(currentRSS);
    }

    getLogger()->log(EXTENSION_LOG_DETAIL, NULL, "Got lock, Resetting pauseEvict");
    pauseEvict = false;

    bool queueEmpty = false;
    size_t total = 0;
    size_t attempts = 0;

    lastRSSTarget = currentRSS - (quantumCount * quantumSize / 2);

    do {
        size_t cur = 0;

        while (cur < quantumSize) {
            EvictItem *evictItem = evpolicy->getOneEvictItem();
            if (evictItem == NULL) {
                queueEmpty = true;
                break;
            }
            std::string key = evictItem->getKey();

            RCPtr<VBucket> vb = store->getVBucket(evictItem->vbucketId());
            int bucket_num;
            LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
            StoredValue *v = vb->ht.unlocked_find(key, bucket_num, false);

            if (!v) {
                stats.evictionStats.failedTotal.numKeyNotPresent++;
            } else if (!v->eligibleForEviction()) {
                if (v->isResident() == false) {
                    stats.evictionStats.failedTotal.numAlreadyEvicted++;
                } else if (v->isClean() == false) {
                    stats.evictionStats.failedTotal.numDirties++;
                } else if (v->isDeleted() == false) {
                    // this shoud never occurs
                    assert("v is not eligible for eviction but is already deleted!");
                }
            } else if (!evpolicy->eligibleForEviction(v, evictItem)) {
                getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Eviction: Key not eligible for eviction.");
                stats.evictionStats.failedTotal.numPolicyIneligible++;
            } else {
                // Fail if key is present in any checkpoint
                bool inCheckpoint = vb->checkpointManager.isKeyResidentInCheckpoints(v->getKey(), -1);
                if (inCheckpoint) {
                    stats.evictionStats.failedTotal.numInCheckpoints++;
                } else if (v->ejectValue(stats, vb->ht)) {
                    if (enableEvictionHistograms) {
                        stats.evictionStats.evictItemAges.add(ep_current_time() - v->getDataAge());
                    }
                    cur += v->valLength();
                    /* update stats for eviction that just happened */
                    stats.evictionStats.numTotalKeysEvicted++;
                    stats.evictionStats.numKeysEvicted++;
                }
            }
        }

        total += cur;
        currentRSS = GetSelfRSS();
    } while (!queueEmpty && attempts++ < quantumCount && currentRSS > RSSThreshold);

    if (currentRSS > RSSThreshold) {
        if (queueEmpty) {
            getLogger()->log(EXTENSION_LOG_DETAIL, NULL, "Eviction: Empty activeList, ejection failed. Evicted %uB in an attempt to get allocated memory to %uB.", total, RSSThreshold);
            stats.evictionStats.numEmptyQueue++;
        } else { // attempts == quantumCount
            getLogger()->log(EXTENSION_LOG_DETAIL, NULL, "Eviction: Empty activeList, ejection failed. Quantum maximum count reached, evicted %uB.", total);
            stats.evictionStats.numMaxQuanta++;
        }
    }

    if (!allowOps(currentRSS) && attempts >= quantumCount) {
        pauseEvict = true;
        lastEvictTime = ep_current_time();
    }
    return allowOps(currentRSS);
}

// Evict key if it is older than the timestamp
bool EvictionPolicy::evictItemByAge(time_t timestamp, StoredValue *v, RCPtr<VBucket> vb) {
    time_t keyAge = ep_abs_time(v->getDataAge());
    assert(timestamp != 0);

    if (keyAge >= timestamp) {
        return false;
    } else if (v->ejectValue(stats, vb->ht)) {
        stats.pruneStats.numKeysPruned++;
        return true;
    }
    return false;
}

void LRUPolicy::initRebuild() {
    stopBuild = !(EvictionManager::getInstance()->enableJob());
    if (!stopBuild) {
        startTime = ep_real_time();
        if (maxSize != tempList->getMaxSize()) {
            LRUFixedList *newTempList = new LRUFixedList(maxSize);
            stats.evictionStats.memSize.incr(newTempList->memSize());

            // Move as many items as possible to the new templist
            size_t numInserted = 0, oldSize = tempList->getMaxSize();
            LRUItem **array = tempList->getArray();
            LRUItem *item;
            for (size_t i = 0; i <= oldSize; i++) {
                item = array[i];
                if (item != NULL) {
                    if (numInserted < maxSize) {
                        newTempList->insert(item);
                        numInserted++;
                    } else {
                        item->reduceCurrentSize(stats);
                        delete item;
                    }
                }
            }
            // Allocate the rest
            for ( ; numInserted < maxSize; numInserted++) {
                item = new LRUItem(0);
                item->increaseCurrentSize(stats);
            }
            newTempList->reset();

            stats.evictionStats.memSize.decr(tempList->memSize());
            delete tempList;
            tempList = newTempList;
        }
        stageIter = stage.begin();
    }
}

bool LRUPolicy::addEvictItem(StoredValue *v, RCPtr<VBucket> currentBucket) {
    stopBuild |= !(EvictionManager::getInstance()->enableJob());
    if (stopBuild) {
        return false;
    }
    time_t t = v->getDataAge();
    if (!freshStart && t > oldest && t < newest) {
        return false;
    }
    LRUItem item(v, currentBucket->getId(), t);
    size_t size = tempList->size();
    if (size && (size == maxSize) &&
            (lruItemCompare(*tempList->last(), item) < 0)) {
        return false;
    }
    if (stageIter == stage.end()) {
        stage.push_back(NULL);
        // this assumes that three pointers are used per node of activeList
        stats.evictionStats.memSize.incr(3 * sizeof(int*));
        stageIter--;
    }
    if (*stageIter == NULL) {
        *stageIter = new LRUItem(item);
    } else {
        (*stageIter)->reduceCurrentSize(stats);
        **stageIter = item;
    }
    (*stageIter)->increaseCurrentSize(stats);
    stageIter++;
    return true;
}

bool LRUPolicy::storeEvictItem() {
    stopBuild |= !(EvictionManager::getInstance()->enableJob());
    if (stopBuild) {
        return false;
    }
    tempList->insert(stage, stageIter);
    stageIter = stage.begin();
    return true;
}

void LRUPolicy::completeRebuild() {
    BlockTimer timer(&timestats.completeHisto);
    stopBuild |= !(EvictionManager::getInstance()->enableJob());
    if (stopBuild) {
        tempList->reset();
    } else {
        tempList->build();

        if (tempList->size()) {
            oldest = tempList->first()->getAttr();
            newest = tempList->last()->getAttr();
        } else {
            oldest = newest = 0;
        }

        genLruHisto();

        LockHolder lh(swapLock);
        LRUFixedList* tmp = inactiveList;  
        inactiveList = tempList;
        tempList = tmp;
        stats.evictionStats.numInactiveListItems = inactiveList->size();
        lh.unlock();
        tempList->reset();

        if (*it == NULL || freshStart) {
            bool switched = switchActiveListIter(true);
            if (switched) {
                stats.evictionStats.backgroundSwaps++;
            }
        }
    }
    endTime = ep_real_time();
    timestats.startTime = startTime;
    timestats.endTime = endTime;
}

void RandomPolicy::initRebuild() {
    stopBuild = !(EvictionManager::getInstance()->enableJob());
    if (!stopBuild) {
        tempList = new RandomList();
        startTime = ep_real_time();
        stats.evictionStats.memSize.incr(sizeof(RandomList) + RandomList::nodeSize());
    }
}

bool RandomPolicy::addEvictItem(StoredValue *v,RCPtr<VBucket> currentBucket) {
    stopBuild |= !(EvictionManager::getInstance()->enableJob());
    if (stopBuild || size == maxSize) {
        return false;
    }
    EvictItem *item = new EvictItem(v, currentBucket->getId());
    item->increaseCurrentSize(stats);
    tempList->add(item);
    stats.evictionStats.memSize.incr(RandomList::nodeSize());
    size++;
    return true;
}

bool RandomPolicy::storeEvictItem() {
    stopBuild |= !(EvictionManager::getInstance()->enableJob());
    if (stopBuild || size > maxSize) {
        return false;
    }
    return true;
}

void RandomPolicy::completeRebuild() {
    BlockTimer timer(&timestats.completeHisto);
    stopBuild |= !(EvictionManager::getInstance()->enableJob());
    if (stopBuild) {
        clearTemplist();
    } else {
        RandomList::iterator tempit = tempList->begin();
        queueSize = size;
        tempit = it.swap(tempit);
        EvictItem *node;
        while ((node = ++tempit) != NULL) {
            node->reduceCurrentSize(stats);
            stats.evictionStats.memSize.decr(RandomList::nodeSize());
            delete node;
        }
        stats.evictionStats.memSize.decr(sizeof(RandomList) + RandomList::nodeSize());
        delete activeList;
        activeList = tempList;
        tempList = NULL;
        size = 0;
    }
    endTime = ep_real_time();
    timestats.startTime = startTime;
    timestats.endTime = endTime;
}
