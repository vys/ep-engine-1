#include <eviction.hh>

double LRUPolicy::rebuildPercent = 0.5;
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

// Evict keys from memory to make room for 'size' bytes
bool EvictionManager::evictSize(size_t size)
{
    size_t cur = 0;

    while(cur < size) {
        EvictItem *ent = evpolicy->evict();
        if (ent == NULL) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL, "Eviction: Empty list, ejection failed.  Evicted only %uB out of a total %uB required.", cur, size);
            stats.evictionStats.numEmptyQueue++;
            return false;
        }
        std::string k;
        uint16_t b;
        k = ent->getKey();
        b = ent->vbucketId();

        RCPtr<VBucket> vb = store->getVBucket(b);
        int bucket_num;
        LockHolder lh = vb->ht.getLockedBucket(k, &bucket_num);
        StoredValue *v = vb->ht.unlocked_find(k, bucket_num, false);

        if (!v) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL, "Eviction: Key not present.");
            stats.evictionStats.failedTotal.numKeyNotPresent++;
        } else if (!v->eligibleForEviction()) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL, "Eviction: Key not eligible for eviction.");
            if (v->isResident() == false) {
                stats.evictionStats.failedTotal.numAlreadyEvicted++;
            } else if (v->isClean() == false) {
                stats.evictionStats.failedTotal.numDirties++;
            } else if (v->isDeleted() == false) {
                stats.evictionStats.failedTotal.numDeleted++;
            }
        } else if (!evpolicy->eligibleForEviction(v, ent)) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL, "Eviction: Key not eligible for eviction.");
            stats.evictionStats.failedTotal.numPolicyIneligible++;
        } else {
            bool inCheckpoint = vb->checkpointManager.isKeyResidentInCheckpoints(v->getKey(),
                                                                                 v->getCas());
            if (inCheckpoint) {
                stats.evictionStats.failedTotal.numInCheckpoints++;
            } else if (v->ejectValue(stats, vb->ht)) {
                cur += v->valLength(); 
                /* update stats for eviction that just happened */
                stats.evictionStats.numTotalKeysEvicted++;
                stats.evictionStats.numKeysEvicted++;
            }
        }
        delete ent;
    }

    return true;
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
        templist = new FixedList<LRUItem, LRUItemCompare>(maxSize);
        stats.evictionStats.memSize.incr(templist->memSize());
        startTime = ep_real_time();
    }
}

bool LRUPolicy::addEvictItem(StoredValue *v, RCPtr<VBucket> currentBucket) {
    BlockTimer timer(&timestats.visitHisto);
    stopBuild |= !(EvictionManager::getInstance()->enableJob());
    if (stopBuild) {
        return false;
    }
    LRUItem *item = new LRUItem(v, currentBucket->getId(), v->getDataAge());
    size_t size = templist->size();
    if (size && (size == maxSize) &&
            (lruItemCompare(*templist->last(), *item) < 0)) {
        delete item;
        return false;
    }
    item->increaseCurrentSize(stats);
    stage.push_front(item);
    // this assumes that three pointers are used per node of list
    stats.evictionStats.memSize.incr(3 * sizeof(int*));
    return true;
}

bool LRUPolicy::storeEvictItem() {
    BlockTimer timer(&timestats.storeHisto);
    stopBuild |= !(EvictionManager::getInstance()->enableJob());
    if (stopBuild) {
        return false;
    }
    std::list<LRUItem*> *l = templist->insert(stage);
    for (std::list<LRUItem*>::iterator iter = l->begin(); iter != l->end(); iter++) {
        LRUItem *item = *iter;
        item->reduceCurrentSize(stats);
        delete item;
    }
    delete l;
    clearStage();
    return true;
}

void LRUPolicy::completeRebuild() {
    BlockTimer timer(&timestats.completeHisto);
    stopBuild |= !(EvictionManager::getInstance()->enableJob());
    if (stopBuild) {
        clearTemplist();
        clearStage(true);
    } else {
        templist->build();
        curSize = templist->size();
        count.incr(curSize);
        genLruHisto();
        FixedList<LRUItem, LRUItemCompare>::iterator tempit = templist->begin();
        tempit = it.swap(tempit);
        while (tempit != list->end()) {
            LRUItem *item = tempit++;
            count--;
            item->reduceCurrentSize(stats);
            delete item;
        }
        stats.evictionStats.memSize.decr(templist->memSize());
        delete list;
        list = templist;
        templist = NULL;
    }
    assert(stage.size() == 0);
    endTime = ep_real_time();
    timestats.startTime = startTime;
    timestats.endTime = endTime;
}

void RandomPolicy::initRebuild() {
    stopBuild = !(EvictionManager::getInstance()->enableJob());
    if (!stopBuild) {
        templist = new RandomList();
        startTime = ep_real_time();
        stats.evictionStats.memSize.incr(sizeof(RandomList) + RandomList::nodeSize());
    }
}

bool RandomPolicy::addEvictItem(StoredValue *v,RCPtr<VBucket> currentBucket) {
    BlockTimer timer(&timestats.visitHisto);
    stopBuild |= !(EvictionManager::getInstance()->enableJob());
    if (stopBuild || size == maxSize) {
        return false;
    }
    EvictItem *item = new EvictItem(v, currentBucket->getId());
    item->increaseCurrentSize(stats);
    templist->add(item);
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
        RandomList::iterator tempit = templist->begin();
        queueSize = size;
        tempit = it.swap(tempit);
        EvictItem *node;
        while ((node = ++tempit) != NULL) {
            node->reduceCurrentSize(stats);
            stats.evictionStats.memSize.decr(RandomList::nodeSize());
            delete node;
        }
        stats.evictionStats.memSize.decr(sizeof(RandomList) + RandomList::nodeSize());
        delete list;
        list = templist;
        templist = NULL;
        size = 0;
    }
    endTime = ep_real_time();
    timestats.startTime = startTime;
    timestats.endTime = endTime;
}
