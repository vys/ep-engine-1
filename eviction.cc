#include <eviction.hh>

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
        delete ent;
        count--;

        RCPtr<VBucket> vb = store->getVBucket(b);
        int bucket_num(0);
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
    }

    return true;
}

// Periodic check to set policy and queue size due to config change
// Return policy if it needs to run as a background job.
EvictionPolicy *EvictionManager::evictionBGJob(void)
{
    if (pauseJob) {
        return NULL;
    }

    if (policyName != evpolicy->description()) {
        EvictionPolicy *p = EvictionPolicyFactory::getInstance(policyName, store, stats, maxSize);
        if (p) {
            delete evpolicy;
            evpolicy = p;
        }
    }
    evpolicy->setSize(maxSize);
    if (evpolicy->backgroundJob) { 
        return evpolicy; 
    } else { 
        return NULL; 
    }
}

void LRUPolicy::initRebuild() {
    buildDead = !(store->getEvictionManager()->enableJob());
    if (!buildDead) {
        templist = new FixedList<LRUItem, LRUItemCompare>(maxSize);
        stats.evictionStats.memSize.incr(templist->memSize());
        startTime = ep_real_time();
    }
}

bool LRUPolicy::addEvictItem(StoredValue *v, RCPtr<VBucket> currentBucket) {
    BlockTimer timer(&timestats.visitHisto);
    if (buildDead || !(store->getEvictionManager()->enableJob())) {
        buildDead = true;
        return false;
    }
    LRUItem *item = new LRUItem(v, currentBucket->getId(), v->getDataAge());
    item->increaseCurrentSize(stats);
    size_t __size = templist->size();
    if (__size && (__size == maxSize) &&
            (lruItemCompare(*templist->last(), *item) < 0)) {
        return false;
    }
    stage.push_front(item);
    // this assumes that three pointers are used per node of list
    stats.evictionStats.memSize.incr(3 * sizeof(int*));
    return true;
}

bool LRUPolicy::storeEvictItem() {
    BlockTimer timer(&timestats.storeHisto);
    if (buildDead || !(store->getEvictionManager()->enableJob())) {
        buildDead = true;
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
    if (buildDead || !(store->getEvictionManager()->enableJob())) {
        clearTemplist();
    } else {
        templist->build();
        genLruHisto();
        FixedList<LRUItem, LRUItemCompare>::iterator tempit = templist->begin();
        tempit = it.swap(tempit);
        while (tempit != list->end()) {
            LRUItem *item = tempit++;
            item->reduceCurrentSize(stats);
            delete item;
        }
        stats.evictionStats.memSize.decr(templist->memSize());
        delete list;
        list = templist;
        templist = NULL;
    }
    clearStage();
    endTime = ep_real_time();
    timestats.startTime = startTime;
    timestats.endTime = endTime;
}

void RandomPolicy::initRebuild() {
    buildDead = !(store->getEvictionManager()->enableJob());
    if (!buildDead) {
        templist = new RandomList();
        startTime = ep_real_time();
        stats.evictionStats.memSize.incr(sizeof(RandomList) + RandomList::nodeSize());
    }
}

bool RandomPolicy::addEvictItem(StoredValue *v,RCPtr<VBucket> currentBucket) {
    BlockTimer timer(&timestats.visitHisto);
    if (buildDead || !(store->getEvictionManager()->enableJob())) {
        buildDead = true;
        return false;
    }
    if (size == maxSize) {
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
    if (buildDead || !(store->getEvictionManager()->enableJob())) {
        buildDead = true;
        return false;
    }
    if (size < maxSize) { // shouldn't this be <=
        return true;
    }
    return false;
}

void RandomPolicy::completeRebuild() {
    BlockTimer timer(&timestats.completeHisto);
    if (buildDead || !(store->getEvictionManager()->enableJob())) {
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
