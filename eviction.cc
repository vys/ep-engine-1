#include <eviction.hh>

// Evict keys from memory to make room for 'size' bytes
bool EvictionManager::evictSize(size_t size)
{
    size_t cur = 0;

    if (pauseEviction) { return false; }

    while(cur < size) {
        EvictItem *ent = evpolicy->evict();
        if (ent == NULL) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL, "Eviction: Empty list, ejection failed.  Evicted only %udB out of a total %udB required.", cur, size);
            stats.evictStats.numEmptyQueue++;
            return false;
        }
        std::string k;
        uint16_t b;
        k = ent->getKey();
        b = ent->get_vbucket_id();
        ent->reduceCurrentSize(stats);
        delete ent;
        count--;

        RCPtr<VBucket> vb = store->getVBucket(b);
        int bucket_num(0);
        LockHolder lh = vb->ht.getLockedBucket(k, &bucket_num);
        StoredValue *v = vb->ht.unlocked_find(k, bucket_num, false);

        if (!v) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL, "Eviction: Key not present.");
            stats.evictStats.failedTotal.numKeyNotPresent++;
    //        failedstats.numKeyNotPresent++;
        } else if (!v->ejectValue(stats, vb->ht)) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL, "Eviction: Key not eligible for eviction.");
            if (v->isResident() == false) {
                stats.evictStats.failedTotal.numAlreadyEvicted++;
      //          failedstats.numAlreadyEvicted++;
            } else if (v->isClean() == false) {
                stats.evictStats.failedTotal.numDirties++;
        //        failedstats.numDirties++;
            } else if (v->isDeleted() == false) {
                stats.evictStats.failedTotal.numDeleted++;
          //      failedstats.numDeleted++;
            }
        } else {
            cur += v->valLength(); 
            /* update stats for eviction that just happened */
            stats.evictStats.numTotalKeysEvicted++;
            stats.evictStats.numKeysEvicted++;
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
    if (store->getEvictionManager()->enableJob()) {
        templist = new FixedList<LRUItem, LRUItemCompare>(maxSize);
        stats.currentEvictionMemSize.incr(templist->memSize());
        timestats.reset();
        timestats.startTime = ep_real_time(); 
    }
}

bool LRUPolicy::addEvictItem(StoredValue *v, RCPtr<VBucket> currentBucket) {
    if (templist && store->getEvictionManager()->enableJob()) {
        time_t start = ep_real_time();
        LRUItem *item = new LRUItem(v, currentBucket->getId(), v->getDataAge());
        item->increaseCurrentSize(stats);
        if ((templist->size() == maxSize) && (lruItemCompare(*templist->last(), *item) < 0)) {
            return false;
        }
        stage.push_front(item);
        // this assumes that three pointers are used per node of list
        stats.currentEvictionMemSize.incr(3 * sizeof(int*));
        timestats.visitTime += ep_real_time() - start;
        return true;
    }
    clearTemplist();
    clearStage();
    return false;
}

bool LRUPolicy::storeEvictItem() {
    if (templist && store->getEvictionManager()->enableJob()) {
        time_t start = ep_real_time();
        templist->insert(stage, true);
        timestats.storeTime += ep_real_time() - start;
        clearStage();
        return true;
    }
    clearTemplist();
    clearStage();
    return false;
}

void LRUPolicy::completeRebuild() {
    if (templist && store->getEvictionManager()->enableJob()) {
        time_t start = ep_real_time();
        templist->build();
        FixedList<LRUItem, LRUItemCompare>::iterator tempit = templist->begin();
        tempit = it.swap(tempit);
        while (tempit != list->end()) {
            LRUItem *item = tempit++;
            item->reduceCurrentSize(stats);
            delete item;
        }
        stats.currentEvictionMemSize.decr(templist->memSize());
        delete list;
        list = templist;
        templist = NULL;
        timestats.endTime = ep_real_time();
        timestats.completeTime = timestats.endTime - start;
        userstats = timestats;
    } else {
        clearTemplist();
    }
    clearStage();
}
