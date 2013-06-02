/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
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
#include <vector>
#include <time.h>
#include <string.h>
#include <sstream>
#include <iostream>
#include <functional>

#include "ep.hh"
#include "flusher.hh"
#include "locks.hh"
#include "dispatcher.hh"
#include "kvstore.hh"
#include "ep_engine.h"
#include "htresizer.hh"
#include "eviction.hh"
#include "kvstore-mapper.hh"
#include "crc32.hh"

extern "C" {
    static rel_time_t uninitialized_current_time(void) {
        abort();
        return 0;
    }

    static time_t default_abs_time(rel_time_t) {
        abort();
        return 0;
    }

    static rel_time_t default_reltime(time_t) {
        abort();
        return 0;
    }

    rel_time_t (*ep_current_time)() = uninitialized_current_time;
    time_t (*ep_abs_time)(rel_time_t) = default_abs_time;
    rel_time_t (*ep_reltime)(time_t) = default_reltime;

    time_t ep_real_time() {
        return ep_abs_time(ep_current_time());
    }
}

/**
 * Dispatcher job that performs disk fetches for non-resident get
 * requests.
 */
class BGFetchCallback : public DispatcherCallback {
public:
    BGFetchCallback(EventuallyPersistentStore *e,
                    const std::string &k, uint16_t vbid, uint16_t vbv,
                    uint64_t r, const void *c) :
        ep(e), key(k), vbucket(vbid), vbver(vbv), rowid(r), cookie(c),
        counter(ep->bgFetchQueue), init(gethrtime()) {
        assert(ep);
        assert(cookie);
    }

    bool callback(Dispatcher &, TaskId) {
        ep->completeBGFetch(key, vbucket, vbver, rowid, cookie, init);
        return false;
    }

    std::string description() {
        std::stringstream ss;
        ss << "Fetching item from disk:  " << key;
        return ss.str();
    }

private:
    EventuallyPersistentStore *ep;
    std::string                key;
    uint16_t                   vbucket;
    uint16_t                   vbver;
    uint64_t                   rowid;
    const void                *cookie;
    BGFetchCounter             counter;

    hrtime_t init;
};

/**
 * Dispatcher job for performing disk fetches for "stats vkey".
 */
class VKeyStatBGFetchCallback : public DispatcherCallback {
public:
    VKeyStatBGFetchCallback(EventuallyPersistentStore *e,
                            const std::string &k, uint16_t vbid, uint16_t vbv,
                            uint64_t r,
                            const void *c, shared_ptr<Callback<GetValue> > cb) :
        ep(e), key(k), vbucket(vbid), vbver(vbv), rowid(r), cookie(c),
        lookup_cb(cb), counter(e->bgFetchQueue) {
        assert(ep);
        assert(cookie);
        assert(lookup_cb);
    }

    bool callback(Dispatcher &, TaskId) {
        RememberingCallback<GetValue> gcb;

        int id = KVStoreMapper::getKVStoreId(key, vbucket);
        ep->getROUnderlying(id)->get(key, rowid, vbucket, vbver, gcb);
        gcb.waitForValue();
        assert(gcb.fired);
        lookup_cb->callback(gcb.val);

        return false;
    }

    std::string description() {
        std::stringstream ss;
        ss << "Fetching item from disk for vkey stat:  " << key;
        return ss.str();
    }

private:
    EventuallyPersistentStore       *ep;
    std::string                      key;
    uint16_t                         vbucket;
    uint16_t                         vbver;
    uint64_t                         rowid;
    const void                      *cookie;
    shared_ptr<Callback<GetValue> >  lookup_cb;
    BGFetchCounter                   counter;
};

/**
 * Dispatcher job responsible for keeping the current state of
 * vbuckets recorded in the main db.
 */
class SnapshotVBucketsCallback : public DispatcherCallback {
public:
    SnapshotVBucketsCallback(EventuallyPersistentStore *e, const Priority &p, int i)
        : ep(e), priority(p), kvid(i) { }

    bool callback(Dispatcher &, TaskId) {
        ep->snapshotVBuckets(priority, kvid);
        return false;
    }

    std::string description() {
        return "Snapshotting vbuckets";
    }
private:
    EventuallyPersistentStore *ep;
    const Priority &priority;
    int kvid;
};

/**
 * Wake up connections blocked on pending vbuckets when their state
 * changes.
 */
class NotifyVBStateChangeCallback : public DispatcherCallback {
public:
    NotifyVBStateChangeCallback(RCPtr<VBucket> vb, EventuallyPersistentEngine &e)
        : vbucket(vb), engine(e) { }

    bool callback(Dispatcher &, TaskId) {
        vbucket->fireAllOps(engine);
        return false;
    }

    std::string description() {
        std::stringstream ss;
        ss << "Notifying state change of vbucket " << vbucket->getId();
        return ss.str();
    }

private:
    RCPtr<VBucket>              vbucket;
    EventuallyPersistentEngine &engine;
};

/**
 * Dispatcher job to perform fast vbucket deletion.
 */
class FastVBucketDeletionCallback : public DispatcherCallback {
public:
    FastVBucketDeletionCallback(EventuallyPersistentStore *e, RCPtr<VBucket> vb,
                                uint16_t vbv, EPStats &st) : ep(e),
                                                             vbucket(vb->getId()),
                                                             vbver(vbv),
                                               stats(st) {}

    bool callback(Dispatcher &, TaskId) {
        bool rv(true); // try again by default
        hrtime_t start_time(gethrtime());
        vbucket_del_result result = ep->completeVBucketDeletion(vbucket, vbver);
        if (result == vbucket_del_success || result == vbucket_del_invalid) {
            hrtime_t wall_time = (gethrtime() - start_time) / 1000;
            stats.diskVBDelHisto.add(wall_time);
            stats.vbucketDelMaxWalltime.setIfBigger(wall_time);
            stats.vbucketDelTotWalltime.incr(wall_time);
            rv = false;
        }
        return rv;
    }

    std::string description() {
        std::stringstream ss;
        ss << "Removing vbucket " << vbucket << " from disk";
        return ss.str();
    }

private:
    EventuallyPersistentStore *ep;
    uint16_t vbucket;
    uint16_t vbver;
    EPStats &stats;
};

/**
 * Dispatcher job to perform ranged vbucket deletion.
 */
class VBucketDeletionCallback : public DispatcherCallback {
public:
    VBucketDeletionCallback(EventuallyPersistentStore *e, RCPtr<VBucket> vb,
                            uint16_t vbucket_version, EPStats &st,
                            size_t csize = 100, uint32_t chunk_del_time = 500)
        : ep(e), stats(st), vb_version(vbucket_version),
          chunk_size(csize), chunk_del_threshold_time(chunk_del_time),
          vbdv(csize) {
        assert(ep);
        assert(vb);
        chunk_num = 1;
        execution_time = 0;
        start_wall_time = gethrtime();
        vbucket = vb->getId();
        vb->ht.visit(vbdv);
        vbdv.createRangeList(range_list);
        current_range = range_list.begin();
        if (current_range != range_list.end()) {
            chunk_del_range_size = current_range->second - current_range->first;
        } else {
            chunk_del_range_size = 100;
        }
    }

    bool callback(Dispatcher &d, TaskId t) {
        bool rv = false, isLastChunk = false;

        chunk_range range;
        if (current_range == range_list.end()) {
            range.first = -1;
            range.second = -1;
            isLastChunk = true;
        } else {
            if (current_range->second == range_list.back().second) {
                isLastChunk = true;
            }
            range.first = current_range->first;
            range.second = current_range->second;
        }

        hrtime_t start_time = gethrtime();
        vbucket_del_result result = ep->completeVBucketDeletion(vbucket,
                                                                vb_version,
                                                                range,
                                                                isLastChunk);
        hrtime_t chunk_time = (gethrtime() - start_time) / 1000;
        stats.diskVBChunkDelHisto.add(chunk_time);
        execution_time += chunk_time;

        switch(result) {
        case vbucket_del_success:
            if (!isLastChunk) {
                hrtime_t chunk_del_time = chunk_time / 1000; // chunk deletion exec time in msec
                if (range.first != -1 && range.second != -1 && chunk_del_time != 0) {
                    // Adjust the chunk's range size based on the chunk deletion execution time
                    chunk_del_range_size = (chunk_del_range_size * chunk_del_threshold_time)
                                           / chunk_del_time;
                    chunk_del_range_size = std::max(static_cast<int64_t>(100),
                                                    chunk_del_range_size);
                }

                ++current_range;
                // Split the current chunk into two chunks if its range size > the new range size
                if ((current_range->second - current_range->first) > chunk_del_range_size) {
                    range_list.splitChunkRange(current_range, chunk_del_range_size);
                } else {
                    // Merge the current chunk with its subsequent chunks before we reach the chunk
                    // that includes the end point of the new range size
                    range_list.mergeChunkRanges(current_range, chunk_del_range_size);
                }
                ++chunk_num;
                rv = true;
            } else { // Completion of a vbucket deletion
                stats.diskVBDelHisto.add(execution_time);
                hrtime_t wall_time = (gethrtime() - start_wall_time) / 1000;
                stats.vbucketDelMaxWalltime.setIfBigger(wall_time);
                stats.vbucketDelTotWalltime.incr(wall_time);
            }
            break;
        case vbucket_del_fail:
            d.snooze(t, 10);
            rv = true;
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                             "Reschedule to delete the chunk %d of vbucket %d from disk\n",
                             chunk_num, vbucket);
            break;
        case vbucket_del_invalid:
            break;
        }

        return rv;
    }

    std::string description() {
        std::stringstream ss;
        int64_t range_size = 0;
        if (current_range != range_list.end()) {
            range_size = current_range->second - current_range->first;
        }
        ss << "Removing the chunk " << chunk_num << "/" << range_list.size()
           << " of vbucket " << vbucket << " with the range size " << range_size
           << " from disk.";
        return ss.str();
    }
private:
    EventuallyPersistentStore    *ep;
    EPStats                      &stats;
    uint16_t                      vbucket;
    uint16_t                      vb_version;
    size_t                        chunk_size;
    size_t                        chunk_num;
    int64_t                       chunk_del_range_size;
    uint32_t                      chunk_del_threshold_time;
    VBucketDeletionVisitor        vbdv;
    hrtime_t                      execution_time;
    hrtime_t                      start_wall_time;
    VBDeletionChunkRangeList      range_list;
    chunk_range_iterator          current_range;
};

KVStoreMapper *KVStoreMapper::instance = NULL;



EventuallyPersistentStore::EventuallyPersistentStore(EventuallyPersistentEngine &theEngine,
                                                     KVStore **t,
                                                     bool startVb0,
                                                     bool concurrentDB,
                                                     int numKV) :
    engine(theEngine), stats(engine.getEpStats()),
    tctx(NULL), bgFetchDelay(0), numKVStores(numKV)
{
    doPersistence = getenv("EP_NO_PERSISTENCE") == NULL;

    dispatcher = new Dispatcher *[numKVStores];
    roDispatcher = new Dispatcher *[numKVStores];
    roBackfillDispatcher = new Dispatcher *[numKVStores];
    rwUnderlying = new KVStore *[numKVStores];
    roUnderlying = new KVStore *[numKVStores];
    roBackfillUnderlying = new KVStore *[numKVStores];
    flusher = new Flusher *[numKVStores];
    tctx = new TransactionContext *[numKVStores];
    storageProperties = new StorageProperties *[numKVStores];

    KVStoreMapper::createKVMapper(numKVStores, stats.kvstoreMapVbuckets);

    int maxShards = 0;
    int i = 0;
    for (std::map<std::string, KVStoreConfig*>::iterator it = theEngine.kvstoreConfigMap->begin();
            it != theEngine.kvstoreConfigMap->end(); it++, i++) {

        rwUnderlying[i] = t[i];
        dispatcher[i] = new Dispatcher(theEngine);
        flusher[i] = new Flusher(this, dispatcher[i], i);
        tctx[i] = new TransactionContext(stats, t[i],
                                         theEngine.syncRegistry, i);

        storageProperties[i] = new StorageProperties(t[i]->getStorageProperties());
        if (storageProperties[i]->maxConcurrency() > 1
            && storageProperties[i]->maxReaders() > 1
            && concurrentDB) {
            roUnderlying[i] = engine.newKVStore(it->second);
            roBackfillUnderlying[i] = engine.newKVStore(it->second);
            roDispatcher[i] = new Dispatcher(theEngine);
            roDispatcher[i]->start();
            roBackfillDispatcher[i] = new Dispatcher(theEngine);
            roBackfillDispatcher[i]->start();
        } else {
            roUnderlying[i] = rwUnderlying[i];
            roBackfillUnderlying[i] = rwUnderlying[i];
            roDispatcher[i]= dispatcher[i];
            roBackfillDispatcher[i] = dispatcher[i];
        }

        int nshards = rwUnderlying[i]->getNumShards();
        maxShards = maxShards > nshards ? maxShards : nshards;

        getLogger()->log(EXTENSION_LOG_INFO, NULL,
                         "Storage props:  c=%d/r=%d/rw=%d\n",
                         storageProperties[i]->maxConcurrency(),
                         storageProperties[i]->maxReaders(),
                         storageProperties[i]->maxWriters());
    }

    flushLists = new FlushLists(this, numKVStores, maxShards);

    nonIODispatcher = new Dispatcher(theEngine);
    invalidItemDbPager = new InvalidItemDbPager(this, stats, engine.getVbDelChunkSize());

    stats.memOverhead = sizeof(EventuallyPersistentStore);

    setTxnSize(DEFAULT_TXN_SIZE);
    diskFlushAll.resize(numKVStores);

    if (startVb0) {
        RCPtr<VBucket> vb(new VBucket(0, vbucket_state_active, stats));
        vbuckets.addBucket(vb);
        vbuckets.setBucketVersion(0, 0);
    }

    startDispatcher();
    startFlusher();
    startNonIODispatcher();
    assert(rwUnderlying);
    assert(roUnderlying);
    assert(roBackfillUnderlying);
}

/**
 * Hash table visitor used to collect dirty objects to verify storage.
 */
class VerifyStoredVisitor : public HashTableVisitor {
public:
    std::vector<std::string> dirty;
    void visit(StoredValue *v) {
        if (v->isDirty()) {
            dirty.push_back(v->getKey());
        }
    }
};

EventuallyPersistentStore::~EventuallyPersistentStore() {
    bool forceShutdown = engine.isForceShutdown();
    stopFlusher();
    for (int i = 0; i < numKVStores; ++i) {
        dispatcher[i]->stop(forceShutdown);
        if (hasSeparateRODispatcher(i)) {
            roDispatcher[i]->stop(forceShutdown);
            delete roDispatcher[i];
            delete roBackfillDispatcher[i];
            delete roUnderlying[i];
            delete roBackfillUnderlying[i];
        }
        delete flusher[i];
        delete dispatcher[i];
        delete tctx[i];
    }

    nonIODispatcher->stop(forceShutdown);

    delete flushLists;
    delete []flusher;
    delete []dispatcher;
    delete []tctx;
    delete []roUnderlying;
    delete []roBackfillUnderlying;
    delete []roDispatcher;
    delete []roBackfillDispatcher;

    delete nonIODispatcher;
    KVStoreMapper::destroy();
}

void EventuallyPersistentStore::startDispatcher() {
    for (int i = 0 ; i < numKVStores; ++i) {
        dispatcher[i]->start();
    }
}

void EventuallyPersistentStore::startNonIODispatcher() {
    nonIODispatcher->start();
}

Flusher* EventuallyPersistentStore::getFlusher(int id) {
    return flusher[id];
}

void EventuallyPersistentStore::startFlusher() {
    for (int i = 0 ; i < numKVStores; ++i) {
        flusher[i]->start();
    }
}

void EventuallyPersistentStore::stopFlusher() {
    for (int i = 0 ; i < numKVStores; ++i) {
        bool rv = flusher[i]->stop(engine.isForceShutdown());
        if (rv && !engine.isForceShutdown()) {
            flusher[i]->wait();
        }
    }
}

bool EventuallyPersistentStore::pauseFlusher() {
    for (int i = 0 ; i < numKVStores; ++i) {
        pauseFlusher(i);
    }
    return true;
}

bool EventuallyPersistentStore::resumeFlusher() {
    for (int i = 0 ; i < numKVStores; ++i) {
        resumeFlusher(i);
    }
    return true;
}

bool EventuallyPersistentStore::pauseFlusher(int kvId) {
    tctx[kvId]->commitSoon();
    flusher[kvId]->pause(FLUSHER_FLAG_FLUSHPARAM);
    return true;
}

bool EventuallyPersistentStore::resumeFlusher(int kvId) {
    flusher[kvId]->resume(FLUSHER_FLAG_FLUSHPARAM);
    return true;
}

RCPtr<VBucket> EventuallyPersistentStore::getVBucket(uint16_t vbucket) {
    return vbuckets.getBucket(vbucket);
}

RCPtr<VBucket> EventuallyPersistentStore::getVBucket(uint16_t vbid,
                                                     vbucket_state_t wanted_state) {
    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    vbucket_state_t found_state(vb ? vb->getState() : vbucket_state_dead);
    if (found_state == wanted_state) {
        return vb;
    } else {
        RCPtr<VBucket> rv;
        return rv;
    }
}

/// @cond DETAILS
/**
 * Inner loop of deleteExpiredItems.
 */
class Deleter {
public:
    Deleter(EventuallyPersistentStore *ep) : e(ep), startTime(ep_real_time()) {}
    void operator() (std::pair<uint16_t, std::string> vk) {
        RCPtr<VBucket> vb = e->getVBucket(vk.first);
        if (vb) {
            int bucket_num(0);
            LockHolder lh = vb->ht.getLockedBucket(vk.second, &bucket_num);
            StoredValue *v = vb->ht.unlocked_find(vk.second, bucket_num);
            if (v && v->isExpired(startTime)) {
                value_t value(NULL);
                uint64_t cas = v->getCas();
                MutationValue mv;
                mutation_type_t delrv = vb->ht.unlocked_softDelete(vk.second, 0, bucket_num, mv);
                if (delrv == WAS_CLEAN || delrv == WAS_DIRTY || delrv == NOT_FOUND ) {
                    if (!mv.wasDirty) {
                        e->queueFlusher(vb, v);
                    }
                }
                e->queueDirty(vk.second, vb->getId(), queue_op_del, value,
                              v->getFlags(), v->getExptime(), cas, v->getId());
            }
        }
    }
private:
    EventuallyPersistentStore *e;
    time_t                     startTime;
};
/// @endcond

void
EventuallyPersistentStore::deleteExpiredItems(std::list<std::pair<uint16_t, std::string> > &keys) {
    // This can be made a lot more efficient, but I'd rather see it
    // show up in a profiling report first.
    std::for_each(keys.begin(), keys.end(), Deleter(this));
}

StoredValue *EventuallyPersistentStore::fetchValidValue(RCPtr<VBucket> vb,
                                                        const std::string &key,
                                                        int bucket_num,
                                                        bool wantDeleted) {
    StoredValue *v = vb->ht.unlocked_find(key, bucket_num, wantDeleted);
    if (!CheckpointManager::isInconsistentSlaveCheckpoint() &&
            v && !v->isDeleted()) { // In the deleted case, we ignore expiration time.
        if (v->isExpired(ep_real_time())) {
            ++stats.expired;
            value_t value(NULL);
            uint64_t cas = v->getCas();
            MutationValue mv;
            mutation_type_t delrv = vb->ht.unlocked_softDelete(key, 0, bucket_num, mv);
            if (delrv == WAS_CLEAN || delrv == WAS_DIRTY || delrv == NOT_FOUND ) {
                // As replication is interleaved with online restore, deletion of items that might
                // exist in the restore backup files should be queued and replicated.
                if (!mv.wasDirty) {
                    queueFlusher(vb, v);
                }
            }
            queueDirty(key, vb->getId(), queue_op_del, value,
                       v->getFlags(), v->getExptime(), cas, v->getId());
            return (wantDeleted ? v : NULL);
        }
        v->touch();
    }
    return v;
}

protocol_binary_response_status EventuallyPersistentStore::evictKey(const std::string &key,
                                                                    uint16_t vbucket,
                                                                    const char **msg,
                                                                    size_t *msg_size,
                                                                    bool force) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb || (vb->getState() != vbucket_state_active && !force)) {
        return PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num, force);

    protocol_binary_response_status rv(PROTOCOL_BINARY_RESPONSE_SUCCESS);

    *msg_size = 0;
    if (v) {
        if (force)  {
            v->markClean(NULL);
        }
        if (v->isResident()) {
            if (v->ejectValue(stats, vb->ht)) {
                *msg = "Ejected.";
            } else if (v->isDirty()) {
                *msg = "Can't eject: Dirty object.";
            } else {
                *msg = "Can't eject: Small object.";
            }
        } else {
            *msg = "Already ejected.";
        }
    } else {
        *msg = "Not found.";
        rv = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
    }

    return rv;
}

ENGINE_ERROR_CODE EventuallyPersistentStore::set(Item &item,
                                                 const void *cookie,
                                                 bool force) {

    RCPtr<VBucket> vb = getVBucket(item.getVBucketId());
    if (!vb || vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_active) {
        if (vb->checkpointManager.isHotReload()) {
            if (vb->addPendingOp(cookie)) {
                return ENGINE_EWOULDBLOCK;
            }
        }
    } else if (vb->getState() == vbucket_state_replica && !force) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_pending && !force) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    bool cas_op = (item.getCas() != 0);

    int64_t row_id = -1;
    MutationValue mv;
    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(item.getKey(), &bucket_num);
    mutation_type_t mtype = vb->ht.set_unlocked(item, bucket_num, row_id, mv);
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    switch (mtype) {
    case NOMEM:
        ret = ENGINE_ENOMEM;
        break;
    case INVALID_CAS:
    case IS_LOCKED:
        ret = ENGINE_KEY_EEXISTS;
        break;
    case NOT_FOUND:
        if (cas_op) {
            ret = ENGINE_KEY_ENOENT;
            break;
        }
        // FALLTHROUGH
    case WAS_CLEAN:
        if (!mv.wasDirty) {
            queueFlusher(vb, mv.sv, item.getQueuedTime());
        }
        lh.unlock();

        // FALLTHROUGH
    case WAS_DIRTY:
        // Even if the item was dirty, push it into the vbucket's open checkpoint.
        queueDirty(item.getKey(), item.getVBucketId(), queue_op_set, item.getValue(),
                   item.getFlags(), item.getExptime(), item.getCas(), row_id,
                   item.getCksum(), item.getQueuedTime());
        break;
    case INVALID_VBUCKET:
        ret = ENGINE_NOT_MY_VBUCKET;
        break;
    }

    return ret;
}

ENGINE_ERROR_CODE EventuallyPersistentStore::add(Item &item,
                                                 const void *cookie)
{
    RCPtr<VBucket> vb = getVBucket(item.getVBucketId());
    if (!vb || vb->getState() == vbucket_state_dead || vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_active) {
        if (vb->checkpointManager.isHotReload()) {
            if (vb->addPendingOp(cookie)) {
                return ENGINE_EWOULDBLOCK;
            }
        }
    } else if(vb->getState() == vbucket_state_pending) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    if (item.getCas() != 0) {
        // Adding with a cas value doesn't make sense..
        return ENGINE_NOT_STORED;
    }

    int bucket_num(0);
    MutationValue mv;
    LockHolder lh = vb->ht.getLockedBucket(item.getKey(), &bucket_num);
    switch (vb->ht.add_unlocked(item, bucket_num, mv)) {
    case ADD_NOMEM:
        return ENGINE_ENOMEM;
    case ADD_EXISTS:
        return ENGINE_NOT_STORED;
    case ADD_SUCCESS:
    case ADD_UNDEL:
        if (!mv.wasDirty) {
            queueFlusher(vb, mv.sv);
        }
        lh.unlock();
        queueDirty(item.getKey(), item.getVBucketId(), queue_op_set, item.getValue(),
                   item.getFlags(), item.getExptime(), item.getCas(), -1, item.getCksum());
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentStore::addTAPBackfillItem(Item &item) {

    RCPtr<VBucket> vb = getVBucket(item.getVBucketId());
    if (!vb ||
        vb->getState() == vbucket_state_dead ||
        (vb->getState() == vbucket_state_active &&
         !CheckpointManager::isInconsistentSlaveCheckpoint())) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    }

    bool cas_op = (item.getCas() != 0);

    int64_t row_id = -1;
    int bucket_num(0);
    MutationValue mv;
    LockHolder lh = vb->ht.getLockedBucket(item.getKey(), &bucket_num);
    mutation_type_t mtype = vb->ht.set_unlocked(item, bucket_num, row_id, mv);
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    switch (mtype) {
    case NOMEM:
        ret = ENGINE_ENOMEM;
        break;
    case INVALID_CAS:
    case IS_LOCKED:
        ret = ENGINE_KEY_EEXISTS;
        break;
    case WAS_DIRTY:
        // Do normal stuff, but don't enqueue dirty flags.
        break;
    case NOT_FOUND:
        if (cas_op) {
            ret = ENGINE_KEY_ENOENT;
            break;
        }
        // FALLTHROUGH
    case WAS_CLEAN:
        if (!mv.wasDirty) {
            queueFlusher(vb, mv.sv);
        }
        break;
    case INVALID_VBUCKET:
        ret = ENGINE_NOT_MY_VBUCKET;
        break;
    }

    return ret;
}


void EventuallyPersistentStore::snapshotVBuckets(const Priority &priority, int kvid) {

    class VBucketStateVisitor : public VBucketVisitor {
    public:
        VBucketStateVisitor(VBucketMap &vb_map) : vbuckets(vb_map) { }
        bool visitBucket(RCPtr<VBucket> vb) {
            std::pair<uint16_t, uint16_t> p(vb->getId(),
                                            vbuckets.getBucketVersion(vb->getId()));
            vbucket_state vb_state;
            vb_state.state = VBucket::toString(vb->getState());
            vb_state.checkpointId = vbuckets.getPersistenceCheckpointId(vb->getId());
            states[p] = vb_state;
            return false;
        }

        void visit(StoredValue*) {
            assert(false); // this does not happen
        }

        std::map<std::pair<uint16_t, uint16_t>, vbucket_state> states;

    private:
        VBucketMap &vbuckets;
    };

    if (priority == Priority::VBucketPersistHighPriority) {
        vbuckets.setHighPriorityVbSnapshotFlag(false);
    } else {
        vbuckets.setLowPriorityVbSnapshotFlag(false);
    }

    VBucketStateVisitor v(vbuckets);
    visit(v);
    if (!rwUnderlying[kvid]->snapshotVBuckets(v.states)) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "Rescheduling a task to snapshot vbuckets\n");
        scheduleVBSnapshot(priority, kvid);
    }
}

void EventuallyPersistentStore::setVBucketState(uint16_t vbid,
                                                vbucket_state_t to) {
    // Lock to prevent a race condition between a failed update and add.
    LockHolder lh(vbsetMutex);
    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (vb) {
        vb->setState(to, engine.getServerApi());
        lh.unlock();
        nonIODispatcher->schedule(shared_ptr<DispatcherCallback>
                                             (new NotifyVBStateChangeCallback(vb,
                                                                        engine)),
                                  NULL, Priority::NotifyVBStateChangePriority, 0, false);
        int beginId, endId;
        KVStoreMapper::getVBucketToKVId(vbid, beginId, endId);
        for (int kvId = beginId; kvId < endId; kvId++) {
            scheduleVBSnapshot(Priority::VBucketPersistLowPriority, kvId);
        }
    } else {
        RCPtr<VBucket> newvb(new VBucket(vbid, to, stats));
        if (to != vbucket_state_active) {
            newvb->checkpointManager.setOpenCheckpointId(0);
        }
        uint16_t vb_version = vbuckets.getBucketVersion(vbid);
        uint16_t vb_new_version = vb_version == (std::numeric_limits<uint16_t>::max() - 1) ?
                                  0 : vb_version + 1;
        vbuckets.addBucket(newvb);
        vbuckets.setBucketVersion(vbid, vb_new_version);
        lh.unlock();
        int beginId, endId;
        KVStoreMapper::getVBucketToKVId(vbid, beginId, endId);
        for (int kvId = beginId; kvId < endId; kvId++) {
            scheduleVBSnapshot(Priority::VBucketPersistHighPriority, kvId);
        }
    }
}

void EventuallyPersistentStore::scheduleVBSnapshot(const Priority &p, int kvid) {
    if (p == Priority::VBucketPersistHighPriority) {
        if (!vbuckets.setHighPriorityVbSnapshotFlag(true)) {
            return;
        }
    } else {
        if (!vbuckets.setLowPriorityVbSnapshotFlag(true)) {
            return;
        }
    }
    dispatcher[kvid]->schedule(shared_ptr<DispatcherCallback>(
                        new SnapshotVBucketsCallback(this, p, kvid)), NULL, p, 0, false);
}

vbucket_del_result
EventuallyPersistentStore::completeVBucketDeletion(uint16_t vbid, uint16_t vb_version,
                                                   std::pair<int64_t, int64_t> row_range,
                                                   bool isLastChunk) {
    LockHolder lh(vbsetMutex);

    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (!vb || vb->getState() == vbucket_state_dead || vbuckets.isBucketDeletion(vbid)) {
        lh.unlock();
        if (row_range.first < 0 || row_range.second < 0 ||
            rwUnderlying[0]->delVBucket(vbid, vb_version, row_range)) {
            if (isLastChunk) {
                vbuckets.setBucketDeletion(vbid, false);
                ++stats.vbucketDeletions;
            }
            return vbucket_del_success;
        } else {
            ++stats.vbucketDeletionFail;
            return vbucket_del_fail;
        }
    }
    return vbucket_del_invalid;
}

vbucket_del_result
EventuallyPersistentStore::completeVBucketDeletion(uint16_t vbid, uint16_t vbver) {
    LockHolder lh(vbsetMutex);

    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (!vb || vb->getState() == vbucket_state_dead || vbuckets.isBucketDeletion(vbid)) {
        lh.unlock();
        if (rwUnderlying[0]->delVBucket(vbid, vbver)) {
            vbuckets.setBucketDeletion(vbid, false);
            ++stats.vbucketDeletions;
            return vbucket_del_success;
        } else {
            ++stats.vbucketDeletionFail;
            return vbucket_del_fail;
        }
    }
    return vbucket_del_invalid;
}

void EventuallyPersistentStore::scheduleVBDeletion(RCPtr<VBucket> vb, uint16_t vb_version,
                                                   double delay=0) {
    int beginId, endId;
    KVStoreMapper::getVBucketToKVId(vb->getId(), beginId, endId);
    if (vbuckets.setBucketDeletion(vb->getId(), true)) {
        for (int id = beginId; id < endId; id++) {
            if (storageProperties[id]->hasEfficientVBDeletion()) {
                shared_ptr<DispatcherCallback> cb(new FastVBucketDeletionCallback(this, vb,
                            vb_version,
                            stats));
                dispatcher[id]->schedule(cb,
                        NULL, Priority::FastVBucketDeletionPriority,
                        delay, false);
            } else {
                size_t chunk_size = engine.getVbDelChunkSize();
                uint32_t vb_chunk_del_time = engine.getVbChunkDelThresholdTime();
                shared_ptr<DispatcherCallback> cb(new VBucketDeletionCallback(this, vb, vb_version,
                            stats, chunk_size,
                            vb_chunk_del_time));
                dispatcher[id]->schedule(cb,
                        NULL, Priority::VBucketDeletionPriority,
                        delay, false);
            }
        }
    }
}

bool EventuallyPersistentStore::deleteVBucket(uint16_t vbid) {
    // Lock to prevent a race condition between a failed update and add (and delete).
    LockHolder lh(vbsetMutex);
    bool rv(false);

    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (vb && vb->getState() == vbucket_state_dead) {
        uint16_t vb_version = vbuckets.getBucketVersion(vbid);
        lh.unlock();
        rv = true;
        MultiLockHolder hlh = vb->ht.getLockedVBucket();
        HashTableStatVisitor statvis = vb->ht.unlocked_clear();
        stats.currentSize.decr(statvis.memSize - statvis.valSize);
        assert(stats.currentSize.get() < GIGANTOR);
        vbuckets.removeBucket(vbid);
        hlh.unlock();
        int beginId, endId;
        KVStoreMapper::getVBucketToKVId(vbid, beginId, endId);
        for (int kvId = beginId; kvId < endId; kvId++) {
            scheduleVBSnapshot(Priority::VBucketPersistHighPriority, kvId);
        }
        scheduleVBDeletion(vb, vb_version);
    }
    return rv;
}

bool EventuallyPersistentStore::resetVBucket(uint16_t vbid, bool underlying) {
    LockHolder lh(vbsetMutex);
    bool rv(false);

    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (vb) {
        if (vb->ht.getNumItems() == 0) { // Already reset?
            return true;
        }
        uint16_t vb_version = vbuckets.getBucketVersion(vbid);
        uint16_t vb_new_version = vb_version == (std::numeric_limits<uint16_t>::max() - 1) ?
                                  0 : vb_version + 1;
        MultiLockHolder hlh = vb->ht.getLockedVBucket();
        vbuckets.setBucketVersion(vbid, vb_new_version);
        vbuckets.setPersistenceCheckpointId(vbid, 0);
        lh.unlock();

        // Clear the hashtable, checkpoints, and stats for the target vbucket.
        HashTableStatVisitor statvis = vb->ht.unlocked_clear();
        hlh.unlock();
        stats.currentSize.decr(statvis.memSize - statvis.valSize);
        assert(stats.currentSize.get() < GIGANTOR);
        vb->checkpointManager.clear(vb->getState());
        vb->resetStats();

        int beginId, endId;
        KVStoreMapper::getVBucketToKVId(vbid, beginId, endId);
        for (int kvId = beginId; kvId < endId; kvId++) {
            scheduleVBSnapshot(Priority::VBucketPersistHighPriority, kvId);
        }
        // Clear all the items from the vbucket kv table on disk.
        if (underlying) {
            scheduleVBDeletion(vb, vb_version);
        }
        rv = true;
    }
    return rv;
}

void EventuallyPersistentStore::completeBGFetch(const std::string &key,
                                                uint16_t vbucket,
                                                uint16_t vbver,
                                                uint64_t rowid,
                                                const void *cookie,
                                                hrtime_t init) {
    hrtime_t start(gethrtime());
    ++stats.bg_fetched;
    std::stringstream ss;
    ss << "Completed a background fetch, now at " << bgFetchQueue.get()
       << std::endl;
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL, ss.str().c_str());

    if (!EvictionManager::getInstance()->evictHeadroom()) {
        ++stats.bg_fetch_failed;
        engine.notifyIOComplete(cookie, ENGINE_TMPFAIL);
        return;
    }

    // Go find the data
    RememberingCallback<GetValue> gcb;

    int id = KVStoreMapper::getKVStoreId(key, vbucket);
    roUnderlying[id]->get(key, rowid, vbucket, vbver, gcb);
    gcb.waitForValue();
    assert(gcb.fired);

    // Lock to prevent a race condition between a fetch for restore and delete
    LockHolder lh(vbsetMutex);

    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (vb && vb->getState() == vbucket_state_active && gcb.val.getStatus() == ENGINE_SUCCESS) {
        int bucket_num(0);
        LockHolder hlh = vb->ht.getLockedBucket(key, &bucket_num);
        StoredValue *v = fetchValidValue(vb, key, bucket_num);

        if (v && !v->isResident()) {
            assert(gcb.val.getStatus() == ENGINE_SUCCESS);
            v->restoreValue(gcb.val.getValue()->getValue(), stats, vb->ht);
            assert(v->isResident());
        }
    }

    lh.unlock();

    hrtime_t stop = gethrtime();

    if (stop > start && start > init) {
        // skip the measurement if the counter wrapped...
        ++stats.bgNumOperations;
        hrtime_t w = (start - init) / 1000;
        stats.bgWaitHisto.add(w);
        stats.bgWait += w;
        stats.bgMinWait.setIfLess(w);
        stats.bgMaxWait.setIfBigger(w);

        hrtime_t l = (stop - start) / 1000;
        stats.bgLoadHisto.add(l);
        stats.bgLoad += l;
        stats.bgMinLoad.setIfLess(l);
        stats.bgMaxLoad.setIfBigger(l);
    }

    engine.notifyIOComplete(cookie, gcb.val.getStatus());
    delete gcb.val.getValue();
}

void EventuallyPersistentStore::bgFetch(const std::string &key,
                                        uint16_t vbucket,
                                        uint16_t vbver,
                                        uint64_t rowid,
                                        const void *cookie) {
    shared_ptr<BGFetchCallback> dcb(new BGFetchCallback(this, key,
                                                        vbucket, vbver,
                                                        rowid, cookie));
    assert(bgFetchQueue > 0);
    std::stringstream ss;
    ss << "Queued a background fetch, now at " << bgFetchQueue.get()
       << std::endl;
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL, ss.str().c_str());
    int i = KVStoreMapper::getKVStoreId(key, vbucket);
    roDispatcher[i]->schedule(dcb, NULL, Priority::BgFetcherPriority, bgFetchDelay);
}

GetValue EventuallyPersistentStore::get(const std::string &key,
                                        uint16_t vbucket,
                                        const void *cookie,
                                        bool queueBG, bool honorStates,
                                        bool verifyCksum) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if (honorStates && vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if (vb->getState() == vbucket_state_active) {
        if (vb->checkpointManager.isHotReload()) {
            if (vb->addPendingOp(cookie)) {
                return GetValue(NULL, ENGINE_EWOULDBLOCK);
            }
        }
    } else if(honorStates && vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if(honorStates && vb->getState() == vbucket_state_pending) {
        if (vb->addPendingOp(cookie)) {
            return GetValue(NULL, ENGINE_EWOULDBLOCK);
        }
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num);

    if (v) {
        // If the value is not resident, wait for it...
        if (!v->isResident()) {
            if (queueBG) {
                bgFetch(key, vbucket, vbuckets.getBucketVersion(vbucket),
                        v->getId(), cookie);
            }
            return GetValue(NULL, ENGINE_EWOULDBLOCK, v->getId(), -1, v);
        }

        // return an invalid cas value if the item is locked
        uint64_t icas = v->isLocked(ep_current_time())
            ? static_cast<uint64_t>(-1)
            : v->getCas();

        Item * it = new Item(v->getKey(), v->getFlags(), v->getExptime(),
                             v->getValue(), v->getCksum(), icas, v->getId(), vbucket);

        GetValue rv(it, ENGINE_SUCCESS, v->getId(), -1, v);

        if (verifyCksum) {
            DataIntegrity *d = DataIntegrity::getDi(it->getCksumMeta());
            if (!d->verifyCksum(it)) {
                ++stats.cksumFailed;
                ++stats.ejectedDueToChecksum;
                /* Setting the mismatch flag */
                it->setCorruptCksumFlag();
                v->ejectValue(stats, vb->ht);
            }
        }
        return rv;
    } else {
        GetValue rv;
        if (isRestoreEnabled()) {
            rv.setStatus(ENGINE_TMPFAIL);
        }
        return rv;
    }
}

GetValue EventuallyPersistentStore::getAndUpdateTtl(const std::string &key,
                                                    uint16_t vbucket,
                                                    const void *cookie,
                                                    bool queueBG,
                                                    uint32_t exptime)
{
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if (vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if (vb->getState() == vbucket_state_active) {
        if (vb->checkpointManager.isHotReload()) {
            if (vb->addPendingOp(cookie)) {
                return GetValue(NULL, ENGINE_EWOULDBLOCK);
            }
        }
    } else if (vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return GetValue(NULL, ENGINE_NOT_MY_VBUCKET);
    } else if (vb->getState() == vbucket_state_pending) {
        if (vb->addPendingOp(cookie)) {
            return GetValue(NULL, ENGINE_EWOULDBLOCK);
        }
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num);

    if (v) {
        v->setExptime(exptime);
        // If the value is not resident, wait for it...
        if (!v->isResident()) {
            if (queueBG) {
                bgFetch(key, vbucket, vbuckets.getBucketVersion(vbucket),
                        v->getId(), cookie);
                return GetValue(NULL, ENGINE_EWOULDBLOCK, v->getId());
            } else {
                // You didn't want the item anyway...
                return GetValue(NULL, ENGINE_SUCCESS, v->getId());
            }
        }

        // return an invalid cas value if the item is locked
        uint64_t icas = v->isLocked(ep_current_time())
            ? static_cast<uint64_t>(-1)
            : v->getCas();

        Item *it = new Item(v->getKey(), v->getFlags(), v->getExptime(),
                       v->getValue(), v->getCksum(), icas, v->getId(), vbucket);
        GetValue rv(it, ENGINE_SUCCESS, v->getId());

        DataIntegrity *d = DataIntegrity::getDi(it->getCksumMeta());
        if (!d->verifyCksum(it)) {
            ++stats.cksumFailed;
            ++stats.ejectedDueToChecksum;
            /* Setting the mismatch flag */
            it->setCorruptCksumFlag();
            v->ejectValue(stats, vb->ht);
        }
        return rv;
    } else {
        GetValue rv;
        if (isRestoreEnabled()) {
            rv.setStatus(ENGINE_TMPFAIL);
        }
        return rv;
    }
}

ENGINE_ERROR_CODE
EventuallyPersistentStore::getFromUnderlying(const std::string &key,
                                             uint16_t vbucket,
                                             const void *cookie,
                                             shared_ptr<Callback<GetValue> > cb) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb || vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_active) {
        if (vb->checkpointManager.isHotReload()) {
            if (vb->addPendingOp(cookie)) {
                return ENGINE_EWOULDBLOCK;
            }
        }
    } else if (vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_pending) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num);

    if (v) {
        uint16_t vbver = vbuckets.getBucketVersion(vbucket);
        shared_ptr<VKeyStatBGFetchCallback> dcb(new VKeyStatBGFetchCallback(this, key,
                                                                            vbucket,
                                                                            vbver,
                                                                            v->getId(),
                                                                            cookie, cb));
        assert(bgFetchQueue > 0);
        int i = KVStoreMapper::getKVStoreId(key, vbucket);
        roDispatcher[i]->schedule(dcb, NULL, Priority::VKeyStatBgFetcherPriority, bgFetchDelay);
        return ENGINE_EWOULDBLOCK;
    } else if (isRestoreEnabled()) {
        return ENGINE_TMPFAIL;
    } else {
        return ENGINE_KEY_ENOENT;
    }
}

bool EventuallyPersistentStore::getLocked(const std::string &key,
                                          uint16_t vbucket,
                                          Callback<GetValue> &cb,
                                          rel_time_t currentTime,
                                          uint32_t lockTimeout,
                                          std::string &metadata,
                                          const void *cookie) {
    RCPtr<VBucket> vb = getVBucket(vbucket, vbucket_state_active);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        ++stats.getl_misses_notmyvbuckets;
        GetValue rv(NULL, ENGINE_NOT_MY_VBUCKET);
        cb.callback(rv);
        return false;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num);

    if (v) {

        // if v is locked return error
        if (v->isLocked(currentTime)) {
            // Return the metadata associated with this lock, if any
            metadata = v->getMetadata();

            ++stats.getl_misses_locked;
            GetValue rv;
            cb.callback(rv);
            return false;
        }

        // If the value is not resident, wait for it...
        if (!v->isResident()) {

            if (cookie) {
                bgFetch(key, vbucket, vbuckets.getBucketVersion(vbucket),
                        v->getId(), cookie);
            }
            GetValue rv(NULL, ENGINE_EWOULDBLOCK, v->getId());
            cb.callback(rv);
            return false;
        }

        ++stats.getl_hits;

        // acquire lock and increment cas value
        // lock will also associate the metadata with the lock
        v->lock(currentTime + lockTimeout, metadata);

        Item *it = new Item(v->getKey(), v->getFlags(), v->getExptime(),
                            v->getValue(), v->getCksum(), v->getCas());

        DataIntegrity *d = DataIntegrity::getDi(it->getCksumMeta());
        if (!d->verifyCksum(it)) {
            ++stats.cksumFailed;
            ++stats.ejectedDueToChecksum;
            /* Setting the mismatch flag */
            it->setCorruptCksumFlag();
            v->ejectValue(stats, vb->ht);
        }

        it->setCas();
        v->setCas(it->getCas());

        GetValue rv(it);
        cb.callback(rv);

    } else {
        ++stats.getl_misses_notfound;
        GetValue rv;
        if (isRestoreEnabled()) {
            rv.setStatus(ENGINE_TMPFAIL);
        }
        cb.callback(rv);
    }
    return true;
}

StoredValue* EventuallyPersistentStore::getStoredValue(const std::string &key,
                                                       uint16_t vbucket,
                                                       bool honorStates) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return NULL;
    } else if (honorStates && vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return NULL;
    } else if (vb->getState() == vbucket_state_active) {
        // OK
    } else if(honorStates && vb->getState() == vbucket_state_replica) {
        ++stats.numNotMyVBuckets;
        return NULL;
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    return fetchValidValue(vb, key, bucket_num);
}

ENGINE_ERROR_CODE
EventuallyPersistentStore::unlockKey(const std::string &key,
                                     uint16_t vbucket,
                                     uint64_t cas,
                                     rel_time_t currentTime)
{

    RCPtr<VBucket> vb = getVBucket(vbucket, vbucket_state_active);
    if (!vb) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    }

    int bucket_num(0);
    ++stats.num_unlocks;
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num);

    if (v) {
        if (v->isLocked(currentTime)) {
            if (v->getCas() == cas) {
                // Unlock will also clear the metadata associated with the lock
                v->unlock();
                return ENGINE_SUCCESS;
            }
        }
        return ENGINE_TMPFAIL;
    }

    if (isRestoreEnabled()) {
        return ENGINE_TMPFAIL;
    }

    return ENGINE_KEY_ENOENT;
}


bool EventuallyPersistentStore::getKeyStats(const std::string &key,
                                            uint16_t vbucket,
                                            struct key_stats &kstats)
{
    RCPtr<VBucket> vb = getVBucket(vbucket, vbucket_state_active);
    if (!vb) {
        return false;
    }

    bool found = false;
    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num);

    found = (v != NULL);
    if (found) {
        kstats.dirty = v->isDirty();
        kstats.exptime = v->getExptime();
        kstats.flags = v->getFlags();
        kstats.cas = v->getCas();
        // TODO:  Know this somehow.
        kstats.dirtied = 0; // v->getDirtied();
        kstats.data_age = v->getDataAge();
        kstats.last_modification_time = ep_abs_time(v->getDataAge());
        kstats.resident = v->isResident();
    }
    return found;
}

void EventuallyPersistentStore::setGetItemsThresholds(size_t upper, size_t lower, size_t maxChecks) {
    getItemsUpperThreshold = upper;
    getItemsLowerThreshold = lower;
    maxGetItemsChecks = maxChecks;
    getItemsThresholdChecks = 0;
}

void EventuallyPersistentStore::setMinDataAge(int to) {
    stats.min_data_age.set(to);
}

void EventuallyPersistentStore::setQueueAgeCap(int to) {
    stats.queue_age_cap.set(to);
}

ENGINE_ERROR_CODE EventuallyPersistentStore::del(const std::string &key,
                                                 uint64_t cas,
                                                 uint16_t vbucket,
                                                 const void *cookie,
                                                 bool force) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb || vb->getState() == vbucket_state_dead) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if (vb->getState() == vbucket_state_active) {
        if (vb->checkpointManager.isHotReload()) {
            if (vb->addPendingOp(cookie)) {
                return ENGINE_EWOULDBLOCK;
            }
        }
    } else if(vb->getState() == vbucket_state_replica && !force) {
        ++stats.numNotMyVBuckets;
        return ENGINE_NOT_MY_VBUCKET;
    } else if(vb->getState() == vbucket_state_pending && !force) {
        if (vb->addPendingOp(cookie)) {
            return ENGINE_EWOULDBLOCK;
        }
    }

    int bucket_num(0);
    int rowid = -1;
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = vb->ht.unlocked_find(key, bucket_num);
    if (!v) {
        if (isRestoreEnabled()) {
            LockHolder rlh(restore.mutex);
            restore.itemsDeleted.insert(key);
        } else {
            if (CheckpointManager::isInconsistentSlaveCheckpoint()) {
                queueDirty(key, vbucket, queue_op_del, value_t(NULL), 0, 0, cas, rowid);

                getLogger()->log(EXTENSION_LOG_INFO, NULL, "Forward del %s to checkpoint "
                                                "(not-found in hash table)", key.c_str());
            }
            return ENGINE_KEY_ENOENT;
        }
    } else {
        rowid = v->getId();
    }

    MutationValue mv;
    mutation_type_t delrv = vb->ht.unlocked_softDelete(key, cas, bucket_num, mv);
    ENGINE_ERROR_CODE rv;
    bool expired = false;

    if (delrv == NOT_FOUND || delrv == INVALID_CAS) {
        if (v && v->isExpired(ep_real_time())) {
            expired = true;
        }
        rv = ENGINE_KEY_ENOENT;
    } else if (delrv == IS_LOCKED) {
        rv = ENGINE_TMPFAIL;
    } else { // WAS_CLEAN or WAS_DIRTY
        rv = ENGINE_SUCCESS;
    }

    if (delrv == WAS_CLEAN ||
        delrv == WAS_DIRTY ||
        (delrv == NOT_FOUND && expired)) {
        // As replication is interleaved with online restore, deletion of items that might
        // exist in the restore backup files should be queued and replicated.
        if (!mv.wasDirty) {
            queueFlusher(vb, mv.sv);
        }
        lh.unlock();
        queueDirty(key, vbucket, queue_op_del, value_t(NULL), 0, 0, cas, rowid);
    }
    return rv;
}

void EventuallyPersistentStore::reset() {
    std::vector<int> buckets = vbuckets.getBuckets();
    std::vector<int>::iterator it;

    for (int i = 0; i < numKVStores; ++i) {
        if (diskFlushAll[i].cas(false, true)) {
            flusher[i]->setFlushAll(true);
            stats.queue_size.incr(1);
        }
    }

    for (it = buckets.begin(); it != buckets.end(); ++it) {
        resetVBucket(*it, false);
    }
}

// This works on the premise that the stored value is only freed by flusher
void EventuallyPersistentStore::beginFlush(FlushList &out, int kvId) {
    std::vector<uint16_t> vblist = KVStoreMapper::getVBucketsForKVStore(kvId);
    std::vector<uint16_t>::iterator vb_it = vblist.begin();

    if (hasItemsForPersistence(kvId)) {
        scrub_memory();
        flushLists->get(out, kvId);

        size_t num_items = out.size();

        if (num_items > 0 ) {
            for (; vb_it != vblist.end(); vb_it++) {
                RCPtr<VBucket> vb = getVBucket(*vb_it);
                if (!vb) {
                    continue;
                }
                rwUnderlying[kvId]->setPersistenceCheckpointId(*vb_it, vb->checkpointManager.getOpenCheckpointId());
            }

            stats.flusher_todos[kvId].incr(num_items);
            assert(stats.queue_size >= num_items);
            stats.queue_size.decr(num_items);
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                             "Flusher Id %d flushing %d items with %lu still in queue\n",
                             kvId, num_items, stats.queue_size.get());
            return;
        } else {
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                             "Flusher Id %d did not find any items to flush\n", kvId);
            return;
        }
    } else {
        stats.dirtyAge = 0;
        // If the persistence queue is empty, reset queue-related stats for each vbucket.
        for (; vb_it != vblist.end(); vb_it++) {
            RCPtr<VBucket> vb = vbuckets.getBucket(*vb_it);
            if (vb && vb->checkpointManager.getNumItemsForPersistence() == 0) {
                vb->dirtyQueueSize.set(0);
                vb->dirtyQueueMem.set(0);
                vb->dirtyQueueAge.set(0);
                vb->dirtyQueuePendingWrites.set(0);
            }
        }
    }
    return;
}

void EventuallyPersistentStore::requeueRejectedItems(FlushList *rejectList,
                                                     FlushList *flushList,
                                                     int kvId) {
    flushList->splice(flushList->end(), *rejectList);
    stats.flusher_todos[kvId].incr(flushList->size()); // FIXME is this really needed here?, why not do flusher_todo stats directly from flushlist size everytime?
}

void EventuallyPersistentStore::completeFlush(rel_time_t flush_start, int id) {
    LockHolder lh(vbsetMutex);

    std::vector<uint16_t> vblist = KVStoreMapper::getVBucketsForKVStore(id);
    std::vector<uint16_t>::iterator vb_it = vblist.begin();
    for (; vb_it != vblist.end(); vb_it++) {
        RCPtr<VBucket> vb = getVBucket(*vb_it);
        if (!vb || vb->getState() == vbucket_state_dead) {
            continue;
        }
        uint64_t persistedCheckpointId = rwUnderlying[id]->getPersistenceCheckpointId(*vb_it);
        if (persistedCheckpointId > vbuckets.getPersistenceCheckpointId(*vb_it)) {
            vbuckets.setPersistenceCheckpointId(*vb_it, persistedCheckpointId);
        }
    }
    rwUnderlying[id]->clearPersistenceCheckpointIds();
    lh.unlock();

    // Schedule the vbucket state snapshot task to record the latest checkpoint Id
    // that was successfully persisted for each vbucket.
    scheduleVBSnapshot(Priority::VBucketPersistHighPriority, id);

    rel_time_t time_diff = ep_current_time() - flush_start;
    stats.flushDuration.set(time_diff);
    stats.flushDurations[id].set(time_diff);
    stats.flushDurationHighWat.set(std::max(time_diff,
                                            stats.flushDurationHighWat.get()));
    stats.flushDurationHighWats[id].set(std::max(time_diff,
                                            stats.flushDurationHighWats[id].get()));
    stats.cumulativeFlushTime.incr(time_diff);
    stats.cumulativeFlushTimes[id].incr(time_diff);
}

// Returns time in seconds after which flusher should come back.
// If there were no rejects, returns 0 to come back immediately
// else least amount of time remaining for an item to become eligible for flush.
int EventuallyPersistentStore::flushSome(FlushList *flushList,
                                         FlushList *rejectList,
                                         int id) {
    if (!tctx[id]->enter()) {
        ++stats.beginFailed[id];
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Failed to start a transaction.\n");
        // Copy the input queue into the reject queue.
        while (!flushList->empty()) {
            FlushEntry &fe(flushList->front());
            flushList->pop_front();
            rejectList->push_back(fe);
        }
        stats.flusher_todos[id].decr(rejectList->size());
        return 1; // This will cause us to jump out and delay a second
    }
    int tsz = tctx[id]->remaining();
    int oldest = stats.min_data_age;
    int completed(0);
    int rejected(0);
    for (completed = 0;
         completed < tsz && !flushList->empty() && !shouldPreemptFlush(completed, id);
         ++completed) {
        bool wasRejected = false;
        FlushEntry &fe(flushList->front());
        flushList->pop_front();
        int n = flushOne(fe, rejectList, id, wasRejected);
        if (wasRejected) {
            oldest = std::min(oldest, n);
            rejected++;
        }
    }
    if (shouldPreemptFlush(completed, id)) {
        ++stats.flusherPreempts[id];
    } else {
        tctx[id]->commit();
    }
    tctx[id]->leave(completed);
    return (rejected == 0 ? 0 : oldest);
}

size_t EventuallyPersistentStore::getWriteQueueSize(void) {
    return flushLists->size();
}

bool EventuallyPersistentStore::hasItemsForPersistence(int kvid) {
    return 0 != flushLists->size(kvid);
}

void EventuallyPersistentStore::setPersistenceCheckpointId(uint16_t vbid, uint64_t checkpointId) {
    LockHolder lh(vbsetMutex);
    vbuckets.setPersistenceCheckpointId(vbid, checkpointId);
}

// FIXME: Caution. This is no longer safe to use with the new flusher
protocol_binary_response_status EventuallyPersistentStore::revertOnlineUpdate(RCPtr<VBucket> vb) {
    protocol_binary_response_status rv(PROTOCOL_BINARY_RESPONSE_SUCCESS);

    const char *msg = NULL;
    size_t msg_size = 0;
    std::vector<queued_item> item_list;

    assert(0);
    if (!vb || vb->getState() == vbucket_state_dead) {
        return rv;
    }

    uint16_t vbid = vb->getId();
    BlockTimer timer(&stats.checkpointRevertHisto);

    //Acquire a lock before starting the hot reload process
    LockHolder lh(vbsetMutex);
    rv = vb->checkpointManager.beginHotReload();
    if ( rv != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        return rv;
    }
    lh.unlock();

    // Get all the mutations from the current position of the online update cursor to
    // the tail of the current open checkpoint.
    vb->checkpointManager.getAllItemsForOnlineUpdate(item_list);
    if (item_list.size() == 0) {
        // Need to count items for checkpoint_start, checkpoint_end, onlineupdate_start,
        // onlineupdate_revert
        vb->checkpointManager.endHotReload(4);
        return rv;
    }

    std::set<queued_item, CompareQueuedItemsByKey> item_set;
    std::pair<std::set<queued_item, CompareQueuedItemsByKey>::iterator, bool> ret;
    std::vector<queued_item>::reverse_iterator reverse_it = item_list.rbegin();
    // Perform further deduplication here by removing duplicate mutations for each key.
    // For this, traverse the array from the last element.
    uint64_t total = 0;
    for(; reverse_it != item_list.rend(); ++reverse_it, ++total) {
        queued_item item = *reverse_it;

        ret = item_set.insert(item);

        vb->doStatsForFlushing(sizeof(FlushEntry), item->size(), item->getQueuedTime());
    }
    item_list.assign(item_set.begin(), item_set.end());

    std::vector<queued_item>::iterator it = item_list.begin();
    for(; it != item_list.end(); ++it) {
        if ((*it)->getOperation() == queue_op_del)  {
            ++stats.numRevertDeletes;
            //Reset the deleted value first before evict it.
            vb->ht.add((*it)->getItem(), false, false);
            this->evictKey((*it)->getKey(), vbid, &msg, &msg_size, true);
        } else if ((*it)->getOperation() == queue_op_set) {
            //check if it is add or set
            if ((*it)->getRowId() < 0)  {
                ++stats.numRevertAdds;
                //since no value exists on disk, simply delete it from hashtable
                vb->ht.del((*it)->getKey());
            } else {
                ++stats.numRevertUpdates;
                this->evictKey((*it)->getKey(), vbid, &msg, &msg_size, true);
            }
        }

    }
    item_list.clear();

    //Stop the hot reload process
    vb->checkpointManager.endHotReload(total);

    //Notify all the blocked client requests
    nonIODispatcher->schedule(shared_ptr<DispatcherCallback>
                                             (new NotifyVBStateChangeCallback(vb,
                                                                        engine)),
                                  NULL, Priority::NotifyVBStateChangePriority, 0, false);
    return rv;
}

/**
 * Callback invoked after persisting an item from memory to disk.
 *
 * This class exists to create a closure around a few variables within
 * EventuallyPersistentStore::flushOne so that an object can be
 * requeued in case of failure to store in the underlying layer.
 */
#if 0
class PersistenceCallback : public Callback<mutation_result>,
                            public Callback<int> {
public:

    PersistenceCallback(FlushEntry *fe, FlushList *rejList,
                        EventuallyPersistentStore *st, EPStats *s) :
        flushEntry(fe), rejectList(rejList), store(st), stats(s) {
        assert(rejectList);
        assert(s);
    }

    // This callback is invoked for set only.
    bool callback(mutation_result &value) {
        if (value.first == 1) {
            if (value.second > 0) {
                ++stats->newItems;
                setId(value.second);
            }

            ++stats->totalEvictable;

            /*
            FIXME
            This code evicts this item if this is a replica and we are above low-water-mark. I'm commenting this out because in our new eviction model, this needs to be handled differently.

            RCPtr<VBucket> vb = store->getVBucket(flushEntry->getVBucketId());
            if (vb && vb->getState() != vbucket_state_active &&
                vb->getState() != vbucket_state_pending) {
                int bucket_num(0);
                LockHolder lh = vb->ht.getLockedBucket(flushEntry->getKey(), &bucket_num);
                StoredValue *v = store->fetchValidValue(vb, flushEntry->getKey(),
                                                        bucket_num, true);
                assert(v == flushEntry->getStoredValue());
                double current = static_cast<double>(StoredValue::getCurrentSize(*stats));
                double lower = static_cast<double>(stats->mem_low_wat);
                if (v && current > lower) {
                    if (v->ejectValue(*stats, vb->ht) && vb->getState() == vbucket_state_replica) {
                        ++stats->numReplicaEjects;
                    }
                }
            }
            */

            delete flushEntry;
        } else {
            // If the return was 0 here, we're in a bad state because
            // we do not know the rowid of this object.
            if (value.first == 0) {
                RCPtr<VBucket> vb = store->getVBucket(flushEntry->vbId);
                int bucket_num(0);
                LockHolder lh = vb->ht.getLockedBucket(flushEntry->v->getKey(), &bucket_num);
                StoredValue *v = store->fetchValidValue(vb, flushEntry->v->getKey(),
                                                        bucket_num, true);
                assert(v == flushEntry->v);
                if (v) {
                    std::stringstream ss;
                    ss << "Persisting ``" << v->getKey() << "'' on vb"
                       << flushEntry->vbId << " (rowid=" << v->getId()
                       << ") returned 0 updates\n";
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL, "%s", ss.str().c_str());
                } else {
                    getLogger()->log(EXTENSION_LOG_INFO, NULL,
                                     "Error persisting now missing ``%s'' from vb%d\n",
                                     v->getKey().c_str(), flushEntry->vbId);
                }
                delete flushEntry;
            } else {
                redirty();
            }
        }
        return true;
    }

    // This callback is invoked for deletions only.
    //
    // The boolean indicates whether the underlying storage
    // successfully deleted the item.
    //  -1 means fail
    //  1 means we deleted one row
    //  Failures that should never happen:
    //      > 1 would be bad.  We were only trying to delete one row.
    //      0 means we did not delete a row, but did not fail (did not exist)
    bool callback(int &rowsAffected) {
        if (rowsAffected < 0) {
            rejectList->push_back(*flushEntry);
            return true;
        }

        assert(rowsAffected == 1);

        ++stats->delItems;

        RCPtr<VBucket> vb = store->getVBucket(flushEntry->vbId);
        if (vb) {
            int bucket_num;
            LockHolder lh = vb->ht.getLockedBucket(flushEntry->v->getKey(), &bucket_num);
            StoredValue *v = store->fetchValidValue(vb, flushEntry->v->getKey(), bucket_num, true);
            assert(v == flushEntry->v);
            // Only proceed with deletion if the item is still deleted
            if (v->isDeleted()) {
                bool deleted = vb->ht.unlocked_del(v->getKey(), bucket_num);
                assert(deleted);
                delete flushEntry;
            } else {
                redirty();
            }
        }
        return true;
    }

private:

    void setId(int64_t id) {
        bool did = store->invokeOnLockedStoredValue(flushEntry->v->getKey(),
                                                    flushEntry->vbId,
                                                    &StoredValue::setId,
                                                    id);
        if (!did) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Failed to set id on vb%d ``%s''\n",
                             flushEntry->vbId, flushEntry->v->getKey().c_str());
        }
    }

    void redirty() {
        ++stats->flushFailed;

        RCPtr<VBucket> vb = store->getVBucket(flushEntry->vbId);
        if (vb) {
            int bucket_num(0);
            LockHolder lh = vb->ht.getLockedBucket(flushEntry->v->getKey(), &bucket_num);
            StoredValue *v = store->fetchValidValue(vb, flushEntry->v->getKey(), bucket_num, true);
            assert(v == flushEntry->v);
            if (v->isClean()) {
                v->markDirty();
                rejectList->push_back(*flushEntry);
            } else {
                // Some other mutation came for this stored-value while we were executing in
                // kvstore->set or kvstore->del. Since that mutation has queued a flushentry,
                // we will delete this one and not requeue it.
                delete flushEntry;
            }
        } else {
            delete flushEntry;
        }
    }

    FlushEntry *flushEntry;
    FlushList *rejectList;
    EventuallyPersistentStore *store;
    EPStats *stats;
    DISALLOW_COPY_AND_ASSIGN(PersistenceCallback);
};
#endif

int EventuallyPersistentStore::flushOneDeleteAll(int id) {
    rwUnderlying[id]->reset();
    diskFlushAll[id].cas(true, false);
    stats.queue_size.decr(1);

    return 1;
}


int EventuallyPersistentStore::eligibleForPersistence(StoredValue *v, rel_time_t queued) {
    int eligible = 0;

    rel_time_t now = ep_current_time();

    int dataAge = now - v->getDataAge();
    int queueAge = now - queued;

    int minDataAge = stats.min_data_age.get();
    int maxQueueAge = stats.queue_age_cap.get();

    if (queueAge >= maxQueueAge) {
        ++stats.tooOld;
    } else if (dataAge < minDataAge) {
        ++stats.tooYoung;
        eligible = std::min(minDataAge-dataAge, maxQueueAge-queueAge);
    }

    return eligible;
}

/**
 * flushOneDelOrSet
 *
 * This stored-value is dirty. It could be because
 *  - It's a new item, it has been deleted before it was ever saved to disk, it needs to be deleted from memory.
 *  - It's a new item, it needs to be saved to disk.
 *
 *  - It's an existing item, it was mutated, it needs to be saved to disk.
 *
 *  - It's an existing item, it was deleted, it needs to be deleted from disk.
 *  - It's an existing item, it expired, it needs to be deleted from disk.
 *  - It was one of the above cases, and in a previous attempt of this function, it failed, it needs to be retried.
 *
 *  See if we need to yield to the dispatcher for a higher priority job. If so, return.
 *
 *  We acquire lock on hashtable bucket for this key.
 *  Get the storedValue
 *      Evaluate if it's eligible for persistence based on min_data_age/queue_age_cap criteria.
 *      If it's not eligible
 *          add it to rejectList.
 *          Return the time in seconds after which this item will become eligible.
 *
 *  Construct an Item to pass to kvstore->set or kvstore->del.
 *  Release the lock.
 *      Note: While we are executing here after releasing the lock, a frontend
 *      operation can happen on this item. But it will not queue any new FlushEntry
 *      as it's still marked dirty. So, we need to handle this case after we come
 *      back from kvstore operation below.
 *
 *  Invoke the kvstore function. It returns success or failure.
 *      If operation succeeded, we now need to check if a mutation came while we were
 *      processing current mutation.
 *          If yes, we simply add our current flushEntry to rejectList.
 *          If no, we mark storedValue as clean! yay!
 *
 *      If operation failed, we need to simply add it to rejectList.
 *
 *      Note: If we don't requeue the flushEntry, we are responsible for deallocating (deleting) it.
 *
 *  Return 0
 *
 */
/* We have 3 cases to handle.
     *  Delete from memory because item is new and got deleted before it was ever saved to disk.
     *  Delete from disk because item existed and it got deleted from disk.
     *  Save to disk because item needs to be saved.
     */
int EventuallyPersistentStore::flushOneDelOrSet(FlushEntry *fe, FlushList *rejectList, int kvId) {
    if (vbuckets.isHighPriorityVbSnapshotScheduled()) {
        rejectList->push_back(*fe);
        return 0;
    }

    RCPtr<VBucket> vb = getVBucket(fe->getVBucketId());
    if (!vb) {
        delete fe;
        return 0;
    }

    int bucket_num = 0;
    LockHolder lh = vb->ht.getLockedBucket(fe->v->getKey(), &bucket_num);

    if (fe->vbVersion != vbuckets.getBucketVersion(fe->getVBucketId())) {
        delete fe;
        return 0;
    }

    StoredValue *v = vb->ht.unlocked_find(fe->v->getKey(), bucket_num, true);

    assert(fe->v == v);         // what we queued is same as what's in ht.
    assert(v->isDirty());       // we queued it because it became dirty and it still is dirty.
    assert(!v->isPendingId());  // Only we do IO for this item and we have not yet done it.

    // (1) New item, already deleted, just delete from memory.
    if (!v->hasId() && v->isDeleted()) {
        bool deleted = vb->ht.unlocked_del(fe->v->getKey(), bucket_num);
        assert(deleted);
        delete fe;
        return 0;
    }

    // Next cases require disk io. Let's check it's eligibility for persistence.
    int eligibleIn = this->eligibleForPersistence(v, fe->queuedTime);
    if (eligibleIn != 0) {
        rejectList->push_back(*fe);
        ++vb->opsReject;
        return eligibleIn;
    }

    // Ok, we need disk-io. Let's create an item and release the lock.
    Item itm(v->getKey(), v->getFlags(), v->getExptime(), v->getValue(),
                v->getCksum(), v->getCas(), v->getId(), vb->getId());

    time_t now = ep_real_time();
    // (2) Existing item, deleted. Delete from disk, then delete from memory.
    if (v->hasId() && (v->isDeleted() || (!v->isLocked(now) && v->isExpired(now)))) { // These conditions have to be checked inside the lock to avoid race.
        lh.unlock();
        int rowsAffected = rwUnderlying[kvId]->del(v->getKey(), v->getId(), fe->vbId, fe->vbVersion);
        if (rowsAffected == -1) {
            rejectList->push_back(*fe);
            stats.flushFailed++;
            return 0;
        }

        assert(rowsAffected == 1 || !(std::cerr<< "Existing item being deleted failed! rowsAffected = " << rowsAffected <<std::endl));
        stats.delItems++;
        v->clearId();

        // successfully deleted from disk. Let's delete from memory.
        vb = getVBucket(fe->vbId);
        if (vb) {
            LockHolder lh2 = vb->ht.getLockedBucket(v->getKey(), &bucket_num);
            if (fe->vbVersion != vbuckets.getBucketVersion(fe->vbId)) {
                delete fe;
                return 0;
            }

            StoredValue *v2 = vb->ht.unlocked_find(v->getKey(), bucket_num, true);
            assert(v2 == v);

            // let's confirm a new set has not come for this key. Only then delete it.
            now = ep_real_time();
            if (v->isDeleted() || (!v->isLocked(now) && v->isExpired(now))) {
                bool deleted = vb->ht.unlocked_del(v->getKey(), bucket_num);
                assert(deleted);
                delete fe;
                return 0;
            } else { // undeleted, let's requeue.
                rejectList->push_back(*fe);
                return 0;
            }
        } else {
            delete fe;
            return 0;
        }
    }

    // (3) New or existing item, not deleted, needs to be saved. saved
    if (!v->isDeleted()) {
        lh.unlock();
        mutation_result result;
        rwUnderlying[kvId]->set(itm, fe->vbVersion, result);
        int rowsAffected = result.first;
        int rowId = result.second;

        if (rowsAffected == -1) {
            rejectList->push_back(*fe);
            stats.flushFailed++;
            return 0;
        }

        assert(rowsAffected == 1 || !(std::cerr<< "New/Existing item being saved failed. rowsAffected = " << rowsAffected <<std::endl));
        if (rowId > 0) {
            v->setId(rowId);
            stats.newItems++;
        }

        // successfully saved to disk. Let's check if the item has been mutated again.
        vb = getVBucket(fe->vbId);
        if (vb) {
            LockHolder lh2 = vb->ht.getLockedBucket(v->getKey(), &bucket_num);
            if (fe->vbVersion != vbuckets.getBucketVersion(fe->vbId)) {
                delete fe;
                return 0;
            }

            StoredValue *v2 = vb->ht.unlocked_find(v->getKey(), bucket_num, true);
            assert(v2 == v);

            // let's confirm a new set or delete has not come for this key. Only then delete fe.
            if (v->isDeleted()) {
                rejectList->push_back(*fe);
                return 0;
            }

            if (v->getValue() == itm.getValue()) { // no new mutation has come. Let's mark it clean and be done.
                v->markClean(NULL);
                ++stats.totalEvictable;
                delete fe;
                return 0;

            } else {
                rejectList->push_back(*fe);
                return 0;
            }

        } else {
            delete fe;
            return 0;
        }
    }
    assert(false && "we should never come here!");
    return 0;
}

int EventuallyPersistentStore::flushOne(FlushEntry &fe,
                                        FlushList *rejectList,
                                        int kvId, bool &wasRejected) {

    size_t prevRejectCount = rejectList->size();

    int rv = 0;

    if (fe.vbVersion == vbuckets.getBucketVersion(fe.getVBucketId())) {
        rv = flushOneDelOrSet(&fe, rejectList, kvId);
        if (rejectList->size() == prevRejectCount) {
            // flush operation was not rejected
            tctx[kvId]->addUncommittedItem();
        } else {
            wasRejected = true;
        }
    }

    stats.flusher_todos[kvId]--;

    return rv;

}

void EventuallyPersistentStore::queueDirty(const std::string &key,
                                           uint16_t vbid,
                                           enum queue_operation op,
                                           const value_t &value,
                                           uint32_t flags,
                                           time_t exptime,
                                           uint64_t cas,
                                           int64_t rowid,
                                           const std::string &cksum,
                                           time_t queued) {

    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (vb) {
        QueuedItem *qi = NULL;
        if (op == queue_op_set) {
            qi = new QueuedItem(key, value, vbid, op, vbuckets.getBucketVersion(vbid),
                                rowid, flags, exptime, cas, cksum, queued);
            engine.addMutationEvent(&qi->getItem());
        } else {
            qi = new QueuedItem(key, vbid, op, vbuckets.getBucketVersion(vbid), rowid, flags,
                                exptime, cas, cksum, queued);
            engine.addDeleteEvent(key, vbid, cas);
        }

        queued_item item(qi);
        vb->checkpointManager.queueDirty(item, vb);

    }
}

int EventuallyPersistentStore::restoreItem(Item &itm, enum queue_operation op)
{
    const std::string &key = itm.getKey();
    uint16_t vbid = itm.getVBucketId();
    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (!vb) {
        return -1;
    }

    DataIntegrity *di = DataIntegrity::getDi(itm.getCksumMeta());
    /* Log error if checksum does not match */
    if (!di->verifyCksum((Item *)&itm)) {
        ++stats.cksumFailed;
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                        "Restore : Checksum verification failed for the key %s\n",
                        itm.getKey().c_str());
    }

    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    LockHolder rlh(restore.mutex);
    MutationValue mv;
    if (restore.itemsDeleted.find(key) == restore.itemsDeleted.end() &&
        vb->ht.unlocked_restoreItem(itm, op, bucket_num, mv)) {
        StoredValue *v = vb->ht.unlocked_find(itm.getKey(), bucket_num, true);
        assert(v);
        if (!mv.wasDirty) {
            queueFlusher(vb, v);
        }
        return 0;
    }

    return 1;
}

bool EventuallyPersistentStore::isRestoreEnabled() {
    return engine.restore.enabled.get();
}

void EventuallyPersistentStore::completeOnlineRestore() {
    LockHolder lh(restore.mutex);
    restore.itemsDeleted.clear();
}

void EventuallyPersistentStore::warmup(Atomic<bool> &vbStateLoaded, int id) {
    LoadStorageKVPairCallback cb(vbuckets, stats, this);
    if (id == 0) {
    std::map<std::pair<uint16_t, uint16_t>, vbucket_state> state =
        roUnderlying[0]->listPersistedVbuckets();
    std::map<std::pair<uint16_t, uint16_t>, vbucket_state>::iterator it;
    for (it = state.begin(); it != state.end(); ++it) {
        std::pair<uint16_t, uint16_t> vbp = it->first;
        vbucket_state vbs = it->second;
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "Reloading vbucket %d - was in %s state\n",
                         vbp.first, vbs.state.c_str());
        cb.initVBucket(vbp.first, vbp.second, vbs.checkpointId + 1,
                       VBucket::fromString(vbs.state.c_str()));
    }
    vbStateLoaded.set(true);
    }

    roUnderlying[id]->dump(cb);
    invalidItemDbPager->createRangeList();
    // Hack Alert:: Fix for multiple vbuckets
    if (id == 0) {
        RCPtr<VBucket> vb = getVBucket(0, vbucket_state_active);
        if (vb) {
            stats.totalEvictable = vb->ht.getNumItems() - vb->ht.getNumNonResidentItems(); // FIXME
        }
    }
}

void LoadStorageKVPairCallback::initVBucket(uint16_t vbid,
                                            uint16_t vb_version,
                                            uint64_t checkpointId,
                                            vbucket_state_t prevState,
                                            vbucket_state_t newState) {
    RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
    if (!vb) {
        vb.reset(new VBucket(vbid, newState, stats));
        vbuckets.addBucket(vb);
    }
    // Set the past initial state of each vbucket.
    vb->setInitialState(prevState);
    // Pass the open checkpoint Id for each vbucket.
    vb->checkpointManager.setOpenCheckpointId(checkpointId);
    // For each vbucket, set its vbucket version.
    vbuckets.setBucketVersion(vbid, vb_version);
    // For each vbucket, set its latest checkpoint Id that was successfully persisted.
    vbuckets.setPersistenceCheckpointId(vbid, checkpointId - 1);
}

CallbackResult LoadStorageKVPairCallback::callback(GetValue &val) {
    Item *i = val.getValue();
    if (i != NULL) {
        uint16_t vb_version = vbuckets.getBucketVersion(i->getVBucketId());
        if (vb_version != static_cast<uint16_t>(-1) && val.getVBucketVersion() != vb_version) {
            epstore->getInvalidItemDbPager()->addInvalidItem(i, val.getVBucketVersion());
            delete i;
            return CB_SUCCESS;
        }

        RCPtr<VBucket> vb = vbuckets.getBucket(i->getVBucketId());
        if (!vb) {
            vb.reset(new VBucket(i->getVBucketId(), vbucket_state_dead, stats));
            vbuckets.addBucket(vb);
            vbuckets.setBucketVersion(i->getVBucketId(), val.getVBucketVersion());
        }
        bool retain(shouldBeResident());
        bool succeeded(false);

        switch (vb->ht.add(*i, false, retain)) {
        case ADD_SUCCESS:
        case ADD_UNDEL:
            // Yay
            succeeded = true;
            break;
        case ADD_EXISTS:
            // Boo
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Warmup dataload error: Duplicate key: %s.\n",
                             i->getKey().c_str());
            ++stats.warmDups;
            succeeded = true;
            break;
        case ADD_NOMEM:
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Emergency startup purge to free space for load.\n");
            purge();
            // Try that item again.
            switch(vb->ht.add(*i, false, retain)) {
            case ADD_SUCCESS:
            case ADD_UNDEL:
                succeeded = true;
                break;
            case ADD_EXISTS:
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Warmup dataload error: Duplicate key: %s.\n",
                                 i->getKey().c_str());
                ++stats.warmDups;
                succeeded = true;
                break;
            case ADD_NOMEM:
                // Store failed despite purge. Don't know where to go from here
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Cannot store an item after emergency purge.\n");
                ++stats.warmOOM;
                break;
            default:
                abort();
            }
            break;
        default:
            abort();
        }

        if (succeeded && i->isExpired(startTime)) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Item was expired at load:  %s\n",
                             i->getKey().c_str());
            epstore->del(i->getKey(), 0, i->getVBucketId(), NULL, true);
        }

        delete i;
    }
    ++stats.warmedUp;
    return CB_SUCCESS;
}

bool LoadStorageKVPairCallback::shouldBeResident() {
    size_t headroom = stats.maxDataSize * 0.1;
    return (stats.maxDataSize > (GetSelfRSS() + headroom));
}

void LoadStorageKVPairCallback::purge() {

    class EmergencyPurgeVisitor : public VBucketVisitor {
    public:
        EmergencyPurgeVisitor(EPStats &s) : stats(s) {}

        void visit(StoredValue *v) {
            if (v->ejectValue(stats, currentBucket->ht)) {
                stats.warmupEvictions++;
            }
        }
    private:
        EPStats &stats;
    };

    std::vector<int> vbucketIds(vbuckets.getBuckets());
    std::vector<int>::iterator it;
    EmergencyPurgeVisitor epv(stats);
    for (it = vbucketIds.begin(); it != vbucketIds.end(); ++it) {
        int vbid = *it;
        RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
        if (vb && epv.visitBucket(vb)) {
            vb->ht.visit(epv);
        }
    }
    getLogger()->log(EXTENSION_LOG_WARNING, NULL, "Evicted %zu keys during warmup", stats.warmupEvictions.get());
}

bool TransactionContext::enter() {
    if (!intxn) {
        _remaining = txnSize.get();
        intxn = underlying->begin();
    }
    return intxn;
}

void TransactionContext::leave(int completed) {
    _remaining -= completed;
    if (remaining() <= 0 && intxn) {
        commit();
    }
}

void TransactionContext::commit() {
    BlockTimer timer(&stats.diskCommitHisto);
    rel_time_t cstart = ep_current_time();
    while (!underlying->commit()) {
        sleep(1);
        ++stats.commitFailed[id];
    }
    ++stats.flusherCommits[id];
    rel_time_t time_diff = ep_current_time() - cstart;

    stats.commit_time.set(time_diff);
    stats.commit_times[id].set(time_diff);
    stats.cumulativeCommitTime.incr(time_diff);
    stats.cumulativeCommitTimes[id].incr(time_diff);
    intxn = false;
    //syncRegistry.itemsPersisted(uncommittedItems);
    uncommittedItems.clear();
    numUncommittedItems = 0;
}

void TransactionContext::addUncommittedItem() {
    ++numUncommittedItems;
}

VBCBAdaptor::VBCBAdaptor(EventuallyPersistentStore *s,
                         shared_ptr<VBucketVisitor> v,
                         const char *l, double sleep) :
    store(s), visitor(v), label(l), sleepTime(sleep), currentvb(0)
{
    const VBucketFilter &vbFilter = visitor->getVBucketFilter();
    size_t maxSize = store->vbuckets.getSize();
    for (size_t i = 0; i <= maxSize; ++i) {
        assert(i <= std::numeric_limits<uint16_t>::max());
        uint16_t vbid = static_cast<uint16_t>(i);
        RCPtr<VBucket> vb = store->vbuckets.getBucket(vbid);
        if (vb && vbFilter(vbid)) {
            vbList.push(vbid);
        }
    }
}

bool VBCBAdaptor::callback(Dispatcher & d, TaskId t) {
    if (!vbList.empty()) {
        currentvb = vbList.front();
        RCPtr<VBucket> vb = store->vbuckets.getBucket(currentvb);
        if (vb) {
            if (visitor->pauseVisitor()) {
                d.snooze(t, sleepTime);
                return true;
            }
            if (visitor->visitBucket(vb)) {
                vb->ht.visit(*visitor);
            }
        }
        vbList.pop();
    }

    bool isdone = vbList.empty();
    if (isdone) {
        visitor->complete();
    }
    return !isdone;
}

size_t EventuallyPersistentStore::getBlobSize(const std::string &key,
                       uint16_t vbucket) {
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb || vb->getState() != vbucket_state_active) {
        return 0;
    }
    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
    StoredValue *v = fetchValidValue(vb, key, bucket_num);
    if (!v) {
        return 0;
    }
    return v->valLength();
}

void EventuallyPersistentStore::queueFlusher(RCPtr<VBucket> vb, StoredValue *v, time_t queued) {
    uint16_t vbid = vb->getId();
    std::string key = v->getKey();

    int kvid = KVStoreMapper::getKVStoreId(key, vbid);
    int shard = rwUnderlying[kvid]->getShardId(key, vbid);

    FlushEntry *fe = new FlushEntry(v, vbid, getVBucketVersion(vbid), queued);
    assert(v->isDirty());
    ++stats.queue_size;
    ++stats.totalEnqueued;
    vb->doStatsForQueueing(sizeof(FlushEntry), v->size(), fe->queuedTime);
    --stats.totalEvictable;

    flushLists->push(kvid, shard, *fe);
}

void EventuallyPersistentStore::getFlushItems(std::list<queued_item>& flushItems, int kvId) {
    Flusher *fl = getFlusher(kvId);

    // Pause flusher
    fl->pause(FLUSHER_FLAG_RETRIEVEITEMS);
    fl->wait(paused);

    // Get flush list, reject list and helper items from flusher
    fl->retrievePendingItems(flushItems);

    // Get flushList items from epstore
    flushLists->getCopy(flushItems, kvId);

    // Resume flusher
    fl->resume(FLUSHER_FLAG_RETRIEVEITEMS);
}
