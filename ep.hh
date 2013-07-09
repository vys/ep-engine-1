/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Zynga inc.
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
#ifndef EP_HH
#define EP_HH 1

#include <pthread.h>
#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdexcept>
#include <iostream>
#include <queue>
#include <limits>
#include <unistd.h>

#include <set>
#include <list>
#include <queue>
#include <algorithm>

#include <memcached/engine.h>

extern EXTENSION_LOGGER_DESCRIPTOR *getLogger(void);

#include "queueditem.hh"
#include "stats.hh"
#include "locks.hh"
#include "kvstore.hh"
#include "stored-value.hh"
#include "sync_registry.hh"
#include "atomic.hh"
#include "dispatcher.hh"
#include "vbucket.hh"
#include "item_pager.hh"
#include "eviction.hh"
#include "flushlist.hh"
#include "crc32.hh"

#define DEFAULT_TXN_SIZE 10000
#define MAX_TXN_SIZE 10000000

#define MAX_DATA_AGE_PARAM 86400
#define MAX_BG_FETCH_DELAY 900

// To be set with different bits each
#define FLUSHER_FLAG_FLUSHPARAM    0x01
#define FLUSHER_FLAG_RETRIEVEITEMS 0x02

/**
 * vbucket-aware hashtable visitor.
 */
class VBucketVisitor : public HashTableVisitor {
public:

    VBucketVisitor() : HashTableVisitor() { }

    VBucketVisitor(const VBucketFilter &filter) :
        HashTableVisitor(), vBucketFilter(filter) { }

    /**
     * Begin visiting a bucket.
     *
     * @param vb the vbucket we are beginning to visit
     *
     * @return true iff we want to walk the hashtable in this vbucket
     */
    virtual bool visitBucket(RCPtr<VBucket> vb) {
        if (vBucketFilter(vb->getId())) {
            currentBucket = vb;
            return true;
        }
        return false;
    }

    // This is unused in all implementations so far.
    void visit(StoredValue* v) {
        (void)v;
        abort();
    }

    const VBucketFilter &getVBucketFilter() {
        return vBucketFilter;
    }

    /**
     * Called after all vbuckets have been visited.
     */
    virtual void complete() { }

    /**
     * Return true if visiting vbuckets should be paused temporarily.
     */
    virtual bool pauseVisitor() {
        return false;
    }

protected:
    VBucketFilter vBucketFilter;
    RCPtr<VBucket> currentBucket;
};

typedef std::pair<int64_t, int64_t> chunk_range;
typedef std::list<chunk_range>::iterator chunk_range_iterator;

/**
 * Collection class that maintains the sorted list of row ID chunk ranges
 * for a vbucket deletion.
 */
class VBDeletionChunkRangeList {
public:

    VBDeletionChunkRangeList() { }

    chunk_range_iterator begin() {
        return range_list.begin();
    }

    chunk_range_iterator end() {
        return range_list.end();
    }

    void add(int64_t start_id, int64_t end_id) {
        chunk_range r(start_id, end_id);
        add(r);
    }

    void add(chunk_range range) {
        if (range.first > range.second || (size() > 0 && back().second > range.first)) {
            return;
        }
        range_list.push_back(range);
    }

    const chunk_range& front() {
        return range_list.front();
    }

    const chunk_range& back() {
        return range_list.back();
    }

    size_t size() {
        return range_list.size();
    }

    /**
     * Split the given chunk range into two ranges by using the new range size
     * @param it pointer to a chunk range to be split
     * @param range_size range size used for chunk split
     */
    void splitChunkRange(chunk_range_iterator it, int64_t range_size) {
        if (it == end() || (it->second - it->first) <= range_size) {
            return;
        }

        int64_t range_end = it->second;
        it->second = it->first + range_size;
        chunk_range r(it->second + 1, range_end);
        range_list.insert(++it, r);
    }

    /**
     * Merge multiple chunk ranges into one range
     * @param start the pointer to the start chunk range for the merge operation
     * @param range_size the new range size used for merging chunk ranges
     */
    void mergeChunkRanges(chunk_range_iterator start, int64_t range_size) {
        if (start == end() || (start->second - start->first) >= range_size) {
            return;
        }
        // Find the closest chunk C1 whose end point is greater than the point advanced by
        // the new range size from the start chunk range's start point.
        chunk_range_iterator p = findClosestChunkByRangeSize(start, range_size);
        if (p != end()) {
            int64_t endpoint = start->first + range_size;
            if (p->first <= endpoint && endpoint <= p->second) {
                // Set the start chunk range's end point by using the new range size
                start->second = endpoint;
                p->first = endpoint + 1;
            } else {
                chunk_range_iterator iter = p;
                start->second = (--iter)->second;
            }
        } else { // Reached to the end of the range list
            start->second = back().second;
        }
        // Remove the list of chunks between the start chunk and the chunk C1, excluding
        // these two chunks
        removeChunkRanges(start, p);
    }

private:

    /**
     * Remove the sub list of chunk ranges between two iterator arguments, excluding the ranges
     * pointed by these two iterators.
     * @param first iterator that points to the first chunk range in the sub list
     * @param last iterator that points to the last chunk range in the sub list
     */
    void removeChunkRanges(chunk_range_iterator first, chunk_range_iterator last) {
        if (first == last || first == end() ||
            (first != end() && last != end() && first->second > last->first)) {
            return;
        }
        range_list.erase(++first, last);
    }

    /**
     * Find the closest chunk range whose end point is greater than the point advanced by
     * a specified range size from the start point of a given chunk range.
     * @param it pointer to a given chunk range
     * @param range_size range size to be advanced
     * @return the iterator that points to the chunk range found
     */
    chunk_range_iterator findClosestChunkByRangeSize(chunk_range_iterator it,
                                                     int64_t range_size) {
        chunk_range_iterator p = it;
        while (p != end() && p->second <= (it->first + range_size)) {
            ++p;
        }
        return p;
    }

    std::list<chunk_range> range_list;
};

/**
 * Hash table visitor that builds ranges of row IDs for deleting vbuckets.
 */
class VBucketDeletionVisitor : public HashTableVisitor {
public:
    /**
     * Construct a VBucketDeletionVisitor that will attempt to get all the
     * row_ids for a given vbucket from memory.
     */
    VBucketDeletionVisitor(size_t deletion_size)
        : row_ids(new std::set<int64_t>), chunk_size(deletion_size) {}

    ~VBucketDeletionVisitor() {
        if (row_ids) {
            delete row_ids;
        }
    }

    void visit(StoredValue *v) {
        if(v->hasId()) {
            row_ids->insert(v->getId());
        }
    }

    /**
     * Construct the list of chunks from the row id list for a given vbucket.
     * Note that each chunk might have a different range size as each chunk is
     * simply created by taking "chunk_size" elements from the row id list.
     *
     */
    void createRangeList(VBDeletionChunkRangeList& range_list) {
        size_t counter = 0;
        int64_t start_row_id = -1, end_row_id = -1;

        std::set<int64_t>::iterator iter;
        for (iter = row_ids->begin(); iter != row_ids->end(); ++iter) {
            ++counter;
            if (counter == 1) {
                start_row_id = *iter;
            }
            if (counter == chunk_size || iter == --(row_ids->end())) {
                end_row_id = *iter;
                chunk_range r(start_row_id, end_row_id);
                range_list.add(r);
                counter = 0;
            }
        }

        delete row_ids;
        row_ids = NULL;
    }

    std::set<int64_t>                       *row_ids;
    size_t                                   chunk_size;
};

// Forward declaration
class Flusher;
class TapBGFetchCallback;
class EventuallyPersistentStore;

/**
 * Helper class used to insert items into the storage by using
 * the KVStore::dump method to load items from the database
 */
class LoadStorageKVPairCallback : public Callback<GetValue> {
public:
    LoadStorageKVPairCallback(VBucketMap &vb, EPStats &st,
                              EventuallyPersistentStore *ep)
        : vbuckets(vb), stats(st), epstore(ep), startTime(ep_real_time()) {
        assert(epstore);
    }

    void initVBucket(uint16_t vbid,
                     uint16_t vb_version,
                     uint64_t checkpointId,
                     vbucket_state_t prevState = vbucket_state_dead,
                     vbucket_state_t newState = vbucket_state_dead);
    CallbackResult callback(GetValue &val);

private:

    bool shouldBeResident();
    void purge();

    VBucketMap &vbuckets;
    EPStats    &stats;
    EventuallyPersistentStore *epstore;
    time_t      startTime;
};

/**
 * Maintains scope of a underlying storage transaction, being useful
 * and what not.
 */
class TransactionContext {
public:

    TransactionContext(EPStats &st, KVStore *ks, SyncRegistry &syncReg, int _id)
        : stats(st), underlying(ks), _remaining(0), intxn(false), 
        syncRegistry(syncReg), id(_id) {}

    /**
     * Call this whenever entering a transaction.
     *
     * This will (when necessary) begin the tranasaction and reset the
     * counter of remaining items for a transaction.
     *
     * @return true if we're in a transaction
     */
    bool enter();

    /**
     * Called whenever leaving, having completed the given number of
     * updates.
     *
     * When the number of updates completed exceeds the number
     * permitted per transaction, a transaction will be closed and
     * reopened.
     */
    void leave(int completed);

    /**
     * Explicitly commit a transaction.
     *
     * This will reset the remaining counter and begin a new
     * transaction for the next batch.
     */
    void commit();

    /**
     * Get the number of updates permitted by this transaction.
     */
    size_t remaining() {
        return _remaining;
    }

    /**
     * Request a commit occur at the next opportunity.
     */
    void commitSoon() {
        _remaining = 0;
    }

    /**
     * Get the current number of updates permitted per transaction.
     */
    int getTxnSize() {
        return txnSize.get();
    }

    /**
     * Set the current number of updates permitted per transaction.
     */
    void setTxnSize(int to) {
        txnSize.set(to);
    }

    void addUncommittedItem();

    size_t getNumUncommittedItems() {
        return numUncommittedItems;
    }

private:
    EPStats     &stats;
    KVStore     *underlying;
    int          _remaining;
    Atomic<int>  txnSize;
    Atomic<size_t> numUncommittedItems;
    bool         intxn;
    std::list<queued_item>     uncommittedItems;
    SyncRegistry              &syncRegistry;
    int id;
};

/**
 * VBucket visitor callback adaptor.
 */
class VBCBAdaptor : public DispatcherCallback {
public:

    VBCBAdaptor(EventuallyPersistentStore *s,
                shared_ptr<VBucketVisitor> v, const char *l, double sleep=0);

    std::string description() {
        std::stringstream rv;
        rv << label << " on vb " << currentvb;
        return rv.str();
    }

    bool callback(Dispatcher &d, TaskId t);

private:
    std::queue<uint16_t>        vbList;
    EventuallyPersistentStore  *store;
    shared_ptr<VBucketVisitor>  visitor;
    const char                 *label;
    double                      sleepTime;
    uint16_t                    currentvb;

    DISALLOW_COPY_AND_ASSIGN(VBCBAdaptor);
};


class EventuallyPersistentEngine;

/**
 * Manager of all interaction with the persistence.
 */
class EventuallyPersistentStore {
public:

    EventuallyPersistentStore(EventuallyPersistentEngine &theEngine,
                              KVStore **t, bool startVb0,
                              bool concurrentDB, int numKV);

    ~EventuallyPersistentStore();

    /**
     * Set an item in the store.
     * @param item the item to set
     * @param cookie the cookie representing the client to store the item
     * @param force override access to the vbucket even if the state of the
     *              vbucket would deny mutations.
     * @return the result of the store operation
     */
    ENGINE_ERROR_CODE set(Item &item,
                          const void *cookie,
                          bool force = false);

    ENGINE_ERROR_CODE add(Item &item, const void *cookie);

    /**
     * Add an TAP backfill item into its corresponding vbucket
     * @param item the item to be added
     * @return the result of the operation
     */
    ENGINE_ERROR_CODE addTAPBackfillItem(Item &item);

    /**
     * Retrieve a value.
     *
     * @param key the key to fetch
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie the connection cookie
     * @param queueBG if true, automatically queue a background fetch if necessary
     * @param honorStates if false, fetch a result regardless of state
     * @param verifyCksum if true verify the checksum 
     *
     * @return a GetValue representing the result of the request
     */
    GetValue get(const std::string &key, uint16_t vbucket,
                 const void *cookie, bool queueBG=true,
                 bool honorStates=true, bool verifyCksum = false);

    /**
     * Retrieve a value, but update its TTL first
     *
     * @param key the key to fetch
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie the connection cookie
     * @param queueBG if true, automatically queue a background fetch if necessary
     * @param exptime the new expiry time for the object
     *
     * @return a GetValue representing the result of the request
     */
    GetValue getAndUpdateTtl(const std::string &key, uint16_t vbucket,
                             const void *cookie, bool queueBG, uint32_t exptime);

    /**
     * Retrieve an item from the disk for vkey stats
     *
     * @param key the key to fetch
     * @param vbucket the vbucket from which to retrieve the key
     * @param cookie the connection cookie
     * @param cb callback to return an item fetched from the disk
     *
     * @return a status resulting form executing the method
     */
    ENGINE_ERROR_CODE getFromUnderlying(const std::string &key,
                                        uint16_t vbucket,
                                        const void *cookie,
                                        shared_ptr<Callback<GetValue> > cb);

    protocol_binary_response_status evictKey(const std::string &key,
                                             uint16_t vbucket,
                                             const char **msg,
                                             size_t *msg_size,
                                             bool force=false);


    /**
     * Delete an item from the store
     * @param key key to delete
     * @param cas the CAS ID for a CASed delete (0 to override)
     * @param vbucket the bucket for the key
     * @param cookie the cookie representing the client
     * @param force override access to the vbucket even if the state of the
     *              vbucket would deny mutations.
     * @return the result of the delete operation
     */
    ENGINE_ERROR_CODE del(const std::string &key, uint64_t cas,
                          uint16_t vbucket, const void *cookie,
                          bool force = false);


    /**
     * Revert online update, all mutations during online update period will be reverted
     * Deleted and reset items will be marked as evicted. Newly added items will be removed
     * from hashtable. During revert period, all other clients request will be temporarily
     * blocked.
     * @return the result of the stop onlineupdate operation
     */
    protocol_binary_response_status revertOnlineUpdate(RCPtr<VBucket> vb);

    void reset();

    void setGetItemsThresholds(size_t upper, size_t lower, size_t maxChecks);

    void setMinDataAge(int to);

    /**
     * Set the background fetch delay.
     *
     * This exists for debugging and testing purposes.  It
     * artificially injects delays into background fetches that are
     * performed when the user requests an item whose value is not
     * currently resident.
     *
     * @param to how long to delay before performing a bg fetch
     */
    void setBGFetchDelay(uint32_t to) {
        bgFetchDelay = to;
    }

    void setQueueAgeCap(int to);

    void startDispatcher(void);

    void startNonIODispatcher(void);

    /**
     * Get the current dispatcher.
     *
     * You can use this to queue io related jobs.  Don't do stupid things with
     * it.
     */
    Dispatcher* getDispatcher(int i) {
        assert(dispatcher);
        return dispatcher[i];
    }

    /**
     * Get the current read-only IO dispatcher.
     */
    Dispatcher* getRODispatcher(int i) {
        assert(roDispatcher);
        return roDispatcher[i];
    }

    Dispatcher* getROBackfillDispatcher(int i) {
        assert(roBackfillDispatcher);
        return roBackfillDispatcher[i];
    }

    /**
     * True if the RW dispatcher and RO dispatcher are distinct.
     */
    bool hasSeparateRODispatcher(int i) {
        return dispatcher[i] != roDispatcher[i];
    }

    /**
     * Get the current non-io dispatcher.
     *
     * Use this dispatcher to queue non-io jobs.
     */
    Dispatcher* getNonIODispatcher(void) {
        assert(nonIODispatcher);
        return nonIODispatcher;
    }

    void stopFlusher(void);

    void startFlusher(void);

    bool pauseFlusher(void);
    bool resumeFlusher(void);
    bool pauseFlusher(int kvId);
    bool resumeFlusher(int kvId);

    /**
     * Enqueue a background fetch for a key.
     *
     * @param key the key to be bg fetched
     * @param vbucket the vbucket in which the key lives
     * @param vbver the version of the vbucket
     * @param rowid the rowid of the record within its shard
     * @param cookie the cookie of the requestor
     */
    void bgFetch(const std::string &key,
                 uint16_t vbucket,
                 uint16_t vbver,
                 uint64_t rowid,
                 const void *cookie);

    /**
     * Complete a background fetch.
     *
     * @param key the key that was fetched
     * @param vbucket the vbucket in which the key lived
     * @param vbver the vbucket version
     * @param rowid the rowid of the record within its shard
     * @param cookie the cookie of the requestor
     * @param init the timestamp of when the request came in
     */
    void completeBGFetch(const std::string &key,
                         uint16_t vbucket,
                         uint16_t vbver,
                         uint64_t rowid,
                         const void *cookie,
                         hrtime_t init);

    RCPtr<VBucket> getVBucket(uint16_t vbid);

    uint16_t getVBucketVersion(uint16_t vbv) {
        return vbuckets.getBucketVersion(vbv);
    }

    uint64_t getLastPersistedCheckpointId(uint16_t vb) {
        return vbuckets.getPersistenceCheckpointId(vb);
    }

    void snapshotVBuckets(const Priority &priority, int kvid);
    void setVBucketState(uint16_t vbid,
                         vbucket_state_t state);

    /**
     * Perform a ranged vbucket deletion.
     */
    vbucket_del_result completeVBucketDeletion(uint16_t vbid, uint16_t vb_version,
                                               std::pair<int64_t, int64_t> row_range,
                                               bool isLastChunk);
    /**
     * Perform a fast vbucket deletion.
     */
    vbucket_del_result completeVBucketDeletion(uint16_t vbid, uint16_t vbver);
    bool deleteVBucket(uint16_t vbid);

    /**
     * Reset a given vbucket from memory and disk. This differs from vbucket deletion in that
     * it does not delete the vbucket instance from memory hash table.
     */
    bool resetVBucket(uint16_t vbid, bool underlying = true);

    void visit(VBucketVisitor &visitor) {
        size_t maxSize = vbuckets.getSize();
        for (size_t i = 0; i <= maxSize; ++i) {
            assert(i <= std::numeric_limits<uint16_t>::max());
            uint16_t vbid = static_cast<uint16_t>(i);
            RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
            if (vb) {
                bool wantData = visitor.visitBucket(vb);
                // We could've lost this along the way.
                if (wantData) {
                    vb->ht.visit(visitor);
                }
            }
        }
        visitor.complete();
    }

    /**
     * Run a vbucket visitor with separate jobs per vbucket.
     *
     * Note that this is asynchronous.
     */
    void visit(shared_ptr<VBucketVisitor> visitor, const char *lbl,
               Dispatcher *d, const Priority &prio, bool isDaemon=true, double sleepTime=0) {
        d->schedule(shared_ptr<DispatcherCallback>(new VBCBAdaptor(this, visitor, lbl, sleepTime)),
                    NULL, prio, 0, isDaemon);
    }

    void warmup(Atomic<bool> &vbStateLoaded, int id);

    int getTxnSize() {
        return tctx[0]->getTxnSize();
    }

    void setTxnSize(int to) {
        for (int i = 0; i < numKVStores; ++i) {
            tctx[i]->setTxnSize(to);
        }
    }

    size_t getNumUncommittedItems() {
        size_t ret = 0;
        for (int i = 0; i < numKVStores; ++i) {
            ret += tctx[i]->getNumUncommittedItems();
        }
        return ret;
    }

    Flusher* getFlusher(int id = 0);

    bool getKeyStats(const std::string &key, uint16_t vbucket,
                     key_stats &kstats);

    bool getLocked(const std::string &key, uint16_t vbucket,
                   Callback<GetValue> &cb,
                   rel_time_t currentTime, uint32_t lockTimeout,
                   std::string &metadata, const void *cookie);

    /**
     * Retrieve the StoredValue associated with a key/vbucket pair.
     *
     * @param key the key
     * @param vbucket the vbucket's ID
     * @param honorStates if false, fetch a result regardless of state
     *
     * @return a pointer to the StoredValue associated with the key/vbucket,
     *         if any, NULL otherwise
     */
    StoredValue* getStoredValue(const std::string &key,
                                uint16_t vbucket,
                                bool honorStates = true);

    ENGINE_ERROR_CODE unlockKey(const std::string &key,
                                uint16_t vbucket,
                                uint64_t cas,
                                rel_time_t currentTime);

    // Return blob size for the given key. This gives the actual on-disk size
    // even if the key is not resident
    size_t getBlobSize(const std::string &key, uint16_t vbucket);

    KVStore* getRWUnderlying(int id) {
        // This method might also be called leakAbstraction()
        return rwUnderlying[id];
    }

    KVStore* getROUnderlying(int id) {
        // This method might also be called leakAbstraction()
        return roUnderlying[id];
    }

    KVStore* getROBackfillUnderlying(int id) {
        return roBackfillUnderlying[id];
    }

    int getNumKVStores() {
        return numKVStores;
    }

    InvalidItemDbPager* getInvalidItemDbPager() {
        return invalidItemDbPager;
    }

    void deleteExpiredItems(std::list<std::pair<uint16_t, std::string> > &);

    void scheduleVBSnapshot(const Priority &priority, int kvid);

    const VBucketMap &getVBuckets() {
        return vbuckets;
    }

    EventuallyPersistentEngine& getEPEngine() {
        return engine;
    }

    /**
     * During restore from backup we read the most recent values first
     * and works our way back until epoch.. We should therefore only
     * add values to the backup if they're not there;
     *
     * @return 0 success, 1 skipped, -1 invalid vbucket
     */
    int restoreItem(Item &itm, enum queue_operation op);

    bool isFlushAllScheduled() {
        bool val = false;
        for (size_t i = 0; i < diskFlushAll.size(); ++i) {
            val |= diskFlushAll[i].get();
        }
        return val;
    }

    void setPersistenceCheckpointId(uint16_t vbid, uint64_t checkpointId);

    bool isRestoreEnabled();

    /**
     * Complete the online restore phase by clearing the list of deleted items that
     * are received from the upstream master via TAP or from the normal clients
     */
    void completeOnlineRestore();

    StorageProperties *getStorageProperties(int kvid) { 
        return storageProperties[kvid];
    }

    void beginFlush(FlushList &out, int kvId);

    void copyItemsFromFlushList(FlushList &from, std::list<queued_item> &to) {
        for (FlushList::iterator it = from.begin(); it != from.end(); it++) {
            StoredValue *v = it->getStoredValue();

            RCPtr<VBucket> vb = getVBucket(it->getVBucketId());
            if (!vb) {
                continue;
            }

            int bucket_num = 0;
            LockHolder lhb = vb->ht.getLockedBucket(v->getKey(), &bucket_num);
            assert(v == vb->ht.unlocked_find(v->getKey(), bucket_num, true));

            uint64_t icas = v->isLocked(ep_current_time())
                ? static_cast<uint64_t>(-1)
                : v->getCas();

            if (v->isDeleted()) {
                to.push_back(new QueuedItem(v->getKey(), it->getVBucketId(), queue_op_del, -1, v->getId(),
                            v->getFlags(), v->getExptime(), icas, v->getCksum()));
            } else {
                to.push_back(new QueuedItem(v->getKey(), it->getVBucketId(), queue_op_set, -1, v->getId(),
                            v->getFlags(), v->getExptime(), icas, v->getCksum()));
            }
        }
    }

    void getFlushItems(std::list<queued_item>& flushItems, int kvId);

    /**
     * Retrieve a StoredValue and invoke a method on it.
     *
     * Note that because of complications with void/non-void methods
     * and potentially missing StoredValues along with the way I
     * actually intend to use this, I don't return any values from
     * this.
     *
     * @param key the item's key to retrieve
     * @param vbid the vbucket containing the item
     * @param f the method to invoke on the item
     * @param arg the argument to supply to the method f
     *
     * @return true if the object was found and method was invoked
     */
    template<typename A>
    bool invokeOnLockedStoredValue(const std::string &key, uint16_t vbid,
                                   void (StoredValue::* f)(A),
                                   A &arg) {
        RCPtr<VBucket> vb = getVBucket(vbid);
        if (!vb) {
            return false;
        }

        int bucket_num(0);
        LockHolder lh = vb->ht.getLockedBucket(key, &bucket_num);
        StoredValue *v = vb->ht.unlocked_find(key, bucket_num, true);

        if (v) {
            std::mem_fun(f)(v, arg);
        }
        return v != NULL;
    }

private:

    void scheduleVBDeletion(RCPtr<VBucket> vb, uint16_t vb_version, double delay);

    RCPtr<VBucket> getVBucket(uint16_t vbid, vbucket_state_t wanted_state);

    /* Queue an item to be written to persistent layer. */
    void queueDirty(const std::string &key, uint16_t vbid,
                    enum queue_operation op, const value_t &value,
                    uint32_t flags = 0, time_t exptime = 0, uint64_t cas = 0,
                    int64_t rowid = -1, const std::string &cksum = "",
                    time_t queued = -1);

    void queueFlusher(RCPtr<VBucket> vb, StoredValue *v, time_t queued = -1); 

    void requeueRejectedItems(FlushList *rejectList,
                              FlushList *flushList,
                              int kvId);
    void completeFlush(rel_time_t flush_start, int id);

    void enqueueCommit();
    int eligibleForPersistence(StoredValue*, rel_time_t);
    int flushSome(FlushList *flushItems,
                  FlushList *rejectList,
                  int kvId);
    int flushOne(FlushEntry &fe,
                 FlushList *rejectQueue,
                 int kvId,
                 bool &wasRejected);
    int flushOneDeleteAll(int id);
    int flushOneDelOrSet(FlushEntry *fe, FlushList *rejectList, int kvId);

    StoredValue *fetchValidValue(RCPtr<VBucket> vb, const std::string &key,
                                 int bucket_num, bool wantsDeleted=false);

    bool shouldPreemptFlush(size_t completed, int i) {
        return (completed > 100
                && bgFetchQueue > 0
                && !hasSeparateRODispatcher(i));
    }

    size_t getWriteQueueSize(void);

    bool hasItemsForPersistence(int id);

    friend class Flusher;
    friend class BGFetchCallback;
    friend class VKeyStatBGFetchCallback;
    friend class TapBGFetchCallback;
    friend class TapConnection;
    friend class PersistenceCallback;
    friend class Deleter;
    friend class VBCBAdaptor;

    EventuallyPersistentEngine &engine;
    EPStats                    &stats;
    StorageProperties          **storageProperties;
    bool                       doPersistence;
    KVStore                    **rwUnderlying;
    KVStore                    **roUnderlying;
    KVStore                    **roBackfillUnderlying;
    Dispatcher                 **dispatcher;
    Dispatcher                 **roDispatcher;
    Dispatcher                 **roBackfillDispatcher;
    Dispatcher                 *nonIODispatcher;
    Flusher                    **flusher;
    FlushLists                 *flushLists;
    //AtomicList<FlushEntry >    *toFlush;
    InvalidItemDbPager         *invalidItemDbPager;
    VBucketMap                 vbuckets;
    SyncObject                 mutex;
    pthread_t                  thread;
    Atomic<size_t>             bgFetchQueue;
    std::vector<Atomic<bool> > diskFlushAll;
    TransactionContext         **tctx;
    Mutex                      vbsetMutex;
    uint32_t                   bgFetchDelay;
    size_t                     getItemsUpperThreshold;
    size_t                     getItemsLowerThreshold;
    size_t                     maxGetItemsChecks;
    Atomic<size_t>             getItemsThresholdChecks;
    int                        numKVStores;
    // During restore we're bypassing the checkpoint lists with the
    // objects we're restoring, but we need them to be persisted.
    // This is solved by using a separate list for those objects.
    struct {
        Mutex mutex;
        // Maintain the list of deleted keys that are received from the upstream
        // master via TAP or from the normal clients during online restore.
        // As an alternative to std::set, we can consider boost::unordered_set later.
        std::set<std::string> itemsDeleted;
    } restore;

    DISALLOW_COPY_AND_ASSIGN(EventuallyPersistentStore);
};

/**
 * Object whose existence maintains a counter incremented.
 *
 * When the object is constructed, it increments the given counter,
 * when destructed, it decrements the counter.
 */
class BGFetchCounter {
public:

    BGFetchCounter(Atomic<size_t> &c) : counter(c) {
        ++counter;
    }

    ~BGFetchCounter() {
        --counter;
        assert(counter.get() < GIGANTOR);
    }

private:
    Atomic<size_t> &counter;
};



#endif /* EP_HH */
