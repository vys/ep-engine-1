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
#ifndef STATS_HH
#define STATS_HH 1

#include <map>
#include <memcached/engine.h>

#include "common.hh"
#include "atomic.hh"
#include "histo.hh"
#include "rss.hh"

#ifndef DEFAULT_MAX_DATA_SIZE
/* Something something something ought to be enough for anybody */
#define DEFAULT_MAX_DATA_SIZE (std::numeric_limits<size_t>::max())
#endif

static const hrtime_t ONE_SECOND(1000000);

class FailedEvictions {
public:
    FailedEvictions()
    {
        reset();
    }

    Atomic<uint32_t>    numKeyNotPresent;
    Atomic<uint32_t>    numDirties;
    Atomic<uint32_t>    numAlreadyEvicted;
    Atomic<uint32_t>    numPolicyIneligible;
    Atomic<uint32_t>    numInCheckpoints;
    Atomic<uint32_t>    evictionStopped;

    void reset()
    {
        numKeyNotPresent = 0;
        numDirties = 0;
        numAlreadyEvicted = 0;
        numPolicyIneligible = 0;
        numInCheckpoints = 0;
        evictionStopped = 0;
    }
};

class EvictionStats {
public:
    EvictionStats ()
    {
        // We don't want to reset static variables during construction
        reset_();
    }

    Atomic<uint32_t>        numTotalEvictions;
    Atomic<uint32_t>        numEvictions;
    Atomic<uint32_t>        numTotalKeysEvicted; // Total evictions so far
    Atomic<uint32_t>        numKeysEvicted;      // Evictions in this run
    Atomic<uint32_t>        numEmptyQueue;
    Atomic<uint32_t>        numActiveListItems;
    Atomic<uint32_t>        numInactiveListItems;
    Atomic<uint32_t>        numMaxQuanta;
    Atomic<uint32_t>        frontendSwaps;
    Atomic<uint32_t>        backgroundSwaps;
    //! Total size of objects used by eviction.
    Atomic<size_t>          memSize;
    FailedEvictions         failedTotal;         // All failures so far
    // Add histogram structure here
    Histogram<rel_time_t>   evictItemAges;

    void reset()
    {
        reset_();
        EvictionStats::memSize = 0;
    }

private:
    void reset_()
    {
        numTotalEvictions = 0;
        numEvictions = 0;
        numTotalKeysEvicted = 0;
        numEmptyQueue = 0;
        numMaxQuanta = 0;
        numKeysEvicted = 0;
        numActiveListItems = 0;
        numInactiveListItems = 0;
        frontendSwaps = 0;
        backgroundSwaps = 0;
        failedTotal.reset();
        evictItemAges.reset();
    }
};

class EvictionPruneStats {
public:
    EvictionPruneStats() {
        reset();
    }

    Atomic<uint32_t> numPruneRuns;
    Atomic<uint64_t> numKeysPruned;

    void reset() {
        numPruneRuns = 0;
        numKeysPruned = 0;
    }
};

// Timing stats for expiry pager
class ExpiryPagerTimeStats {
public:
    ExpiryPagerTimeStats() : startTime(0), endTime(0) {}

    void reset() {
        startTime = 0;
        endTime = 0;
    }

    void getStats(const void *cookie, ADD_STAT add_stat);

    time_t startTime;
    time_t endTime;
};

/**
 * Global engine stats container.
 */
class EPStats {
public:

    EPStats() : maxDataSize(DEFAULT_MAX_DATA_SIZE),
                dirtyAgeHisto(GrowingWidthGenerator<hrtime_t>(0, ONE_SECOND, 1.4), 25),
                dataAgeHisto(GrowingWidthGenerator<hrtime_t>(0, ONE_SECOND, 1.4), 25),
                diskCommitHisto(GrowingWidthGenerator<hrtime_t>(0, ONE_SECOND, 1.4), 25 ) {
                    for (int i = 0; i < 2; i++) {
                        itemAgeHisto[i] = new Histogram <int>;
                        diskItemSizeHisto[i] = new Histogram <size_t>;
                        memItemSizeHisto[i] = new Histogram <size_t>;
                    }
     }

    ~EPStats() {

        for (int i = 0; i < 2; i++) {
            delete itemAgeHisto[i];
            delete diskItemSizeHisto[i];
            delete memItemSizeHisto[i];
        }
    }

    //numBlobs
    Atomic<size_t>numBlobs;
    // How to map keys,vbuckets to kvstores
    Atomic<bool> kvstoreMapVbuckets;
    //! How long it took us to load the data from disk.
    Atomic<hrtime_t> warmupTime;
    //! Whether we're warming up.
    Atomic<bool> warmupComplete;
    //! Number of records warmed up.
    Atomic<size_t> warmedUp;
    //! Number of warmup failures due to duplicates
    Atomic<size_t> warmDups;
    //! Number of OOM failures at warmup time.
    Atomic<size_t> warmOOM;
    //! Number of evictions at warmup time.
    Atomic<size_t> warmupEvictions;

    //! size of the input queue
    Atomic<size_t> queue_size;
    //! Number of times flusher left rejected items to the flushQueue
    std::vector<Atomic<size_t> > flusherRequeuedRejected;
    //! Size of the in-process (output) queue for each flusher
    std::vector<Atomic<size_t> > flusher_todos;
    //! Number of deduplications fixed by the flusher
    std::vector<Atomic<size_t> > flusherDedup;
    //! Number of transaction commits.
    std::vector<Atomic<size_t> > flusherCommits;
    //! Number of times the flusher was preempted for a read
    std::vector<Atomic<size_t> > flusherPreempts;
    //! Total time spent flushing.
    Atomic<size_t> cumulativeFlushTime;
    //! Total time spent flushing per flusher.
    std::vector<Atomic<size_t> > cumulativeFlushTimes;
    //! Total time spent committing.
    Atomic<size_t> cumulativeCommitTime;
    //! Total time spent committing per transaction context.
    std::vector<Atomic<size_t> > cumulativeCommitTimes;
    //! Objects that were rejected from persistence for being too fresh.
    Atomic<size_t> tooYoung;
    //! Objects that were forced into persistence for being too old.
    Atomic<size_t> tooOld;
    //! Number of items persisted.
    Atomic<size_t> totalPersisted;
    //! Cumulative number of items added to the queue.
    Atomic<size_t> totalEnqueued;
    //! Number of new items created in the DB.
    Atomic<size_t> newItems;
    //! Number of items removed from the DB.
    Atomic<size_t> delItems;
    //! Number of times an item flush failed.
    Atomic<size_t> flushFailed;
    //! Number of times an item is not flushed due to the item's expiry
    Atomic<size_t> flushExpired;
    //! Number of times an object was expired on access.
    Atomic<size_t> expired;
    //! Number of times we failed to start a transaction
    std::vector<Atomic<size_t> > beginFailed;
    //! Number of times a commit failed.
    std::vector<Atomic<size_t> > commitFailed;
    //! How long an object is dirty before written.
    Atomic<rel_time_t> dirtyAge;
    //! Oldest enqueued object we've seen while persisting.
    Atomic<rel_time_t> dirtyAgeHighWat;
    //! How old persisted data was when it hit the persistence layer
    Atomic<rel_time_t> dataAge;
    //! Oldest data we've seen while persisting.
    Atomic<rel_time_t> dataAgeHighWat;
    //! How long does it take to do an entire flush cycle.
    Atomic<rel_time_t> flushDuration;
    //! How long does it take to do an entire flush cycle per flusher.
    std::vector<Atomic<rel_time_t> > flushDurations;
    //! Longest flush cycle we've seen.
    Atomic<rel_time_t> flushDurationHighWat;
    //! Longest flush cycle we've seen per flusher.
    std::vector<Atomic<rel_time_t> > flushDurationHighWats;
    //! Amount of time spent in the last commit phase.
    Atomic<rel_time_t> commit_time;
    //! Amount of time spent in the last commit phase for each kvstore.
    std::vector<Atomic<rel_time_t> > commit_times;
    //! Number of times we deleted a vbucket.
    Atomic<size_t> vbucketDeletions;
    //! Number of times we failed to delete a vbucket.
    Atomic<size_t> vbucketDeletionFail;
    //! Beyond this point are config items
    //! Minimum data age before a record can be persisted
    Atomic<int> min_data_age;
    //! Maximum data age before a record is forced to be persisted
    Atomic<int> queue_age_cap;
    //! Number of times background fetches occurred.
    Atomic<size_t> bg_fetched;
    //! Number of times background fetches failed due to OOM.
    Atomic<size_t> bg_fetch_failed;
    //! Number of times we needed to kick in the pager
    Atomic<size_t> pagerRuns;
    //! Number of times the expiry pager runs for purging expired items
    Atomic<size_t> expiryPagerRuns;
    //! Number of times the checkpoint remover runs for removing closed unreferenced checkpoints.
    Atomic<size_t> checkpointRemoverRuns;
    //! Number of items removed from closed unreferenced checkpoints.
    Atomic<size_t> itemsRemovedFromCheckpoints;
    //! Number of times a value is ejected
    Atomic<size_t> numValueEjects;
    //! Number of times a replica value is ejected
    Atomic<size_t> numReplicaEjects;
    //! Number of times a value could not be ejected
    Atomic<size_t> numFailedEjects;
    //! Number of times "Not my bucket" happened
    Atomic<size_t> numNotMyVBuckets;
    //! Whether the DB cleaner completes cleaning up invalid items with old vb versions
    Atomic<bool> dbCleanerComplete;
    //! Number of deleted items reverted from hot reload
    Atomic<size_t> numRevertDeletes;
    //! Number of new items reverted from hot reload
    Atomic<size_t> numRevertAdds;
    //! Number of updated items reverted from hot reload
    Atomic<size_t> numRevertUpdates;
    //! Max allowable memory size.
    Atomic<size_t> maxDataSize;
    //! Total size of stored objects.
    Atomic<size_t> currentSize;
    //! Total memory overhead to store values for resident keys.
    Atomic<size_t> totalValueSize;
    //! Amount of memory used to track items and what-not.
    Atomic<size_t> memOverhead;

    //! Pager low water mark.
    Atomic<size_t> mem_low_wat;
    //! Pager high water mark
    Atomic<size_t> mem_high_wat;

    //! Number of times unrecoverable oom errors happened while processing operations.
    Atomic<size_t> oom_errors;
    //! Number of times temporary oom errors encountered while processing operations.
    Atomic<size_t> tmp_oom_errors;

    //! Number of read related io operations
    Atomic<size_t> io_num_read;
    //! Number of write related io operations
    Atomic<size_t> io_num_write;
    //! Number of bytes read
    Atomic<size_t> io_read_bytes;
    //! Number of bytes written
    Atomic<size_t> io_write_bytes;

    //! Number of ops blocked on all vbuckets in pending state
    Atomic<size_t> pendingOps;
    //! Total number of ops ever blocked on all vbuckets in pending state
    Atomic<size_t> pendingOpsTotal;
    //! High water value for ops blocked for any individual pending vbucket
    Atomic<size_t> pendingOpsMax;
    //! High water value for time an op is blocked on a pending vbucket
    Atomic<hrtime_t> pendingOpsMaxDuration;

    /*stats for getl*/
    //!Number of times getl succeeded
    Atomic<size_t> getl_hits;
    //!Number of times getl failed due to wrong vbucket
    Atomic<size_t> getl_misses_notmyvbuckets;
    //!Number of times getl failed because it was already locked
    Atomic<size_t> getl_misses_locked;
    //!Number of times getl failed because key was not found
    Atomic<size_t> getl_misses_notfound;
    //!Number of times unlock was issued
    Atomic<size_t> num_unlocks;

    /*stats for append and prepend*/
    //!Number of times append succeeded
    Atomic<size_t> append_hits;
    //!Number of times append failed
    Atomic<size_t> append_fails;
    //!Number of times prepend succeeded
    Atomic<size_t> prepend_hits;
    //!Number of times prepend failed
    Atomic<size_t> prepend_fails;

    //! Histogram of pending operation wait times.
    Histogram<hrtime_t> pendingOpsHisto;

    //! Histogram of memory ages for items
    Histogram<uint32_t> itemMemoryAgeHisto;
    //! Histogram of disk ages for items
    Histogram<uint32_t> itemDiskAgeHisto;

    // Following three histograms are built during hashtable walks.
    // Double buffering is used to avoid the scenario where continuous hashtable
    // visitor runs leave the histograms incomplete and unusable

    // Histogram of age of items
    AtomicPtr<Histogram<int> > itemAgeHisto[2];

    // Histogram of sizes of items that are in memory
    AtomicPtr<Histogram<size_t> > memItemSizeHisto[2];
    // Histogram of sizes of items that are not in memory
    AtomicPtr<Histogram<size_t> > diskItemSizeHisto[2];

    //! The number of samples the bgWaitDelta and bgLoadDelta contains of
    Atomic<size_t> bgNumOperations;
    /** The sum of the deltas (in usec) from an item was put in queue until
     *  the dispatcher started the work for this item
     */
    Atomic<hrtime_t> bgWait;
    //! The shortest wait time
    Atomic<hrtime_t> bgMinWait;
    //! The longest wait time
    Atomic<hrtime_t> bgMaxWait;

    //! Histogram of background wait times.
    Histogram<hrtime_t> bgWaitHisto;

    /** The sum of the deltas (in usec) from the dispatcher started to load
     *  item until was done
     */
    Atomic<hrtime_t> bgLoad;
    //! The shortest load time
    Atomic<hrtime_t> bgMinLoad;
    //! The longest load time
    Atomic<hrtime_t> bgMaxLoad;

    //! Max wall time of deleting a vbucket
    Atomic<hrtime_t> vbucketDelMaxWalltime;
    //! Total wall time of deleting vbuckets
    Atomic<hrtime_t> vbucketDelTotWalltime;

    //! Histogram of background wait loads.
    Histogram<hrtime_t> bgLoadHisto;

    //! The total number of successful flush_all calls
    Atomic<size_t> flushHits;

    //! Current number of soft deletes in waiting
    Atomic<size_t> softDeletes;

    /* TAP related stats */
    //! The total number of tap events sent (not including noops)
    Atomic<size_t> numTapFetched;
    //! Number of background fetched tap items
    Atomic<size_t> numTapBGFetched;
    //! Number of times a tap background fetch task is requeued
    Atomic<size_t> numTapBGFetchRequeued;
    //! Number of foreground fetched tap items
    Atomic<size_t> numTapFGFetched;
    //! Number of tap deletes.
    Atomic<size_t> numTapDeletes;
    //! The number of samples the tapBgWaitDelta and tapBgLoadDelta contains of
    Atomic<size_t> tapBgNumOperations;
    //! The number of tap notify messages throttled by TapThrottle.
    Atomic<size_t> tapThrottled;
    //! Percentage of memory in use before we throttle tap input
    Atomic<double> tapThrottleThreshold;
    //! Persistence queue size threshold when tap should throttle
    Atomic<size_t> tapThrottlePersistThreshold;

    /** The sum of the deltas (in usec) from a tap item was put in queue until
     *  the dispatcher started the work for this item
     */
    Atomic<hrtime_t> tapBgWait;
    //! The shortest tap bg wait time
    Atomic<hrtime_t> tapBgMinWait;
    //! The longest tap bg wait time
    Atomic<hrtime_t> tapBgMaxWait;
    std::vector<Atomic<size_t> > backfillFlushItems;

    //! Histogram of tap background wait loads.
    Histogram<hrtime_t> tapBgWaitHisto;

    /** The sum of the deltas (in usec) from the dispatcher started to load
     *  a tap item until was done
     */
    Atomic<hrtime_t> tapBgLoad;
    //! The shortest tap load time
    Atomic<hrtime_t> tapBgMinLoad;
    //! The longest tap load time
    Atomic<hrtime_t> tapBgMaxLoad;

    /* checksum counters */
    //! Number of times checksum failed
    Atomic<size_t> cksumFailed;

    //! Number of times key got ejected due to checksum
    Atomic<size_t> ejectedDueToChecksum;

    //! Number of times key got invalid checksum string
    Atomic<size_t> invalidCksumString;

    //! Histogram of tap background wait loads.
    Histogram<hrtime_t> tapBgLoadHisto;

    //! Histogram of queue processing dirty age.
    Histogram<hrtime_t> dirtyAgeHisto;
    //! Histogram of queue processing data age.
    Histogram<hrtime_t> dataAgeHisto;

    //
    // Command timers
    //

    //! Histogram of getvbucket timings
    Histogram<hrtime_t> getVbucketCmdHisto;

    //! Histogram of setvbucket timings
    Histogram<hrtime_t> setVbucketCmdHisto;

    //! Histogram of delvbucket timings
    Histogram<hrtime_t> delVbucketCmdHisto;

    //! Histogram of get commands.
    Histogram<hrtime_t> getCmdHisto;

    //! Histogram of store commands.
    Histogram<hrtime_t> storeCmdHisto;

    //! Histogram of arithmetic commands.
    Histogram<hrtime_t> arithCmdHisto;

    //! Histogram of tap VBucket reset timings
    Histogram<hrtime_t> tapVbucketResetHisto;

    //! Histogram of tap mutation timings.
    Histogram<hrtime_t> tapMutationHisto;

    //! Histogram of tap vbucket set timings.
    Histogram<hrtime_t> tapVbucketSetHisto;

    //! Time spent notifying completion of IO.
    Histogram<hrtime_t> notifyIOHisto;

    //
    // DB timers.
    //

    //! Histogram of insert disk writes
    Histogram<hrtime_t> diskInsertHisto;

    //! Histogram of update disk writes
    Histogram<hrtime_t> diskUpdateHisto;

    //! Histogram of delete disk writes
    Histogram<hrtime_t> diskDelHisto;

    //! Histogram of execution time of disk vbucket chunk deletions
    Histogram<hrtime_t> diskVBChunkDelHisto;

    //! Histogram of execution time of disk vbucket deletions
    Histogram<hrtime_t> diskVBDelHisto;

    //! Histogram of execution time of invalid vbucket table deletions from disk
    Histogram<hrtime_t> diskInvalidVBTableDelHisto;

    //! Histogram of disk commits
    Histogram<hrtime_t> diskCommitHisto;

    //! Histogram of purging a chunk of items with the old vbucket version from disk
    Histogram<hrtime_t> diskInvaidItemDelHisto;

    Histogram<hrtime_t> checkpointRevertHisto;

    Atomic<size_t> totalEvictable;

    EvictionStats evictionStats;
    EvictionPruneStats pruneStats;
    ExpiryPagerTimeStats expiryPagerTimeStats;

    //! Reset all stats to reasonable values.
    void reset() {
        tooYoung.set(0);
        tooOld.set(0);
        dirtyAge.set(0);
        dirtyAgeHighWat.set(0);
        flushDuration.set(0);
        flushDurationHighWat.set(0);
        commit_time.set(0);
        pagerRuns.set(0);
        checkpointRemoverRuns.set(0);
        itemsRemovedFromCheckpoints.set(0);
        numValueEjects.set(0);
        numFailedEjects.set(0);
        io_num_read.set(0);
        io_num_write.set(0);
        io_read_bytes.set(0);
        io_write_bytes.set(0);
        bgNumOperations.set(0);
        bgWait.set(0);
        bgLoad.set(0);
        bgMinWait.set(999999999);
        bgMaxWait.set(0);
        bgMinLoad.set(999999999);
        bgMaxLoad.set(0);
        tapBgNumOperations.set(0);
        tapBgWait.set(0);
        tapBgLoad.set(0);
        tapBgMinWait.set(999999999);
        tapBgMaxWait.set(0);
        tapBgMinLoad.set(999999999);
        tapBgMaxLoad.set(0);
        tapThrottled.set(0);
        pendingOps.set(0);
        pendingOpsTotal.set(0);
        pendingOpsMax.set(0);
        pendingOpsMaxDuration.set(0);
        numTapFetched.set(0);
        vbucketDelMaxWalltime.set(0);
        vbucketDelTotWalltime.set(0);

        pendingOpsHisto.reset();
        bgWaitHisto.reset();
        bgLoadHisto.reset();
        tapBgWaitHisto.reset();
        tapBgLoadHisto.reset();
        getVbucketCmdHisto.reset();
        setVbucketCmdHisto.reset();
        delVbucketCmdHisto.reset();
        getCmdHisto.reset();
        storeCmdHisto.reset();
        arithCmdHisto.reset();
        tapVbucketResetHisto.reset();
        tapMutationHisto.reset();
        tapVbucketSetHisto.reset();
        notifyIOHisto.reset();
        diskInsertHisto.reset();
        diskUpdateHisto.reset();
        diskDelHisto.reset();
        diskVBChunkDelHisto.reset();
        diskVBDelHisto.reset();
        diskInvalidVBTableDelHisto.reset();
        diskCommitHisto.reset();
        diskInvaidItemDelHisto.reset();

        dataAgeHisto.reset();
        dirtyAgeHisto.reset();

        itemMemoryAgeHisto.reset();
        itemDiskAgeHisto.reset();
    }
    
    size_t flusher_todo_get() {
        size_t l = flusher_todos.size();
        size_t sum = 0;
        for (size_t i = 0; i < l; i++) {
            sum += flusher_todos[i];
        }
        return sum;
    }

    size_t flusherDedup_get() {
        size_t l = flusherDedup.size();
        size_t sum = 0;
        for (size_t i = 0; i < l; i++) {
            sum += flusherDedup[i];
        }
        return sum;
    }

    size_t flusherCommits_get() {
        size_t l = flusherCommits.size();
        size_t sum = 0;
        for (size_t i = 0; i < l; i++) {
            sum += flusherCommits[i];
        }
        return sum;
    }

    size_t flusherPreempts_get() {
        size_t l = flusherPreempts.size();
        size_t sum = 0;
        for (size_t i = 0; i < l; i++) {
            sum += flusherPreempts[i];
        }
        return sum;
    }

    size_t beginFailed_get() {
        size_t l = beginFailed.size();
        size_t sum = 0;
        for (size_t i = 0; i < l; i++) {
            sum += beginFailed[i];
        }
        return sum;
    }

    size_t commitFailed_get() {
        size_t l = commitFailed.size();
        size_t sum = 0;
        for (size_t i = 0; i < l; i++) {
            sum += commitFailed[i];
        }
        return sum;
    }

private:

    DISALLOW_COPY_AND_ASSIGN(EPStats);
};

/**
 * Stats returned by key stats.
 */
struct key_stats {
    //! The item's CAS
    uint64_t cas;
    //! The expiration time of the itme
    rel_time_t exptime;
    //! When the item was dirtied (if applicable).
    rel_time_t dirtied;
    //! How long the item has been dirty (if applicable).
    rel_time_t data_age;
    //! Last modification time
    rel_time_t last_modification_time;
    //! The item's current flags.
    uint32_t flags;
    //! True if this item is dirty.
    bool dirty;
    //! Resident?
    bool resident;
    //! 1 if in LRU, 0 if not, -1 LRU is being built
    int in_lru;
};

/**
 * Stats returned by the underlying memory allocator.
 */
class MemoryAllocatorStats {
public:
    static void getAllocatorStats(std::map<std::string, size_t> &allocator_stats) {
        size_t mapped = GetSelfRSS();
        allocator_stats.insert(std::pair<std::string, size_t>("process_rss", mapped));

#if defined(HAVE_LIBTCMALLOC) || defined(HAVE_LIBTCMALLOC_MINIMAL)
        TCMallocStats::getStats(allocator_stats);
#elif defined(HAVE_JEMALLOC_JEMALLOC_H)
#if 0
        size_t allocated = JemallocStats::getJemallocAllocated();
        size_t active = JemallocStats::getJemallocActive();
        allocator_stats.insert(std::pair<std::string, size_t>("jemalloc_stats.allocated",
                                                   allocated));
        allocator_stats.insert(std::pair<std::string, size_t>("jemalloc_stats.active",
                                                   active));
#endif
#else 
        (void) allocator_stats;
#endif
    }
};

typedef unordered_map< std::string, Histogram<hrtime_t>*> StatsMap;

class ExtStats {
public:
    ExtStats() {}
    ~ExtStats() {
        StatsMap::iterator iter = smap.begin();
        while (iter != smap.end()) {
            delete iter->second;
            iter++;
        }
    }
    void add(std::string key, hrtime_t value) {
        StatsMap::iterator iter;
        if ((iter = smap.find(key)) == smap.end()) {
            Histogram<hrtime_t> *histo = new Histogram<hrtime_t>;   
            addnew(key, histo);
            histo->add(value);
        }
        else {
            Histogram<hrtime_t> *histo = iter->second;
            histo->add(value);
        }
    }

    const StatsMap &getStats() {
        return smap;
    }

private:
    void addnew(std::string key, Histogram<hrtime_t> *&histo) {
        StatsMap::iterator iter;
        LockHolder lh(smutex);
        if ((iter = smap.find(key)) == smap.end()) {
            smap[key] = histo;
        }
        else {
            delete histo; 
            histo = iter->second;
        }
    }
    StatsMap smap;
    Mutex smutex;       
};

#endif /* STATS_HH */
