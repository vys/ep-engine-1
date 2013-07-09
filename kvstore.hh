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
#ifndef KVSTORE_HH
#define KVSTORE_HH 1

#include <map>
#include <string>
#include <utility>

#include <cstring>

#include "stats.hh"
#include "item.hh"
#include "queueditem.hh"
#include "pathexpand.hh"
#include "flushlist.hh"

#define DEFAULT_KVSTORE_CONFIG "{\"kvstores\":{}}"
#define DEFAULT_DBNAME "/tmp/test.db"
#define DEFAULT_SHARDPATTERN "%d/%b-%i.sqlite"
#define DEFAULT_INITFILE ""
#define DEFAULT_POST_INITFILE ""
#define DEFAULT_DB_SHARDS 4
#define DEFAULT_DB_STRATEGY "multiDB"

/**
 * Result of database mutation operations.
 *
 * This is a pair where .first is the number of rows affected, and
 * .second is the ID that was generated (if any).  .second will be 0
 * on updates (not generating an ID).
 *
 * .first will be -1 if there was an error performing the update.
 *
 * .first will be 0 if the update did not error, but did not occur.
 * This would generally be considered a fatal condition (in practice,
 * it requires you to be firing an update at a missing rowid).
 */
typedef std::pair<int, int64_t> mutation_result;

struct vbucket_state {
    std::string state;
    uint64_t checkpointId;
};

/**
 * Type of vbucket map.
 *
 * key.first is the vbucket identifier.
 * key.second is the vbucket version
 * value is a pair of string representation of the vbucket state and
 * its latest checkpoint Id persisted.
 */
typedef std::map<std::pair<uint16_t, uint16_t>, vbucket_state> vbucket_map_t;

/**
 * Properites of the storage layer.
 *
 * If concurrent filesystem access is possible, maxConcurrency() will
 * be greater than one.  One will need to determine whether more than
 * one writer is possible as well as whether more than one reader is
 * possible.
 */
class StorageProperties {
public:

    StorageProperties(size_t c, size_t r, size_t w, bool evb, bool evd)
        : maxc(c), maxr(r), maxw(w), efficientVBDump(evb),
          efficientVBDeletion(evd) {}

    //! The maximum number of active queries.
    size_t maxConcurrency()   const { return maxc; }
    //! Maximum number of active read-only connections.
    size_t maxReaders()       const { return maxr; }
    //! Maximum number of active connections for read and write.
    size_t maxWriters()       const { return maxw; }
    //! True if we can efficiently dump a single vbucket.
    bool hasEfficientVBDump() const { return efficientVBDump; }
    //! True if we can efficiently delete a vbucket all at once.
    bool hasEfficientVBDeletion() const { return efficientVBDeletion; }

private:
    size_t maxc;
    size_t maxr;
    size_t maxw;
    bool efficientVBDump;
    bool efficientVBDeletion;
};

/**
 * Database strategy
 */
enum db_type {
    single_db,           //!< single database strategy
    multi_db,            //!< multi-database strategy
    single_mt_db,        //!< single database, multi-table strategy
    multi_mt_db,         //!< multi-database, multi-table strategy
    multi_mt_vb_db       //!< multi-db, multi-table strategy sharded by vbucket
};

/**
 * Config file for every kvstore
 */
class KVStoreConfig {
public:
    KVStoreConfig() :
        dbname(DEFAULT_DBNAME), shardpattern(DEFAULT_SHARDPATTERN),
        initfile(DEFAULT_INITFILE), postInitfile(DEFAULT_POST_INITFILE),
        dbStrategy(DEFAULT_DB_STRATEGY), dbShards(DEFAULT_DB_SHARDS), numDataDbs(0)
    {}

    KVStoreConfig(const char * dbn,
            const char * shp,
            const char * ifile,
            const char * pfile,
            const char * dbstrat,
            size_t dbshards) :
        dbname(dbn), shardpattern(shp), initfile(ifile),
        postInitfile(pfile), dbStrategy(dbstrat), dbShards(dbshards)
    {}

    std::string& getDbname() {
        return dbname;
    }

    std::string& getShardpattern() {
        return shardpattern;
    }

    std::string& getInitfile() {
        return initfile;
    }

    std::string& getPostInitfile() {
        return postInitfile;
    }

    std::string getDbStrategy() {
        return dbStrategy;
    }

    size_t getDbShards() {
        return dbShards;
    }

    size_t getNumDataDbs() {
        return numDataDbs;
    }

    std::string& getDataDbnameI(size_t i) {
        assert(i < numDataDbs);
        return dataDbnames[i];
    }

    std::string getDbShardI(size_t i) {
        PathExpander p(numDataDbs ? dataDbnames[i % numDataDbs].c_str() : dbname.c_str());
        return p.expand(shardpattern.c_str(), i);
    }

private:
    std::string dbname;
    std::string shardpattern;
    std::string initfile;
    std::string postInitfile;
    std::string dbStrategy;
    size_t dbShards;
    size_t numDataDbs;
    std::vector<std::string> dataDbnames;

    void addDataDbname(std::string s) {
        dataDbnames.push_back(s);
        numDataDbs++;
    }

    friend class KVStore;
};

/**
 * Base class representing kvstore operations.
 */
class KVStore {
public:

    /**
     * Create a KVStore with the given properties.
     *
     * @param type the type of DB to set up
     * @param stats the server stats
     * @param conf type-specific parameters
     */
    static KVStore *create(KVStoreConfig *c, EventuallyPersistentEngine &theEngine);

    /**
     * Get the name of a db type.
     */
    static const char* typeToString(enum db_type type);

    /**
     * Get the type for a given name.
     *
     * @param name the name to parse
     * @param typeOut a reference to a type to fill
     *
     * @return true if we were able to parse the type
     */
    static bool stringToType(const char *name,
                             enum db_type &typeOut);

    static std::map<std::string, KVStoreConfig*> *parseConfig(EventuallyPersistentEngine &theEngine);

    virtual ~KVStore() {}

    /**
     * Reset the store to a clean state.
     */
    virtual void reset() = 0;

    /**
     * Begin a transaction (if not already in one).
     *
     * @return false if we cannot begin a transaction
     */
    virtual bool begin() = 0;

    /**
     * Commit a transaction (unless not currently in one).
     *
     * @return false if the commit fails
     */
    virtual bool commit() = 0;

    /**
     * Rollback the current transaction.
     */
    virtual void rollback() = 0;

    /**
     * Get the properties of the underlying storage.
     */
    virtual StorageProperties getStorageProperties() = 0;

    /**
     * Set an item into the kv store.
     */
    virtual void set(const Item &item, uint16_t vb_version,
                     mutation_result &p) = 0;

    /**
     * Get an item from the kv store.
     */
    virtual void get(const std::string &key, uint64_t rowid,
                     uint16_t vb, uint16_t vbver,
                     Callback<GetValue> &cb) = 0;

    /**
     * Delete an item from the kv store.
     */
    virtual int del(const std::string &key, uint64_t rowid,
                     uint16_t vb, uint16_t vbver) = 0;

    /**
     * Bulk delete some versioned records from a vbucket.
     */
    virtual bool delVBucket(uint16_t vbucket, uint16_t vb_version) = 0;

    /**
     * Bulk delete some versioned records from a vbucket.
     */
    virtual bool delVBucket(uint16_t vbucket, uint16_t vb_version,
                            std::pair<int64_t, int64_t> row_range) = 0;

    /**
     * Get a list of all persisted vbuckets (with their versions and states).
     */
    virtual vbucket_map_t listPersistedVbuckets(void) = 0;

    /**
     * Persist a snapshot of a collection of stats.
     */
    virtual bool snapshotStats(const std::map<std::string, std::string> &m) = 0;

    /**
     * Snapshot vbucket states.
     */
    virtual bool snapshotVBuckets(const vbucket_map_t &m) = 0;

    /**
     * Pass all stored data through the given callback.
     */
    virtual void dump(Callback<GetValue> &cb) = 0;

    /**
     * Pass all stored data for the given vbucket through the given
     * callback.
     */
    virtual void dump(uint16_t vbid, Callback<GetValue> &cb, bool force = false) = 0;

    /**
     * Get the number of data shards in this kvstore.
     */
    virtual size_t getNumShards() = 0;

    /**
     * get the shard ID for the given key, vbucket
     */

    virtual size_t getShardId(const std::string &key, uint16_t vbid) = 0;

    /**
     * Before persisting a batch of data, do stuf to them that might
     * improve performance at the IO layer.
     */
    virtual void optimizeWrites(FlushList &items) = 0;

    /**
     * Remove invalid vbuckets from the underlying storage engine.
     * @param destroyOnlyOne True if this run should remove only one invalid vbucket.
     * This can be set to true if we want to delete all invalid vbuckets over the time.
     */
    virtual void destroyInvalidVBuckets(bool destroyOnlyOne = false) = 0;

    /**
     * Update vbucket to checkpointId mapping for items picked up by the flusher
     */
    void setPersistenceCheckpointId(uint16_t vbid, uint64_t cpid) {
        persistedCheckpoints[vbid] = cpid;
    }

    /**
     * Read persisted checkpointId for given vbucket
     */
    uint64_t getPersistenceCheckpointId(uint16_t vbid) {
        std::map<uint16_t, uint64_t>::iterator it = persistedCheckpoints.find(vbid);
        if (it != persistedCheckpoints.end()) {
            return (*it).second;
        } else {
            return 0;
        }
    }

    /**
     * Reset vbucket to checkpointId map when flusher has completed execution
     */
    void clearPersistenceCheckpointIds() {
        persistedCheckpoints.clear();
    }


private:
    std::map<uint16_t, uint64_t> persistedCheckpoints;

};

#endif // KVSTORE_HH
