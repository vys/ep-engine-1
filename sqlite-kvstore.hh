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
#ifndef SQLITE_BASE_H
#define SQLITE_BASE_H 1

#include <map>
#include <vector>

#ifdef USE_SYSTEM_LIBSQLITE3
#include <sqlite3.h>
#else
#include "embedded/sqlite3.h"
#endif

#include "kvstore.hh"
#include "sqlite-pst.hh"
#include "sqlite-strategies.hh"
#include "item.hh"
#include "stats.hh"

class EventuallyPersistentEngine;
class EPStats;

/**
 * A persistence store based on sqlite that uses a SqliteStrategy to
 * configure itself.
 */
class StrategicSqlite3 : public KVStore {
public:

    /**
     * Construct an instance of sqlite with the given database name.
     */
    StrategicSqlite3(EPStats &st, shared_ptr<SqliteStrategy> s);

    /**
     * Copying opens a new underlying DB.
     */
    StrategicSqlite3(const StrategicSqlite3 &from);

    /**
     * Cleanup.
     */
    ~StrategicSqlite3() {
        close();
    }

    /**
     * Reset database to a clean state.
     */
    void reset();

    /**
     * Begin a transaction (if not already in one).
     */
    bool begin() {
        if(!intransaction) {
            if (execute("begin immediate") != -1) {
                intransaction = true;
            }
        }
        return intransaction;
    }

    /**
     * Commit a transaction (unless not currently in one).
     *
     * Returns false if the commit fails.
     */
    bool commit() {
        if(intransaction) {
            // If commit returns -1, we're still in a transaction.
            intransaction = (execute("commit") == -1);
        }
        // !intransaction == not in a transaction == committed
        return !intransaction;
    }

    /**
     * Rollback a transaction (unless not currently in one).
     */
    void rollback() {
        if(intransaction) {
            intransaction = false;
            execute("rollback");
        }
    }

    /**
     * Query the properties of the underlying storage.
     */
    StorageProperties getStorageProperties();

    /**
     * Overrides set().
     */
    void set(const Item &item, uint16_t vb_version, mutation_result &p);

    /**
     * Overrides get().
     */
    void get(const std::string &key, uint64_t rowid,
             uint16_t vb, uint16_t vbver, Callback<GetValue> &cb);

    /**
     * Overrides del().
     */
    int del(const std::string &key, uint64_t rowid,
             uint16_t vb, uint16_t vbver);

    bool delVBucket(uint16_t vbucket, uint16_t vb_version);

    bool delVBucket(uint16_t vbucket, uint16_t vb_version,
                    std::pair<int64_t, int64_t> row_range);

    vbucket_map_t listPersistedVbuckets(void);

    /**
     * Take a snapshot of the stats in the main DB.
     */
    bool snapshotStats(const std::map<std::string, std::string> &m);
    /**
     * Take a snapshot of the vbucket states in the main DB.
     */
    bool snapshotVBuckets(const vbucket_map_t &m);

    /**
     * Overrides dump
     */
    void dump(Callback<GetValue> &cb);

    void dump(uint16_t vb, Callback<GetValue> &cb, bool force = false);

    size_t getNumShards() {
        return strategy->getNumOfDbShards();
    }

    size_t getShardId(const std::string &key, uint16_t vbid) {
        return strategy->getDbShardId(key, vbid);
    }

    void optimizeWrites(FlushList &items) {
        strategy->optimizeWrites(items);
    }

    void destroyInvalidVBuckets(bool destroyOnlyOne = false) {
        strategy->destroyInvalidTables(destroyOnlyOne);
    }

private:
    /**
     * Shortcut to execute a simple query.
     *
     * @param query a simple query with no bindings to execute directly
     */
    int execute(const char *query) {
        PreparedStatement st(db, query);
        return st.execute();
    }

    template <typename T1, typename T2>
    bool storeMap(PreparedStatement *clearSt,
                  PreparedStatement *insSt,
                  const std::map<T1, T2> &m);

    void insert(const Item &itm, uint16_t vb_version, mutation_result &p);
    void update(const Item &itm, uint16_t vb_version, mutation_result &p);
    int64_t lastRowId();

    EPStats &stats;

    /**
     * Direct access to the DB.
     */
    sqlite3 *db;

    void open() {
        assert(strategy);
        db = strategy->open();
        intransaction = false;
    }

    void close() {
        strategy->close();
        intransaction = false;
        db = NULL;
    }

    shared_ptr<SqliteStrategy> strategy;

    bool intransaction;


    // Disallow assignment.
    void operator=(const StrategicSqlite3 &from);
};

#endif /* SQLITE_BASE_H */
