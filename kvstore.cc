/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#include <string>
#include <map>
#include <sys/stat.h>
#include <fstream>

#include "common.hh"
#include "ep_engine.h"
#include "stats.hh"
#include "kvstore.hh"
#include "sqlite-kvstore.hh"
#include "tools/cJSON.h"

KVStore *KVStore::create(KVStoreConfig *c, EventuallyPersistentEngine &theEngine) {
    Configuration &conf = theEngine.getConfiguration();
    SqliteStrategy *sqliteInstance = NULL;

    enum db_type type = multi_db;

    if (!KVStore::stringToType(c->getDbStrategy().c_str(), type)) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Unhandled db type: %s", c->getDbStrategy().c_str());
        return NULL;
    }

    switch (type) {
    case multi_db:
        sqliteInstance = new MultiDBSingleTableSqliteStrategy(c);
        break;
    case single_db:
        sqliteInstance = new SingleTableSqliteStrategy(c);
        break;
    case single_mt_db:
        sqliteInstance = new MultiTableSqliteStrategy(c, conf.getMaxVbuckets());
        break;
    case multi_mt_db:
        sqliteInstance = new ShardedMultiTableSqliteStrategy(c, conf.getMaxVbuckets());
        break;
    case multi_mt_vb_db:
        sqliteInstance = new ShardedByVBucketSqliteStrategy(c, conf.getMaxVbuckets());
        break;
    }

    return new StrategicSqlite3(theEngine.getEpStats(),
                                shared_ptr<SqliteStrategy>(sqliteInstance));
}

static const char* MULTI_DB_NAME("multiDB");
static const char* SINGLE_DB_NAME("singleDB");
static const char* SINGLE_MT_DB_NAME("singleMTDB");
static const char* MULTI_MT_DB_NAME("multiMTDB");
static const char* MULTI_MT_VB_DB_NAME("multiMTVBDB");

const char* KVStore::typeToString(db_type type) {
    char *rv(NULL);
    switch (type) {
    case multi_db:
        return MULTI_DB_NAME;
        break;
    case single_db:
        return SINGLE_DB_NAME;
        break;
    case single_mt_db:
        return SINGLE_MT_DB_NAME;
        break;
    case multi_mt_db:
        return MULTI_MT_DB_NAME;
        break;
    case multi_mt_vb_db:
        return MULTI_MT_VB_DB_NAME;
        break;
    }
    assert(rv);
    return rv;
}

bool KVStore::stringToType(const char *name,
                           enum db_type &typeOut) {
    bool rv(true);
    if (strcmp(name, MULTI_DB_NAME) == 0) {
        typeOut = multi_db;
    } else if(strcmp(name, SINGLE_DB_NAME) == 0) {
        typeOut = single_db;
    } else if(strcmp(name, SINGLE_MT_DB_NAME) == 0) {
        typeOut = single_mt_db;
    } else if(strcmp(name, MULTI_MT_DB_NAME) == 0) {
        typeOut = multi_mt_db;
    } else if(strcmp(name, MULTI_MT_VB_DB_NAME) == 0) {
        typeOut = multi_mt_vb_db;
    } else {
        rv = false;
    }
    return rv;
}

#define JSON_ERROR(msg) \
    getLogger()->log(EXTENSION_LOG_WARNING, NULL, \
            "%s: %s", \
            msg, \
            confFile.c_str()); \
    if (c != NULL) { \
        cJSON_Delete(c); \
    } \
    delete confMap; \
    return NULL;

#define CHECK_UNIQUE(str) \
    if (uniques.find(str) != uniques.end()) { \
        getLogger()->log(EXTENSION_LOG_WARNING, NULL, \
                "This dbname will repeat when sharded based on pattern: %s", \
                str.c_str()); \
        return false; \
    } \
    uniques.insert(str);

static bool validateConfig(std::map<std::string, KVStoreConfig*> *confMap) {
    std::set<std::string> uniques;
    for (std::map<std::string, KVStoreConfig*>::iterator it = confMap->begin();
            it != confMap->end(); it++) {
        KVStoreConfig *kvc = it->second;
        CHECK_UNIQUE(kvc->getDbname());
        int s = kvc->getDbShards();
        for (int i = 0; i < s; i++) {
            CHECK_UNIQUE(kvc->getDbShardI(i));
        }
    }
    return true;
}

std::map<std::string, KVStoreConfig*> *KVStore::parseConfig(EventuallyPersistentEngine &theEngine) {
    std::map<std::string, KVStoreConfig*> *confMap = new std::map<std::string, KVStoreConfig*>;
    cJSON *c = NULL, *kvstores, *kvstore, *kvparam, *dbnames, *dbname;

    std::string confFile = theEngine.getConfiguration().getKvstoreConfigFile();
    char *data;

    if (confFile.empty()) {
        data = strdup(DEFAULT_KVSTORE_CONFIG);
    } else {
        struct stat st;
        if (stat(confFile.c_str(), &st) == -1) {
            JSON_ERROR("Cannot read KVStore config file");
        } else {
            data = (char*) malloc((st.st_size + 1) * sizeof(char));
            data[st.st_size] = 0;
            std::ifstream input(confFile.c_str());
            input.read(data, st.st_size);
            input.close();
        }
    }

    if ((c = cJSON_Parse(data)) == NULL) {
        JSON_ERROR("Parse error in JSON file");
    }

    if (cJSON_GetArraySize(c) != 1) {
        JSON_ERROR("The JSON file should contain exactly one object with name 'kvstores'");
    }

    if ((kvstores = cJSON_GetObjectItem(c, "kvstores")) == NULL) {
        JSON_ERROR("Cannot find object 'kvstores' in JSON");
    }

    int k = cJSON_GetArraySize(kvstores);

    // Handle the empty case
    if (k == 0) {
        (*confMap)["kvstore"] = new KVStoreConfig;
        cJSON_Delete(c);
        free(data);
        return confMap;
    }

    for (int i = 0; i < k; i++) {
        kvstore = cJSON_GetArrayItem(kvstores, i);
        if (kvstore->type != cJSON_Object) {
            JSON_ERROR("'kvstores' has an entity of a non-object type");
        }
        std::string s(kvstore->string);
        if (confMap->find(s) != confMap->end()) {
            JSON_ERROR("Duplicate kvstore names in 'kvstores'");
        }

        // Create the KVStoreConfig for this kvstore
        KVStoreConfig *kvc = new KVStoreConfig;
        std::set<std::string> unique_keys;
        int l = cJSON_GetArraySize(kvstore);
        for (int j = 0; j < l; j++) {
            kvparam = cJSON_GetArrayItem(kvstore, j);
            std::string t(kvparam->string);
            if (unique_keys.find(t) != unique_keys.end()) {
                JSON_ERROR("Duplicate parameter in kvstore");
            }
            unique_keys.insert(t);

            if (t.compare("dbname") == 0) {
                if (kvparam->type != cJSON_String) {
                    JSON_ERROR("Parameter dbname must have a string value");
                }
                kvc->dbname = kvparam->valuestring;
            } else if (t.compare("shardpattern") == 0) {
                if (kvparam->type != cJSON_String) {
                    JSON_ERROR("Parameter shardpattern must have a string value");
                }
                kvc->shardpattern = kvparam->valuestring;
                assert(kvc->shardpattern.size());
            } else if (t.compare("initfile") == 0) {
                if (kvparam->type != cJSON_String) {
                    JSON_ERROR("Parameter initfile must have a string value");
                }
                kvc->initfile = kvparam->valuestring;
            } else if (t.compare("postInitfile") == 0) {
                if (kvparam->type != cJSON_String) {
                    JSON_ERROR("Parameter postInitfile must have a string value");
                }
                kvc->postInitfile = kvparam->valuestring;
            } else if (t.compare("db_strategy") == 0) {
                if (kvparam->type != cJSON_String) {
                    JSON_ERROR("Parameter db_strategy must have a string value");
                }
                kvc->dbStrategy = kvparam->valuestring;
            } else if (t.compare("db_shards") == 0) {
                if (kvparam->type != cJSON_Number) {
                    JSON_ERROR("Parameter db_shards must have an integer value");
                }
                kvc->dbShards = kvparam->valueint;
            } else if (t.compare("data_dbnames") == 0) {
                if (kvparam->type != cJSON_Array) {
                    JSON_ERROR("Parameter data_dbnames must be an array of string values");
                }
                dbnames = cJSON_GetObjectItem(kvstore, "data_dbnames");
                int m = cJSON_GetArraySize(dbnames);
                std::set<std::string> unique_dbnames;
                for (int n = 0; n < m; n++) {
                    dbname = cJSON_GetArrayItem(dbnames, n);
                    if (dbname->type != cJSON_String) {
                        JSON_ERROR("Parameter data_dbnames must be an array of string values");
                    }
                    std::string dn(dbname->valuestring);
                    if (unique_dbnames.find(dn) != unique_dbnames.end()) {
                        JSON_ERROR("Duplicate values in data_dbnames");
                    }
                    unique_dbnames.insert(dn);
                    kvc->addDataDbname(dn);
                }
            } else {
                JSON_ERROR("Unknown parameter in kvstore");
            }
        }
        (*confMap)[s] = kvc;
    }

    cJSON_Delete(c);
    free(data);

    if (!validateConfig(confMap)) {
        delete confMap;
        return NULL;
    }

    return confMap;
}
