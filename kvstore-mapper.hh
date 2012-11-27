/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef KVSTORE_MAPPER_HH
#define KVSTORE_MAPPER_HH 1

#include "vbucket.hh"

#define MAX_SHARDS_LIMIT 2520

typedef enum {
    /**
     * Status codes for KVStoreMapper operations
     */
    KVSTORE_ALLOCATION_SUCCESS,
    KVSTORE_NOT_AVAILABLE
} KVSTOREMAPPER_ERROR_CODE;

/*
 * Class to distribute input data across kvstores.
 */
class KVStoreMapper {
public: 
    static void createKVMapper(int numKV, bool mapVB) {
        if (instance == NULL) {
            instance = new KVStoreMapper(numKV, mapVB);
        }
    }

    static void destroy() {
        delete instance;
        instance = NULL;
    }

    /**
     * Assign a vbucket to an available KVStore which holds less number of vbuckets
     * If kvid != -1, assign vbucket explicitly to the specified kvstore (used for warmup)
     */
    static KVSTOREMAPPER_ERROR_CODE assignKVStore(RCPtr<VBucket> &vb, int kvid = -1) {
        assert(instance != NULL);
        LockHolder lh(instance->mutex);
        KVSTOREMAPPER_ERROR_CODE rv = KVSTORE_ALLOCATION_SUCCESS;
        int eligibleKVStore(-1);
        size_t kvstoreSize;

        if (instance->mapVBuckets) {
            std::map<int, std::vector<uint16_t> >::iterator it;

            if (kvid == -1) {
                for (it = instance->kvstoresMap.begin(); it != instance->kvstoresMap.end(); it++) {
                    if (eligibleKVStore == -1) {
                        kvstoreSize = (*it).second.size();
                        eligibleKVStore = (*it).first;
                    } else if (kvstoreSize > (*it).second.size()) {
                        kvstoreSize = (*it).second.size();
                        eligibleKVStore = (*it).first;
                    }
                }
            } else {
                eligibleKVStore = kvid;
            }

            if (eligibleKVStore != -1) {
                vb->setKVStoreId(eligibleKVStore);
                it = instance->kvstoresMap.find(eligibleKVStore);
                assert(it != instance->kvstoresMap.end());
                (*it).second.push_back(vb->getId());
                getLogger()->log(EXTENSION_LOG_INFO, NULL,
                    "Assigned vbucket %d to kvstore %d\n", vb->getId(), eligibleKVStore);
            } else {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                    "Unable to assign vbucket %d to a kvstore\n", vb->getId());
                rv = KVSTORE_NOT_AVAILABLE;
            }
        }

        return rv;
    }

    static int getVBucketToKVId(const RCPtr<VBucket> &vb) {
        if (instance == NULL) {
            // Should never happen
            return -1;
        }
        return vb->getKVStoreId();
    }

    static int getKVStoreId(const std::string &key, const RCPtr<VBucket> &vb) {
        if (instance == NULL) {
            // Should never happen
            return -1;
        }
        if (instance->mapVBuckets) {
            return KVStoreMapper::getVBucketToKVId(vb);
        }

        int h = 5381;
        int i=0;
        const char *str = key.c_str();

        for(i=0; str[i] != 0x00; i++) {
            h = ((h << 5) + h) ^ str[i];
        }
        return std::abs(h / MAX_SHARDS_LIMIT) % (int)instance->numKVStores;
    }

    static std::vector<uint16_t> getVBucketsForKVStore(int kvid) {
        assert(instance != NULL);
        LockHolder lh(instance->mutex);
        std::map<int, std::vector<uint16_t> >::iterator it;
        it = instance->kvstoresMap.find(kvid);
        assert(it != instance->kvstoresMap.end());
        return (*it).second;
    }

private:
    KVStoreMapper(int numKV, bool mapVB) : numKVStores(numKV), mapVBuckets(mapVB), mutex() {
        LockHolder lh(mutex);
        if (mapVBuckets) {
            std::vector<uint16_t> empty;
            int i;
            for (i=0; i < numKVStores; i++) {
                kvstoresMap[i] = empty;
            }
        }
    }

    static KVStoreMapper *instance;
    int numKVStores;
    bool mapVBuckets;
    //Map kvstore_id to a set of vbucket_id which the kvstore holds
    std::map<int, std::vector<uint16_t> > kvstoresMap;
    Mutex mutex;
};

#endif
