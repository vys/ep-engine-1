/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef KVSTORE_MAPPER_HH
#define KVSTORE_MAPPER_HH 1

#include "kvstore.hh"
#include "vbucket.hh"


#define MAX_SHARDS_LIMIT 2520
class EventuallyPersistentEngine;

class KVMapCapacity {
public:
    uint16_t actives;
    uint16_t replicas;
};

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

    // Find an available kvstore to hold a vbucket
    static int findKVStore(vbucket_state_t state, std::map<int, std::vector<uint16_t> > &kvstoresMap,
            KVStore **kvstores, std::map<int, KVMapCapacity> &cap) {

        assert(state == vbucket_state_active || state == vbucket_state_replica);
        assert(instance != NULL);
        int eligibleKVStore(-1);
        size_t kvstoreSize(0), allocSize(0);

        if (instance->mapVBuckets) {
            std::map<int, std::vector<uint16_t> >::iterator it;

            // Find out the kvstore which is holding min vbuckets and min same type of vbucket
            for (it = kvstoresMap.begin(); it != kvstoresMap.end(); it++) {
                KVStore *k = kvstores[(*it).first];
                KVMapCapacity kc = cap[(*it).first];
                if (k->isAvailable()) {
                    size_t allocvbs;
                    if (state == vbucket_state_active) {
                        allocvbs = kc.actives;
                    } else if (state == vbucket_state_replica) {
                        allocvbs = kc.replicas;
                    } else {
                        allocvbs = (*it).second.size() - kc.replicas - kc.actives;
                    }

                    if (eligibleKVStore == -1) {
                        kvstoreSize = (*it).second.size();
                        allocSize = allocvbs;
                        eligibleKVStore = (*it).first;
                    } else if (kvstoreSize > (*it).second.size() ||
                            (kvstoreSize == (*it).second.size() && allocSize > allocvbs)) {
                        kvstoreSize = (*it).second.size();
                        eligibleKVStore = (*it).first;
                    }
                }
            }
        }

        return eligibleKVStore;
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

private:
    KVStoreMapper(int numKV, bool mapVB) :
        numKVStores(numKV), mapVBuckets(mapVB) {}

    static KVStoreMapper *instance;
    int numKVStores;
    bool mapVBuckets;
};

#endif
