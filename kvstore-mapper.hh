/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef KVSTORE_MAPPER_HH
#define KVSTORE_MAPPER_HH 1

#include "queueditem.hh"

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

    static int getVBucketToKVId(uint16_t vbid) {
        if (instance == NULL) {
            // Should never happen
            return -1;
        }
        //FIXME:: Fix with a better algorithm to map vbuckets
        return vbid % instance->numKVStores;
    }

    static int getKVStoreId(const std::string &key, uint16_t vbid) {
        if (instance == NULL) {
            // Should never happen
            return -1;
        }
        if (instance->mapVBuckets) {
            return KVStoreMapper::getVBucketToKVId(vbid);
        }

        int h = 0;
        int i=0;
        const char *str = key.c_str();

        for(i=0; str[i] != 0x00; i++) {
            h = ( h << 4 ) ^ ( h >> 28 ) ^ str[i];
        }
        return std::abs(h) % (int)instance->numKVStores;
    }

private:
    KVStoreMapper(int numKV, bool mapVB) : numKVStores(numKV), mapVBuckets(mapVB) {}

    static KVStoreMapper *instance;
    int numKVStores;
    bool mapVBuckets;
};

/*
 * Filter to map queued_item to kvstore id
 */
class QueuedItemFilter {
public:
    QueuedItemFilter(int _kvid) : kvid(_kvid) {}

    bool operator ()(size_t x) {
        (void) x;
        return true;
    }
    bool operator () (queued_item &itm) {
        return KVStoreMapper::getKVStoreId(itm->getKey(), itm->getVBucketId()) == kvid;
    }

private:
    int kvid;
};
#endif
