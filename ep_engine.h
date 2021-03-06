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
#ifndef EP_ENGINE_H
#define EP_ENGINE_H 1

#include "locks.hh"
#include "ep.hh"
#include "flusher.hh"
#include "kvstore.hh"
#include "ep_extension.h"
#include "dispatcher.hh"
#include "item_pager.hh"
#include "sync_registry.hh"

#include <cstdio>
#include <map>
#include <list>
#include <sstream>
#include <algorithm>
#include <errno.h>
#include <limits>

#include "command_ids.h"

#include "tapconnmap.hh"
#include "tapconnection.hh"
#include "restore.hh"
#include "configuration.hh"
#include "crc32.hh"

#define DEFAULT_TAP_NOOP_INTERVAL 200
#define DEFAULT_BACKFILL_RESIDENT_THRESHOLD 0.9
#define MINIMUM_BACKFILL_RESIDENT_THRESHOLD 0.7

#ifndef DEFAULT_SYNC_TIMEOUT
#define DEFAULT_SYNC_TIMEOUT 2500
#define MAX_SYNC_TIMEOUT 60000
#define MIN_SYNC_TIMEOUT 10
#endif

#ifndef MAX_METADATA_LEN
#define MAX_METADATA_LEN       1024
#endif



extern "C" {
    EXPORT_FUNCTION
    ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                      GET_SERVER_API get_server_api,
                                      ENGINE_HANDLE **handle);
    void *EvpNotifyTapIo(void*arg);

    EXPORT_FUNCTION
    ENGINE_ERROR_CODE getLocked(EventuallyPersistentEngine *e,
            protocol_binary_request_getl *request,
            const void *cookie,
            Item **item,
            const char **msg,
            size_t *msg_size,
            protocol_binary_response_status *res, 
            bool &free_msg);
}

/* We're using notify_io_complete from ptr_fun, but that func
 * got a "C" linkage that ptr_fun doesn't like... just
 * cast it away with this typedef ;)
 */
typedef void (*NOTIFY_IO_COMPLETE_T)(const void *cookie,
                                     ENGINE_ERROR_CODE status);


// Forward decl
class EventuallyPersistentEngine;
class TapConnMap;
class TapThrottle;

/**
 * Base storage callback for things that look up data.
 */
class LookupCallback : public Callback<GetValue> {
public:
    LookupCallback(EventuallyPersistentEngine *e, const void* c) :
        engine(e), cookie(c) {}

    virtual void callback(GetValue &value);
private:
    EventuallyPersistentEngine *engine;
    const void *cookie;
};

/**
 * Vbucket visitor that counts active vbuckets.
 */
class VBucketCountVisitor : public VBucketVisitor {
public:
    VBucketCountVisitor(vbucket_state_t state) : desired_state(state),
                                                 numItems(0),nonResident(0),
                                                 numVbucket(0), htMemory(0),
                                                 htItemMemory(0), htCacheSize(0),
                                                 numEjects(0), opsCreate(0),
                                                 opsUpdate(0), opsDelete(0),
                                                 opsReject(0), queueSize(0),
                                                 queueMemory(0), queueAge(0),
                                                 queueFill(0), queueDrain(0),
                                                 pendingWrites(0), onlineUpdate(false)
    { }

    bool visitBucket(RCPtr<VBucket> vb);

    void visit(StoredValue* v) {
        (void)v;
        assert(false); // this does not happen
    }

    vbucket_state_t getVBucketState() { return desired_state; }

    size_t getNumItems() { return numItems; }

    size_t getNonResident() { return nonResident; }

    size_t getVBucketNumber() { return numVbucket; }

    size_t getMemResidentPer() {
        size_t numResident = numItems - nonResident;
        return (numItems != 0) ? (size_t) (numResident *100.0) / (numItems) : 0;
    }

    size_t getEjects() { return numEjects; }

    size_t getHashtableMemory() { return htMemory; }

    size_t getItemMemory() { return htItemMemory; }
    size_t getCacheSize() { return htCacheSize; }

    size_t getOpsCreate() { return opsCreate; }
    size_t getOpsUpdate() { return opsUpdate; }
    size_t getOpsDelete() { return opsDelete; }
    size_t getOpsReject() { return opsReject; }

    size_t getQueueSize() { return queueSize; }
    size_t getQueueMemory() { return queueMemory; }
    size_t getQueueFill() { return queueFill; }
    size_t getQueueDrain() { return queueDrain; }
    uint64_t getAge() { return queueAge; }
    size_t getPendingWrites() { return pendingWrites; }
    bool   isOnlineUpdate() { return onlineUpdate; }
private:
    vbucket_state_t desired_state;

    size_t numItems;
    size_t nonResident;
    size_t numVbucket;
    size_t htMemory;
    size_t htItemMemory;
    size_t htCacheSize;
    size_t numEjects;

    size_t opsCreate;
    size_t opsUpdate;
    size_t opsDelete;
    size_t opsReject;

    size_t queueSize;
    size_t queueMemory;
    uint64_t queueAge;
    size_t queueFill;
    size_t queueDrain;
    size_t pendingWrites;

    bool   onlineUpdate;
};

/**
 * memcached engine interface to the EventuallyPersistentStore.
 */
class EventuallyPersistentEngine : public ENGINE_HANDLE_V1 {
    friend class LookupCallback;
public:
    ENGINE_ERROR_CODE initialize(const char* config);
    void destroy(bool force);

    ENGINE_ERROR_CODE itemAllocate(const void* cookie,
                                   item** item,
                                   const void* key,
                                   const size_t nkey,
                                   const size_t nbytes,
                                   const int flags,
                                   const rel_time_t exptime,
                                   const char *cksum,
                                   const size_t nck)
    {
#define METADATA_OVERHEAD (sizeof(StoredValue))

        (void)cookie;
        if (nbytes > maxItemSize) {
            return ENGINE_E2BIG;
        }

        time_t expiretime = (exptime == 0) ? 0 : ep_abs_time(ep_reltime(exptime));

        if (!EvictionManager::getInstance()->evictHeadroom()) {
            return memoryCondition();
        }

        *item = new Item(key, nkey, nbytes, flags, expiretime, cksum, nck);
        if (*item == NULL) {
            return memoryCondition();
        } else {
            return ENGINE_SUCCESS;
        }
    }
   
    ENGINE_ERROR_CODE itemDelete(const void* cookie,
                                 const void* key,
                                 const size_t nkey,
                                 uint64_t cas,
                                 uint16_t vbucket)
    {
        std::string k(static_cast<const char*>(key), nkey);
        return itemDelete(cookie, k, cas, vbucket);
    }

    ENGINE_ERROR_CODE itemDelete(const void* cookie,
                                 const std::string &key,
                                 uint64_t cas,
                                 uint16_t vbucket)
    {
        ENGINE_ERROR_CODE ret = epstore->del(key, cas, vbucket, cookie);
        return ret;
    }


    void itemRelease(const void* cookie, item *item)
    {
        (void)cookie;
        delete (Item*)item;
    }

    ENGINE_ERROR_CODE get(const void* cookie,
                          item** item,
                          const void* key,
                          const int nkey,
                          uint16_t vbucket)
    {
        BlockTimer timer(&stats.getCmdHisto);
        std::string k(static_cast<const char*>(key), nkey);

        GetValue gv(epstore->get(k, vbucket, cookie, serverApi->core, true, true));

        if (gv.getStatus() == ENGINE_SUCCESS) {
            *item = gv.getValue();
        } else if (gv.getStatus() == ENGINE_KEY_ENOENT && restore.enabled.get()) {
            return ENGINE_TMPFAIL;
        }

        return gv.getStatus();
    }

    ENGINE_ERROR_CODE getStats(const void* cookie,
                               const char* stat_key,
                               int nkey,
                               ADD_STAT add_stat);

    void updateFrontEndStats(char *(stat_keys[]), 
                               uint64_t *values, 
                               int count);

    void updateExtensionStats(char *(stat_keys[]), 
                               uint64_t *values, 
                               int count);

    void resetStats() { stats.reset(); }

    ENGINE_ERROR_CODE store(const void *cookie,
                            item* itm,
                            uint64_t *cas,
                            ENGINE_STORE_OPERATION operation,
                            uint16_t vbucket);

    ENGINE_ERROR_CODE arithmetic(const void* cookie,
                                 const void* key,
                                 const int nkey,
                                 const bool increment,
                                 const bool create,
                                 const uint64_t delta,
                                 const uint64_t initial,
                                 const rel_time_t exptime,
                                 uint64_t *cas,
                                 uint64_t *result,
                                 uint16_t vbucket)
    {
        BlockTimer timer(&stats.arithCmdHisto);
        item *it = NULL;

        rel_time_t expiretime = (exptime == 0 ||
                                 exptime == 0xffffffff) ?
            0 : ep_abs_time(ep_reltime(exptime));

        ENGINE_ERROR_CODE ret = get(cookie, &it, key, nkey, vbucket);
        if (ret == ENGINE_SUCCESS) {
            Item *item = static_cast<Item*>(it);
            char *endptr = NULL;
            char data[24];
            size_t len = std::min(static_cast<uint32_t>(sizeof(data) - 1),
                                  item->getNBytes());
            data[len] = 0;
            memcpy(data, item->getData(), len);
            uint64_t val = strtoull(data, &endptr, 10);
            if (item->getCas() == (uint64_t) -1) {
                // item is locked, can't perform arithmetic operation
                delete item;
                return ENGINE_TMPFAIL;
            }
            if ((errno != ERANGE) && (isspace(*endptr)
                                      || (*endptr == '\0' && endptr != data))) {
                if (increment) {
                    val += delta;
                } else {
                    if (delta > val) {
                        val = 0;
                    } else {
                        val -= delta;
                    }
                }

                std::stringstream vals;
                vals << val;
                size_t nb = vals.str().length();
                *result = val;
                Item *nit = new Item(key, (uint16_t)nkey, item->getFlags(),
                                     item->getExptime(), vals.str().c_str(), nb,
                                     DISABLED_CRC_STR, DISABLED_CRC_STR_SIZE);
                nit->setCas(item->getCas());
                ret = store(cookie, nit, cas, OPERATION_CAS, vbucket);
                delete nit;
            } else {
                ret = ENGINE_EINVAL;
            }

            delete item;
        } else if (ret == ENGINE_NOT_MY_VBUCKET) {
            return ret;
        } else if (ret == ENGINE_KEY_ENOENT && create) {
            std::stringstream vals;

            vals << initial;
            size_t nb = vals.str().length();

            *result = initial;
            Item *item = new Item(key, (uint16_t)nkey, 0, expiretime,
                                  vals.str().c_str(), nb,
                                  DISABLED_CRC_STR, DISABLED_CRC_STR_SIZE);
            ret = store(cookie, item, cas, OPERATION_ADD, vbucket);
            delete item;
        }

        /* We had a race condition.. just call ourself recursively to retry */
        if (ret == ENGINE_KEY_EEXISTS) {
            return arithmetic(cookie, key, nkey, increment, create, delta,
                              initial, expiretime, cas, result, vbucket);
        }

        return ret;
    }



    ENGINE_ERROR_CODE flush(const void *cookie, time_t when);

    tap_event_t walkTapQueue(const void *cookie, item **itm, void **es,
                             uint16_t *nes, uint8_t *ttl, uint16_t *flags,
                             uint32_t *seqno, uint16_t *vbucket);

    bool createTapQueue(const void *cookie,
                        std::string &client,
                        uint32_t flags,
                        const void *userdata,
                        size_t nuserdata);

    ENGINE_ERROR_CODE tapNotify(const void *cookie,
                                void *engine_specific,
                                uint16_t nengine,
                                uint8_t ttl,
                                uint16_t tap_flags,
                                tap_event_t tap_event,
                                uint32_t tap_seqno,
                                const void *key,
                                size_t nkey,
                                uint32_t flags,
                                uint32_t queued,
                                uint32_t exptime,
                                uint64_t cas,
                                const void *data,
                                size_t ndata,
                                uint16_t vbucket,
                                const char *cksum);

    ENGINE_ERROR_CODE touch(const void* cookie,
                            protocol_binary_request_header *request,
                            ADD_RESPONSE response);

    ENGINE_ERROR_CODE onlineUpdate(const void* cookie,
                            protocol_binary_request_header *request,
                            ADD_RESPONSE response);

    /**
     * Visit the objects and add them to the tap connecitons queue.
     * @todo this code should honor the backfill time!
     */
    void queueBackfill(const VBucketFilter &backfillVBFilter, TapProducer *tc, const void *tok);

    void reportNullCookie(TapConnection &tc);

    void notifyIOComplete(const void *cookie, ENGINE_ERROR_CODE status) {
        if (cookie == NULL) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Tried to signal a NULL cookie!");
        } else {
            BlockTimer bt(&stats.notifyIOHisto);
            serverApi->cookie->notify_io_complete(cookie, status);
        }
    }

    template <typename T>
    void notifyIOComplete(T cookies, ENGINE_ERROR_CODE status) {
        std::for_each(cookies.begin(), cookies.end(),
                      std::bind2nd(std::ptr_fun((NOTIFY_IO_COMPLETE_T)serverApi->cookie->notify_io_complete),
                                   status));
    }

    void handleDisconnect(const void *cookie) {
        tapConnMap.disconnect(cookie, static_cast<int>(tapKeepAlive));
    }

    protocol_binary_response_status stopFlusher(const char **msg, size_t *msg_size) {
        (void) msg_size;
        protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;
        *msg = NULL;
        if (!epstore->pauseFlusher()) {
            if (numKVStores == 1) {
                getLogger()->log(EXTENSION_LOG_INFO, NULL,
                        "Attempted to stop flusher in state [%s]\n",
                        epstore->getFlusher()->stateName());
            } else {
                getLogger()->log(EXTENSION_LOG_INFO, NULL,
                        "Attempted to stop flushers in states:\n");
                for (int i = 0; i < numKVStores; i++) {
                    getLogger()->log(EXTENSION_LOG_INFO, NULL,
                            "Flusher %i: [%s]\n",
                            i, epstore->getFlusher(i)->stateName());
                }
            }
            *msg = "Flusher not running.";
            rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
        return rv;
    }

    protocol_binary_response_status startFlusher(const char **msg, size_t *msg_size) {
        (void) msg_size;
        protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;
        *msg = NULL;
        if (!epstore->resumeFlusher()) {
            if (numKVStores == 1) {
                getLogger()->log(EXTENSION_LOG_INFO, NULL,
                        "Attempted to start flusher in state [%s]\n",
                        epstore->getFlusher()->stateName());
            } else {
                getLogger()->log(EXTENSION_LOG_INFO, NULL,
                        "Attempted to start flushers in states:\n");
                for (int i = 0; i < numKVStores; i++) {
                    getLogger()->log(EXTENSION_LOG_INFO, NULL,
                            "Flusher %i: [%s]\n",
                            i, epstore->getFlusher(i)->stateName());
                }
            }
            *msg = "Flusher not shut down.";
            rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
        return rv;
    }

    bool deleteVBucket(uint16_t vbid) {
        LockHolder lh(restore.mutex);
        bool rv;
        bool is_restore_mode = epstore->isRestoreEnabled(vbid);

        rv = epstore->deleteVBucket(vbid);
        if (is_restore_mode) {
            restore.enabled.set(false);
        }

        return rv;
    }

    bool resetVBucket(uint16_t vbid) {
        return epstore->resetVBucket(vbid);
    }

    void setMinDataAge(int to) {
        epstore->setMinDataAge(to);
    }

    void setQueueAgeCap(int to) {
        epstore->setQueueAgeCap(to);
    }

    void setTxnSize(int to) {
        if (to > 0) {
            epstore->setTxnSize(to);
        }
    }

    void setBGFetchDelay(uint32_t to) {
        epstore->setBGFetchDelay(to);
    }

    void setTapKeepAlive(uint32_t to) {
        tapKeepAlive = to;
    }

    protocol_binary_response_status evictKey(const std::string &key,
                                             uint16_t vbucket,
                                             const char **msg,
                                             size_t *msg_size) {
        return epstore->evictKey(key, vbucket, msg, msg_size);
    }

    bool getLocked(const std::string &key,
                   uint16_t vbucket,
                   Callback<GetValue> &cb,
                   rel_time_t currentTime,
                   uint32_t lockTimeout,
                   std::string &metadata,
                   const void *cookie) {
        return epstore->getLocked(key, vbucket, cb, currentTime, lockTimeout, metadata, cookie);
    }

    ENGINE_ERROR_CODE sync(std::set<key_spec_t> *keys,
                           const void *cookie,
                           sync_type_t syncType,
                           uint8_t replicas,
                           ADD_RESPONSE response);

    ENGINE_ERROR_CODE unlockKey(const std::string &key,
                                uint16_t vbucket,
                                uint64_t cas,
                                rel_time_t currentTime) {
        return epstore->unlockKey(key, vbucket, cas, currentTime);
    }

    RCPtr<VBucket> getVBucket(uint16_t vbucket) {
        return epstore->getVBucket(vbucket);
    }

    ENGINE_ERROR_CODE setVBucketState(uint16_t vbid, vbucket_state_t to) {
        return epstore->setVBucketState(vbid, to);
    }

    ~EventuallyPersistentEngine() {
        delete epstore;
        if (kvstore) {
            for (int i = 0; i < numKVStores; ++i) {
                delete kvstore[i];
            }
            delete []kvstore;
        }
        if (kvstoreConfigMap) {
            for (std::map<std::string, KVStoreConfig*>::iterator it = kvstoreConfigMap->begin();
                    it != kvstoreConfigMap->end(); it++) {
                delete it->second;
            }
            delete kvstoreConfigMap;
        }
        delete getlExtension;
        delete diExtension;
    }

    engine_info *getInfo() {
        return &info.info;
    }

    EPStats &getEpStats() {
        return stats;
    }

    ExtStats &getFrontEndStats() {
        return festats;
    }

    EventuallyPersistentStore* getEpStore() { return epstore; }

    TapConnMap &getTapConnMap() { return tapConnMap; }

    size_t getItemExpiryWindow() const {
        return itemExpiryWindow;
    }

    size_t getVbDelChunkSize() const {
        return vb_del_chunk_size;
    }

    size_t getVbChunkDelThresholdTime() const {
        return vb_chunk_del_threshold_time;
    }

    bool isForceShutdown(void) const {
        return forceShutdown;
    }

    SERVER_HANDLE_V1* getServerApi() { return serverApi; }

    SyncRegistry &getSyncRegistry() {
        return syncRegistry;
    }

    Configuration &getConfiguration() {
        return configuration;
    }

    void notifyTapNotificationThread(void);
    void setTapValidity(const std::string &name, const void* token);

    ENGINE_ERROR_CODE setRestoreMode(uint16_t vbid, bool enable);

    ENGINE_ERROR_CODE handleRestoreCmd(const void* cookie,
                                       protocol_binary_request_header *request,
                                       ADD_RESPONSE response);

    ENGINE_ERROR_CODE deregisterTapClient(const void* cookie,
                                          protocol_binary_request_header *request,
                                          ADD_RESPONSE response);

    ENGINE_ERROR_CODE handleGetLastClosedCheckpointId(const void* cookie,
                                                      protocol_binary_request_header *request,
                                                      ADD_RESPONSE response);

    ENGINE_ERROR_CODE resetReplicationChain(const void* cookie,
                                            protocol_binary_request_header *request,
                                            ADD_RESPONSE response);

    size_t getGetlDefaultTimeout() { return getlDefaultTimeout; }
    size_t getGetlMaxTimeout() { return getlMaxTimeout; }

    /**
     * Set the timeout for the SYNC command. Timeout is in milliseconds.
     */
    void setSyncCmdTimeout(size_t timeout) {
        syncTimeout = timeout;
    }

    size_t getSyncCmdTimeout() const {
        return syncTimeout;
    }

    void setMaxEvictEntries(int val) {
        EvictionManager::getInstance()->setMaxSize(val);
    }

    void setPruneAge(time_t val) {
        EvictionManager::getInstance()->setPruneAge(val);
    }

    size_t getExpiryPagerSleeptime(bool getLruTimer = false) {
        LockHolder lh(expiryPager.mutex);
        return getLruTimer ? expiryPager.lrusleeptime : expiryPager.sleeptime;
    }

    void setAndScheduleExpiryPagerSleeptime(size_t val, bool setLruTimer = false) {
        LockHolder lh(expiryPager.mutex);

        if (setLruTimer) {
            expiryPager.lrusleeptime = val;
        } else {
            expiryPager.sleeptime = val;
        }

        if (expiryPager.running && expiryPager.sleeptime == 0) {
            epstore->getNonIODispatcher()->cancel(expiryPager.task);
            expiryPager.running = false;
        }

        if (expiryPager.sleeptime != 0) { // run only if exp_pager_stime is positive
            if (expiryPager.running) {
                ((ExpiredItemPager*) expiryPager.pager.get())->updateSleepTimes(expiryPager.sleeptime, expiryPager.lrusleeptime);
            } else {
                expiryPager.pager = shared_ptr<DispatcherCallback>(new ExpiredItemPager(epstore, stats, expiryPager.sleeptime, expiryPager.lrusleeptime));
                epstore->getNonIODispatcher()->schedule(expiryPager.pager, &expiryPager.task,
                        Priority::ItemPagerPriority,
                        ((ExpiredItemPager*) expiryPager.pager.get())->callbackFreq());
                expiryPager.running = true;
            }
        }
    }

    void evictionJobEnabled(bool val) {
        EvictionManager::getInstance()->enableJob(val);
    }

    bool setEvictionPolicy(const char *name) {
        return EvictionManager::getInstance()->setPolicy(name);
    }

    std::set<std::string>& getEvictionPolicyNames() {
        return EvictionManager::getInstance()->getPolicyNames();
    }

private:
    EventuallyPersistentEngine(GET_SERVER_API get_server_api);
    friend ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                             GET_SERVER_API get_server_api,
                                             ENGINE_HANDLE **handle);
    tap_event_t doWalkTapQueue(const void *cookie, item **itm, void **es,
                               uint16_t *nes, uint8_t *ttl, uint16_t *flags,
                               uint32_t *seqno, uint16_t *vbucket,
                               TapProducer *c, bool &retry);


    ENGINE_ERROR_CODE processTapAck(const void *cookie,
                                    uint32_t seqno,
                                    uint16_t status,
                                    const std::string &msg);

    /**
     * Report the state of a memory condition when out of memory.
     *
     * @return ETMPFAIL if we think we can recover without interaction,
     *         else ENOMEM
     */
    ENGINE_ERROR_CODE memoryCondition() {
        // Do we think it's possible we could free something?
        bool haveEvidenceWeCanFreeMemory(stats.maxDataSize > stats.memOverhead);
        if (haveEvidenceWeCanFreeMemory) {
            ++stats.tmp_oom_errors;
            return ENGINE_TMPFAIL;
        } else {
            ++stats.oom_errors;
            return ENGINE_ENOMEM;
        }
    }

    friend void *EvpNotifyTapIo(void*arg);
    void notifyTapIoThread(void);

    friend class BackFillVisitor;
    friend class TapBGFetchCallback;
    friend class TapConnMap;
    friend class EventuallyPersistentStore;

    void addMutationEvent(Item *it) {
        if (mutation_count == 0) {
            tapConnMap.notify();
        }
        ++mutation_count;
        syncRegistry.itemModified(*it);
    }

    void addDeleteEvent(const std::string &key, uint16_t vbid, uint64_t cas) {
        if (mutation_count == 0) {
            tapConnMap.notify();
        }
        ++mutation_count;
        syncRegistry.itemDeleted(key_spec_t(cas, vbid, key));
    }

    void startEngineThreads(void);
    void stopEngineThreads(void) {
        if (startedEngineThreads) {
            shutdown = true;
            tapConnMap.notify();
            pthread_join(notifyThreadId, NULL);
        }
    }


    bool dbAccess() {
        bool ret = true;
        for (std::map<std::string, KVStoreConfig*>::iterator it = kvstoreConfigMap->begin();
                it != kvstoreConfigMap->end(); it++) {
            std::string dbname = it->second->getDbname();
            if (access(dbname.c_str(), F_OK) == -1) {
                // file does not exist.. let's try to create it..
                FILE *fp = fopen(dbname.c_str(), "w");
                if (fp == NULL) {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                            "No access to \"%s\".\n", dbname.c_str());
                    ret= false;
                } else {
                    fclose(fp);
                    std::remove(dbname.c_str());
                }
            } else if (access(dbname.c_str(), R_OK) == -1 || access(dbname.c_str(), W_OK) == -1) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                    "No access to \"%s\".\n", dbname.c_str());
                ret = false;
            }
        }

        return ret;
    }

    ENGINE_ERROR_CODE doEngineStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doMemoryStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doKVStoreStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doVBucketStats(const void *cookie, ADD_STAT add_stat,
                                     bool prevStateRequested = false);
    ENGINE_ERROR_CODE doHashStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doCheckpointStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doTapStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doTapAggStats(const void *cookie, ADD_STAT add_stat,
                                    const char *sep, size_t nsep);
    ENGINE_ERROR_CODE doTimingStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doDispatcherStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doKeyStats(const void *cookie, ADD_STAT add_stat,
                                 uint16_t vbid, std::string &key, bool validate=false);
    ENGINE_ERROR_CODE doEvictionStats(const void *cookie, ADD_STAT add_stat);

    void addLookupResult(const void *cookie, Item *result) {
        LockHolder lh(lookupMutex);
        std::map<const void*, Item*>::iterator it = lookups.find(cookie);
        if (it != lookups.end()) {
            if (it->second != NULL) {
                getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                                 "Cleaning up old lookup result for '%s'\n",
                                 it->second->getKey().c_str());
                delete it->second;
            } else {
                getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                                 "Cleaning up old null lookup result\n");
            }
            lookups.erase(it);
        }
        lookups[cookie] = result;
    }

    bool fetchLookupResult(const void *cookie, Item **item) {
        // This will return *and erase* the lookup result for a connection.
        // You look it up, you own it.
        LockHolder lh(lookupMutex);
        std::map<const void*, Item*>::iterator it = lookups.find(cookie);
        if (it != lookups.end()) {
            *item = it->second;
            lookups.erase(it);
            return true;
        } else {
            return false;
        }
    }

    bool createKVStores();
    KVStore *newKVStore(KVStoreConfig *c);

    // Get the current tap connection for this cookie.
    // If this method returns NULL, you should return TAP_DISCONNECT
    TapProducer* getTapProducer(const void *cookie);

    const char *evictionPolicy;
    bool forceShutdown;
    SERVER_HANDLE_V1 *serverApi;
    KVStore **kvstore;
    EventuallyPersistentStore *epstore;
    TapThrottle *tapThrottle;
    std::map<const void*, Item*> lookups;
    Mutex lookupMutex;
    time_t databaseInitTime;
    size_t tapKeepAlive;
    size_t tapNoopInterval;
    size_t nextTapNoop;
    pthread_t notifyThreadId;
    bool startedEngineThreads;
    AtomicList<QueuedItem> pendingTapNotifications;
    volatile bool shutdown;
    GET_SERVER_API getServerApiFunc;
    union {
        engine_info info;
        char buffer[sizeof(engine_info) + 10 * sizeof(feature_info) ];
    } info;
    GetlExtension *getlExtension;
    DiExtension *diExtension;

    TapConnMap tapConnMap;
    Mutex tapMutex;
    size_t maxItemSize;
    size_t memLowWat;
    size_t memHighWat;
    size_t itemExpiryWindow;
    struct ExpiryPagerDelta {
        ExpiryPagerDelta() : sleeptime(0), lrusleeptime(0), running(false) {}
        Mutex mutex;
        size_t sleeptime;
        size_t lrusleeptime;
        TaskId task;
        shared_ptr<DispatcherCallback> pager;
        bool running;
    } expiryPager;

    size_t vb_del_chunk_size;
    size_t vb_chunk_del_threshold_time;
    Atomic<uint64_t> mutation_count;
    size_t getlDefaultTimeout;
    size_t getlMaxTimeout;
    size_t syncTimeout;
    EPStats stats;
    ExtStats festats;
    SyncRegistry syncRegistry;
    int numKVStores;
    Configuration configuration;
    std::map<std::string, KVStoreConfig*> *kvstoreConfigMap;
    struct {
        Mutex mutex;
        RestoreManager *manager;
        Atomic<bool> enabled;
    } restore;
};

#endif
