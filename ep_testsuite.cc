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

#include "config.h"

#include <iostream>
#include <sstream>
#include <map>
#include <string>
#include <vector>
#include <cstdlib>

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <pthread.h>
#include <netinet/in.h>
#include <sys/stat.h>
#include <fstream>

#ifdef HAS_ARPA_INET_H
#include <arpa/inet.h>
#endif

#include <memcached/engine.h>
#include <memcached/engine_testapp.h>

#ifdef USE_SYSTEM_LIBSQLITE3
#include <sqlite3.h>
#else
#include "embedded/sqlite3.h"
#endif

#include "atomic.hh"
#include "sqlite-pst.hh"
#include "mutex.hh"
#include "locks.hh"

#include "tools/cJSON.h"
#include "ep_testsuite.h"
#include "command_ids.h"
#include "sync_registry.hh"
#include "crc32.hh"
#include "tapconnection.hh"

#ifdef linux
/* /usr/include/netinet/in.h defines macros from ntohs() to _bswap_nn to
 * optimize the conversion functions, but the prototypes generate warnings
 * from gcc. The conversion methods isn't the bottleneck for my app, so
 * just remove the warnings by undef'ing the optimization ..
 */
#undef ntohs
#undef ntohl
#undef htons
#undef htonl
#endif

// ptr_fun don't like the extern "C" thing for unlock cookie.. cast it
// away ;)
typedef void (*UNLOCK_COOKIE_T)(const void *cookie);
#define DEFAULT_CRC_STR "0002:" 

extern "C" {

bool abort_msg(const char *expr, const char *msg, int line);

#define check(expr, msg) \
    static_cast<void>((expr) ? 0 : abort_msg(#expr, msg, __LINE__))

#define WHITESPACE_DB "whitespace sucks.db"
#define MULTI_DISPATCHER_CONFIG \
    "kvstore_config_file=t/kv_wal.json;ht_size=129;ht_locks=3;chk_remover_stime=1;chk_period=60"

protocol_binary_response_status last_status(static_cast<protocol_binary_response_status>(0));
char *last_key = NULL;
char *last_body = NULL;
// set dump_stats to true if you like to dump the stats as we go along...
static bool dump_stats = false;
std::map<std::string, std::string> vals;
uint64_t last_cas = 0;

struct test_harness testHarness;

class ThreadData {
public:
    ThreadData(ENGINE_HANDLE *eh, ENGINE_HANDLE_V1 *ehv1,
               int e=0) : h(eh), h1(ehv1), extra(e) {}
    ENGINE_HANDLE    *h;
    ENGINE_HANDLE_V1 *h1;
    int               extra;
};

bool abort_msg(const char *expr, const char *msg, int line) {
    fprintf(stderr, "%s:%d Test failed: `%s' (%s)\n",
            __FILE__, line, msg, expr);
    abort();
    // UNREACHABLE
    return false;
}

class BlockTimerSimple {
public:
    BlockTimerSimple() {
        reset();
    }

    hrtime_t getElapsedTime() {
        return (gethrtime() - startTime) / 1000000000;
    }

    hrtime_t getElapsedTimeUsec() {
        return gethrtime() - startTime;
    }

    double getElapsedTimeDouble() {
        return ((double) (gethrtime() - startTime)) / 1000000000.0;
    }

    void reset() {
        startTime = gethrtime();
    }

    hrtime_t startTime;
};

#define JSON_ERROR(msg) \
    if (c != NULL) { \
        cJSON_Delete(c); \
    } \
    if (data) { \
        free(data); \
    } \
    delete conf; \
    abort_msg(confFile, msg, __LINE__)

static std::map<std::string, std::string> *parsePerfTestConfig(const char *confFile = NULL) {
    std::map<std::string, std::string> *conf = new std::map<std::string, std::string>;
    cJSON *c = NULL;

    if (confFile == NULL) {
        confFile = "t/perf_test_conf.json";
    }

    char *data = NULL;

    struct stat st;
    if (stat(confFile, &st) == -1) {
        JSON_ERROR("Cannot read KVStore config file");
    } else {
        data = (char*) malloc((st.st_size + 1) * sizeof(char));
        data[st.st_size] = 0;
        std::ifstream input(confFile);
        input.read(data, st.st_size);
        input.close();
    }

    if ((c = cJSON_Parse(data)) == NULL) {
        JSON_ERROR("Parse error in JSON file");
    }

    int numKeys = cJSON_GetArraySize(c);
    std::set<std::string> unique_keys;
    for (int i = 0; i < numKeys; i++) {
        cJSON *param = cJSON_GetArrayItem(c, i);
        std::string t(param->string);
        if (unique_keys.find(t) != unique_keys.end()) {
            JSON_ERROR("Duplicate keys in JSON");
        }
        unique_keys.insert(t);

        if (param->type != cJSON_String) {
            JSON_ERROR("Parameter value must be string");
        }
        (*conf)[t] = param->valuestring;
    }

    cJSON_Delete(c);
    free(data);

    return conf;
}

static void rmdb(void) {
    unlink("/tmp/test.db");
    unlink("/tmp/test.db-0.sqlite");
    unlink("/tmp/test.db-1.sqlite");
    unlink("/tmp/test.db-2.sqlite");
    unlink("/tmp/test.db-3.sqlite");
    unlink("/tmp/test.db-wal");
    unlink("/tmp/test.db-0.sqlite-wal");
    unlink("/tmp/test.db-1.sqlite-wal");
    unlink("/tmp/test.db-2.sqlite-wal");
    unlink("/tmp/test.db-3.sqlite-wal");
    unlink("/tmp/test.db-shm");
    unlink("/tmp/test.db-0.sqlite-shm");
    unlink("/tmp/test.db-1.sqlite-shm");
    unlink("/tmp/test.db-2.sqlite-shm");
    unlink("/tmp/test.db-3.sqlite-shm");

    unlink("/tmp/db1.db");
    unlink("/tmp/db1.db-0.sqlite");
    unlink("/tmp/db1.db-1.sqlite");
    unlink("/tmp/db1.db-2.sqlite");
    unlink("/tmp/db1.db-3.sqlite");

    unlink("/tmp/db2.db");
    unlink("/tmp/db2-db1.db-0.sqlite");
    unlink("/tmp/db2-db1.db-3.sqlite");
    unlink("/tmp/db2-db1.db-6.sqlite");
    unlink("/tmp/db2-db2.db-1.sqlite");
    unlink("/tmp/db2-db2.db-4.sqlite");
    unlink("/tmp/db2-db2.db-7.sqlite");
    unlink("/tmp/db2-db3.db-2.sqlite");
    unlink("/tmp/db2-db3.db-5.sqlite");

    unlink("/tmp/db3.db");
    unlink("/tmp/db3.db-0.sqlite");
    unlink("/tmp/db3.db-1.sqlite");
    unlink("/tmp/db3.db-2.sqlite");
    unlink("/tmp/db3.db-3.sqlite");

    unlink("/data_1/test-db.db-3.sqlite");
    unlink("/data_1/test-db.db-2.sqlite");
    unlink("/data_1/test-db.db-0.sqlite");
    unlink("/data_1/test-db.db-1.sqlite");
    unlink("/data_1/test-db.db");
    unlink("/data_2/test-db.db-0.sqlite");
    unlink("/data_2/test-db.db-1.sqlite");
    unlink("/data_2/test-db.db-3.sqlite");
    unlink("/data_2/test-db.db-2.sqlite");
    unlink("/data_2/test-db.db");
    unlink("/data_3/test-db.db-3.sqlite");
    unlink("/data_3/test-db.db");
    unlink("/data_3/test-db.db-2.sqlite");
    unlink("/data_3/test-db.db-1.sqlite");
    unlink("/data_3/test-db.db-0.sqlite");
    unlink("/data_4/test-db.db");
    unlink("/data_4/test-db.db-0.sqlite");
    unlink("/data_4/test-db.db-1.sqlite");
    unlink("/data_4/test-db.db-2.sqlite");
    unlink("/data_4/test-db.db-3.sqlite");
    unlink("/data_5/test-db.db-3.sqlite");
    unlink("/data_5/test-db.db-2.sqlite");
    unlink("/data_5/test-db.db-0.sqlite");
    unlink("/data_5/test-db.db-1.sqlite");
    unlink("/data_5/test-db.db");
    unlink("/data_6/test-db.db-3.sqlite");
    unlink("/data_6/test-db.db-2.sqlite");
    unlink("/data_6/test-db.db-0.sqlite");
    unlink("/data_6/test-db.db-1.sqlite");
    unlink("/data_6/test-db.db");
}

static bool teardown(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    (void)h; (void)h1;
    atexit(rmdb);
    vals.clear();
    return true;
}

static inline void decayingSleep(useconds_t *sleepTime) {
    static const useconds_t maxSleepTime = 500000;
    usleep(*sleepTime);
    *sleepTime = std::min(*sleepTime << 1, maxSleepTime);
}

static ENGINE_ERROR_CODE storeCasVb11(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                      const void *cookie,
                                      ENGINE_STORE_OPERATION op,
                                      const char *key,
                                      const char *value, size_t vlen,
                                      uint32_t flags,
                                      item **outitem, uint64_t casIn,
                                      uint16_t vb, uint64_t exp = 3600) {
    item *it = NULL;
    uint64_t cas = 0;

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    rv = h1->allocate(h, cookie, &it,
                      key, strlen(key),
                      vlen, flags, exp, 0, 0);
    if (rv != ENGINE_SUCCESS) {
        if (outitem) {
            *outitem = NULL;
        }
        return rv;
    }

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, cookie, it, &info)) {
        abort();
    }

    memcpy(info.value[0].iov_base, value, vlen);
    h1->item_set_cas(h, cookie, it, casIn);

    rv = h1->store(h, cookie, it, &cas, op, vb);

    if (outitem) {
        *outitem = it;
    } else {
        h1->release(h, NULL, it);
    }

    return rv;
}

static ENGINE_ERROR_CODE store(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                               const void *cookie,
                               ENGINE_STORE_OPERATION op,
                               const char *key, const char *value,
                               item **outitem, uint64_t casIn = 0,
                               uint16_t vb = 0, uint64_t exp = 3600) {
    return storeCasVb11(h, h1, cookie, op, key, value, strlen(value),
                        9258, outitem, casIn, vb, exp);
}


static ENGINE_ERROR_CODE verify_vb_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                       const char* key, uint16_t vbucket) {
    item *i = NULL;
    ENGINE_ERROR_CODE rv = h1->get(h, NULL, &i, key, strlen(key), vbucket);
    h1->release(h, NULL, i);
    return rv;
}

static ENGINE_ERROR_CODE verify_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                    const char* key) {
    return verify_vb_key(h, h1, key, 0);
}

static bool get_value(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                      const char* key, item_info *info) {
    item *i = NULL;
    if (h1->get(h, NULL, &i, key, strlen(key), 0) != ENGINE_SUCCESS) {
        return false;
    }
    info->nvalue = 1;
    if (!h1->get_item_info(h, NULL, i, info)) {
        h1->release(h, NULL, i);
        fprintf(stderr, "get_item_info failed\n");
        return false;
    }
    h1->release(h, NULL, i);
    return true;
}

static bool get_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, item *i,
                    std::string &key) {

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, i, &info)) {
        fprintf(stderr, "get_item_info failed\n");
        return false;
    }

    key.assign((const char*)info.key, info.nkey);
    return true;
}



static enum test_result check_key_value(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                        const char* key,
                                        const char* val, size_t vlen,
                                        uint16_t vbucket = 0) {
    item *i = NULL;
    ENGINE_ERROR_CODE rv;
    if ((rv = h1->get(h, NULL, &i, key, strlen(key), vbucket)) != ENGINE_SUCCESS) {
        fprintf(stderr, "Expected ENGINE_SUCCESS on get of %s (vb=%d), got %d\n",
                key, vbucket, rv);
        abort();
    }

    item_info info;
    info.nvalue = 1;
    check(h1->get_item_info(h, NULL, i, &info), "check_key_value");

    assert(info.nvalue == 1);
    if (vlen != info.value[0].iov_len) {
        std::cerr << "Expected length " << vlen
                  << " got " << info.value[0].iov_len << std::endl;
        check(vlen == info.value[0].iov_len, "Length mismatch.");
    }

    check(memcmp(info.value[0].iov_base, val, vlen) == 0,
          "Data mismatch");
    h1->release(h, NULL, i);

    return SUCCESS;
}

static void add_stats(const char *key, const uint16_t klen,
                      const char *val, const uint32_t vlen,
                      const void *cookie) {
    (void)cookie;
    std::string k(key, klen);
    std::string v(val, vlen);

    if (dump_stats) {
        std::cout << "stat[" << k << "] = " << v << std::endl;
    }

    vals[k] = v;
}

static bool add_response(const void *key, uint16_t keylen,
                         const void *ext, uint8_t extlen,
                         const void *body, uint32_t bodylen,
                         uint8_t datatype, uint16_t status,
                         uint64_t cas, const void *cookie) {
    (void)ext;
    (void)extlen;
    (void)datatype;
    (void)cookie;
    last_status = static_cast<protocol_binary_response_status>(status);
    if (last_body) {
        free(last_body);
        last_body = NULL;
    }
    if (bodylen > 0) {
        last_body = static_cast<char*>(malloc(bodylen + 1));
        assert(last_body);
        memcpy(last_body, body, bodylen);
        last_body[bodylen] = '\0';
    }
    if (last_key) {
        free(last_key);
        last_key = NULL;
    }
    if (keylen > 0) {
        last_key = static_cast<char*>(malloc(keylen + 1));
        assert(last_key);
        memcpy(last_key, key, keylen);
        last_key[keylen] = '\0';
    }
    last_cas = cas;
    return true;
}

static protocol_binary_request_header* create_packet(uint8_t opcode,
                                                     const char *key,
                                                     const char *val) {
    char *pkt_raw = static_cast<char*>(calloc(1,
                                              sizeof(protocol_binary_request_header)
                                              + strlen(key)
                                              + strlen(val)));
    assert(pkt_raw);
    protocol_binary_request_header *req =
        (protocol_binary_request_header*)pkt_raw;
    req->request.opcode = opcode;
    req->request.bodylen = htonl(strlen(key) + strlen(val));
    req->request.keylen = htons(strlen(key));
    memcpy(pkt_raw + sizeof(protocol_binary_request_header),
           key, strlen(key));
    memcpy(pkt_raw + sizeof(protocol_binary_request_header) + strlen(key),
           val, strlen(val));
    return req;
}

static protocol_binary_request_header* createPacket(uint8_t opcode,
                                                    uint16_t vbid,
                                                    const char *key = NULL,
                                                    const char *val = NULL) {
    char *pkt_raw;
    uint32_t keylen = key != NULL ? strlen(key) : 0;
    uint32_t vallen = val != NULL ? strlen(val) : 0;
    pkt_raw = static_cast<char*>(calloc(1,
                                        sizeof(protocol_binary_request_header)
                                        + keylen
                                        + vallen));
    assert(pkt_raw);
    protocol_binary_request_header *req =
        (protocol_binary_request_header*)pkt_raw;
    req->request.opcode = opcode;
    req->request.vbucket = ntohs(vbid);
    req->request.bodylen = htonl(keylen + vallen);
    req->request.keylen = htons(keylen);
    if (keylen > 0) {
        memcpy(pkt_raw + sizeof(protocol_binary_request_header),
               key, keylen);
    }

    if (vallen > 0) {
        memcpy(pkt_raw + sizeof(protocol_binary_request_header) + keylen,
               val, vallen);
    }

    return req;
}

static void evict_key(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                      const char *key, uint16_t vbucketId=0,
                      const char *msg = NULL) {
    protocol_binary_request_header *pkt = create_packet(CMD_EVICT_KEY,
                                                        key, "");
    pkt->request.vbucket = htons(vbucketId);

    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to evict key.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected success evicting key.");

    if (msg != NULL && strcmp(last_body, msg) != 0) {
        fprintf(stderr, "Expected evict to return ``%s'', but it returned ``%s''\n",
                msg, last_body);
        abort();
    }
}

static enum test_result test_getl(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char *key = "k1";
    char *pkt_raw = static_cast<char*>(calloc(1,sizeof(protocol_binary_request_getl)
                                                 + strlen(key)));
    memcpy(pkt_raw + sizeof(protocol_binary_request_getl), key, strlen(key));
    uint16_t vbucketId = 0;
    uint8_t extlen = 8;
    uint32_t expiration = 25;

    protocol_binary_request_getl *gl = (protocol_binary_request_getl*)pkt_raw;

    gl->message.header.request.opcode = CMD_GET_LOCKED;
    gl->message.header.request.extlen = extlen;
    gl->message.header.request.bodylen = htonl(strlen(key) + gl->message.header.request.extlen);
    gl->message.header.request.keylen = htons(strlen(key));
    gl->message.header.request.vbucket = htons(vbucketId);
    gl->message.body.expiration = htonl(expiration);

    protocol_binary_request_header *pkt = &gl->message.header;

    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "Getl Failed");

    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
          "expected the key to be missing...");
    if (last_body != NULL && (strcmp(last_body, "NOT_FOUND") != 0)) {
        fprintf(stderr, "Should have returned NOT_FOUND. Getl Failed");
        abort();
    }

    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, key, "lockdata", &i, 0, vbucketId)
          == ENGINE_SUCCESS, "Failed to store an item.");

    /* retry getl, should succeed */
    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "Lock failed");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected to be able to getl on first try");
    check(strcmp("lockdata", last_body) == 0, "Body was malformed.");

    /* wait 16 seconds */
    testHarness.time_travel(16);

    /* lock's taken so this should fail */
    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "Lock failed");
    check(last_status == PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
          "Expected to fail getl on second try");

    if (last_body != NULL && (strcmp(last_body, "LOCK_ERROR") != 0)) {
        fprintf(stderr, "Should have returned LOCK_ERROR. Getl Failed");
        abort();
    }

    check(store(h, h1, NULL, OPERATION_SET, key, "lockdata2", &i, 0, vbucketId)
          != ENGINE_SUCCESS, "Should have failed to store an item.");

    /* wait another 10 seconds */
    testHarness.time_travel(10);

    /* retry set, should succeed */
    check(store(h, h1, NULL, OPERATION_SET, key, "lockdata", &i, 0, vbucketId)
          == ENGINE_SUCCESS, "Failed to store an item.");

    /* acquire lock, should succeed */
    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "Lock failed");

    /* try an incr operation followed by a delete, both of which should fail */
    uint64_t cas = 0;
    uint64_t result = 0;

    check(h1->arithmetic(h, NULL, key, 2, true, false, 1, 1, 0,
                         &cas, &result,
                         0)  == ENGINE_TMPFAIL, "Incr failed");


    check(h1->remove(h, NULL, key, 2, 0, 0) == ENGINE_TMPFAIL,
          "Delete failed");


    /* bug MB 2699 append after getl should fail with ENGINE_TMPFAIL */

    testHarness.time_travel(26);

    char binaryData1[] = "abcdefg\0gfedcba";
    char binaryData2[] = "abzdefg\0gfedcba";

    check(storeCasVb11(h, h1, NULL, OPERATION_SET, key,
                       binaryData1, sizeof(binaryData1) - 1, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed set.");

    /* acquire lock, should succeed */
    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "Lock failed");


    /* append should fail */
    check(storeCasVb11(h, h1, NULL, OPERATION_APPEND, key,
                       binaryData2, sizeof(binaryData2) - 1, 82758, &i, 0, 0)
          == ENGINE_TMPFAIL,
          "Append should fail.");

    /* bug MB 3252 & MB 3354.
     * 1. Set a key with an expiry value.
     * 2. Take a lock on the item before it expires
     * 3. Wait for the item to expire
     * 4. Perform a CAS operation, should fail
     * 5. Perform a set operation, should succeed
     */
    const char *ekey = "test_expiry";
    const char *edata = "some test data here.";

    item *it = NULL;

    check(h1->allocate(h, NULL, &it, ekey, strlen(ekey), strlen(edata), 0, 2, 0, 0)
        == ENGINE_SUCCESS, "Allocation Failed");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, edata, strlen(edata));

    check(h1->store(h, NULL, it, &cas, OPERATION_SET, 0) ==
        ENGINE_SUCCESS, "Failed to Store item");
    check_key_value(h, h1, ekey, edata, strlen(edata));
    h1->release(h, NULL, it);

    /* item created. lock it and wait for the object to expire */
    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "Lock failed");

    testHarness.time_travel(3);
    cas = last_cas;

    /* cas should fail */
    check(storeCasVb11(h, h1, NULL, OPERATION_CAS, ekey,
                       binaryData1, sizeof(binaryData1) - 1, 82758, &i, cas, 0)
          != ENGINE_SUCCESS,
          "CAS succeeded.");

    /* but a simple store should succeed */
    check(store(h, h1, NULL, OPERATION_SET, ekey, edata, &i, 0, vbucketId)
          == ENGINE_SUCCESS, "Failed to store an item.");

    return SUCCESS;
}

static enum test_result test_unl(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {

    const char *key = "k2";
    uint16_t vbucketId = 0;

    protocol_binary_request_header *pkt = create_packet(CMD_GET_LOCKED,
                                                        key, "");
    pkt->request.vbucket = htons(vbucketId);

    protocol_binary_request_header *pkt_ul = create_packet(CMD_UNLOCK_KEY,
                                                        key, "");
    pkt_ul->request.vbucket = htons(vbucketId);

    check(h1->unknown_command(h, NULL, pkt_ul, add_response, NULL) == ENGINE_SUCCESS,
          "Getl Failed");

    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
          "expected the key to be missing...");
    if (last_body != NULL && (strcmp(last_body, "NOT_FOUND") != 0)) {
        fprintf(stderr, "Should have returned NOT_FOUND. Unl Failed");
        abort();
    }

    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, key, "lockdata", &i, 0, vbucketId)
          == ENGINE_SUCCESS, "Failed to store an item.");

    /* getl, should succeed */
    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "Lock failed");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected to be able to getl on first try");

    /* save the returned cas value for later */
    uint64_t cas = last_cas;

    /* lock's taken unlocking with a random cas value should fail */
    check(h1->unknown_command(h, NULL, pkt_ul, add_response, NULL) == ENGINE_SUCCESS,
          "Unlock failed");
    check(last_status == PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
          "Expected to fail getl on second try");

    if (last_body != NULL && (strcmp(last_body, "UNLOCK_ERROR") != 0)) {
        fprintf(stderr, "Should have returned UNLOCK_ERROR. Unl Failed");
        abort();
    }

    /* set the correct cas value in the outgoing request */
    pkt_ul->request.cas = cas;

    check(h1->unknown_command(h, NULL, pkt_ul, add_response, NULL) == ENGINE_SUCCESS,
          "Unlock failed");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected to succed unl with correct cas");

    /* acquire lock, should succeed */
    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "Lock failed");

    pkt_ul->request.cas = last_cas;

    /* wait 16 seconds */
    testHarness.time_travel(16);

    /* lock has expired, unl should fail */
    check(h1->unknown_command(h, NULL, pkt_ul, add_response, NULL) == ENGINE_SUCCESS,
          "Unlock failed");
    check(last_status == PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
          "Expected to fail unl on lock timeout");

    return SUCCESS;
}

static bool set_vbucket_state(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                              uint16_t vb, vbucket_state_t state) {

    protocol_binary_request_set_vbucket req;
    protocol_binary_request_header *pkt;
    pkt = reinterpret_cast<protocol_binary_request_header*>(&req);
    memset(&req, 0, sizeof(req));

    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_SET_VBUCKET;
    req.message.header.request.vbucket = htons(vb);
    req.message.body.state = static_cast<vbucket_state_t>(htonl(state));

    if (h1->unknown_command(h, NULL, pkt, add_response, NULL) != ENGINE_SUCCESS) {
        return false;
    }

    return last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static bool set_flush_param(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                            const char *param, const char *val) {
    protocol_binary_request_header *pkt = create_packet(CMD_SET_FLUSH_PARAM,
                                                        param, val);
    if (h1->unknown_command(h, NULL, pkt, add_response, NULL) != ENGINE_SUCCESS) {
        return false;
    }

    return last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static bool set_tap_param(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                            const char *param, const char *val) {
    protocol_binary_request_header *pkt = create_packet(CMD_SET_TAP_PARAM,
                                                        param, val);
    if (h1->unknown_command(h, NULL, pkt, add_response, NULL) != ENGINE_SUCCESS) {
        return false;
    }

    return last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

static bool verify_vbucket_state(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                 uint16_t vb, vbucket_state_t expected,
                                 bool mute = false) {

    protocol_binary_request_get_vbucket req;
    protocol_binary_request_header *pkt;
    pkt = reinterpret_cast<protocol_binary_request_header*>(&req);
    memset(&req, 0, sizeof(req));

    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_GET_VBUCKET;
    req.message.header.request.vbucket = htons(vb);

    ENGINE_ERROR_CODE errcode = h1->unknown_command(h, NULL, pkt, add_response, NULL);
    if (errcode != ENGINE_SUCCESS) {
        if (!mute) {
            fprintf(stderr, "Error code when getting vbucket %d\n", errcode);
        }
        return false;
    }

    if (last_status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        if (!mute) {
            fprintf(stderr, "Last protocol status was %d (%s)\n",
                    last_status, last_body ? last_body : "unknown");
        }
        return false;
    }

    vbucket_state_t state;
    memcpy(&state, last_body, sizeof(state));
    state = static_cast<vbucket_state_t>(ntohl(state));
    return state == expected;
}

static bool verify_vbucket_missing(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                   uint16_t vb) {
    char vbid[8];
    snprintf(vbid, sizeof(vbid), "vb_%d", vb);
    std::string vbstr(vbid);

    // Try up to three times to verify the bucket is missing.  Bucket
    // state changes are async.
    vals.clear();
    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");

    if (vals.find(vbstr) == vals.end()) {
        return true;
    }

    std::cerr << "Expected bucket missing, got " << vals[vbstr] << std::endl;

    return false;
}

static int get_int_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                        const char *statname, const char *statkey = NULL) {
    vals.clear();
    check(h1->get_stats(h, NULL, statkey, statkey == NULL ? 0 : strlen(statkey),
                        add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    std::string s = vals[statname];
    return atoi(s.c_str());
}

static long long get_long_stat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                        const char *statname, const char *statkey = NULL) {
    vals.clear();
    check(h1->get_stats(h, NULL, statkey, statkey == NULL ? 0 : strlen(statkey),
                        add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    std::string s = vals[statname];
    return atoll(s.c_str());
}

static std::string gen_new_config(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, int &warmup,
            uint32_t keys, uint32_t blobsize, uint64_t max_size, int dgm = 1,
            int max_evict_entries = 500000, std::string kvs_config = "t/kv_multikv.json",
            std::string other_config = "") {

    // Following the proportion: 10K locks per 2M keys
    uint32_t ht_locks = keys / 200;

    // Pick an appropriate number from prime_size_table
    uint32_t ht_size = (keys < 2000000 ? 1572869 :
                       (keys < 3500000 ? 3145721 : 
                       (keys < 7000000 ? 6291449 : 12582917)));

    // Query max_size and check if it is valid based on the requirements
    uint64_t evictionheadroom = get_long_stat(h, h1, "eviction_headroom");
    // Overhead = mem_used + Keys * sizeof(StoredValue) + mutation_mem_threshold
    // + eviction_headroom + parallelism
    uint64_t required = get_long_stat(h, h1, "mem_used") + keys * 100 + max_size / 10 + evictionheadroom + 6000000;
    char msg[1024];
    snprintf(msg, 1024, "Not enough memory to run the test. Max_size = %llu, required = %llu\nCleanup db files as well\n",
            (unsigned long long int)max_size, (unsigned long long int)required);
    check(max_size >= required, msg);

    // Compute new max_size
    if (max_size < required) {
        max_size = required + (keys * blobsize) / dgm;
    }

    if (dgm == 1) {
        warmup = 0;
    } else {
        warmup = keys / dgm;
    }

    std::stringstream ss;

    ss << "chk_period=60;"
       << "max_size=" << max_size << ";"
       << "eviction_headroom=" << evictionheadroom << ";"
       << "ht_size=" << ht_size << ";"
       << "ht_locks=" << ht_locks << ";"
       << "max_evict_entries=" << max_evict_entries << ";"
       << "kvstore_config_file="<< kvs_config;

    if (other_config.size()) {
        ss << ";" << other_config;
    }

    return ss.str();
}

static void verify_curr_items(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                              int exp, const char *msg) {
    int curr_items = get_int_stat(h, h1, "curr_items");
    if (curr_items != exp) {
        std::cerr << "Expected "<< exp << " curr_items after " << msg
                  << ", got " << curr_items << std::endl;
        abort();
    }
}

static void wait_for_stat_change(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                 const char *stat, int initial) {
    useconds_t sleepTime = 128;
    while (get_int_stat(h, h1, stat) == initial) {
        decayingSleep(&sleepTime);
    }
}

static void wait_for_stat_match(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                 const std::string stat, std::string match) {
    vals.clear();
    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");

    useconds_t sleepTime = 128;
    while (vals[stat] != match) {
        decayingSleep(&sleepTime);
        vals.clear();
        check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
              "Failed to get stats.");
    }
}

static void wait_for_flusher_to_settle(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    useconds_t sleepTime = 128;
    while (get_int_stat(h, h1, "ep_flusher_todo")
           + get_int_stat(h, h1, "ep_queue_size")
           + get_int_stat(h, h1, "ep_uncommitted_items") > 0) {
        decayingSleep(&sleepTime);
    }
    wait_for_stat_change(h, h1, "ep_commit_num", 0);
}

static void wait_for_persisted_value(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                     const char *key, const char *val,
                                     uint16_t vbucketId=0) {

    item *i = NULL;
    int commitNum = get_int_stat(h, h1, "ep_commit_num");
    check(store(h, h1, NULL, OPERATION_SET, key, val, &i, 0, vbucketId) == ENGINE_SUCCESS,
          "Failed to store an item.");

    // Wait for persistence...
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_change(h, h1, "ep_commit_num", commitNum);
    h1->release(h, NULL, i);
}

static enum test_result test_wrong_vb_mutation(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                               ENGINE_STORE_OPERATION op) {
    item *i = NULL;
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    check(store(h, h1, NULL, op,
                "key", "somevalue", &i, 11, 1) == ENGINE_NOT_MY_VBUCKET,
        "Expected not_my_vbucket");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_pending_vb_mutation(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                                 ENGINE_STORE_OPERATION op) {
    const void *cookie = testHarness.create_cookie();
    testHarness.set_ewouldblock_handling(cookie, false);
    item *i = NULL;
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending), "Failed to set vbucket state.");
    check(verify_vbucket_state(h, h1, 1, vbucket_state_pending), "Bucket state was not set to pending.");
    check(store(h, h1, cookie, op,
                "key", "somevalue", &i, 11, 1) == ENGINE_EWOULDBLOCK,
        "Expected woodblock");
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_replica_vb_mutation(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                                 ENGINE_STORE_OPERATION op) {
    item *i = NULL;
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    check(verify_vbucket_state(h, h1, 1, vbucket_state_replica), "Bucket state was not set to replica.");
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    check(store(h, h1, NULL, op,
                "key", "somevalue", &i, 11, 1) == ENGINE_NOT_MY_VBUCKET,
        "Expected not my vbucket");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

//
// ----------------------------------------------------------------------
// The actual tests are below.
// ----------------------------------------------------------------------
//

static enum test_result test_get_miss(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(verify_key(h, h1, "k") == ENGINE_KEY_ENOENT, "Expected miss.");
    return SUCCESS;
}

static enum test_result test_init_fail(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // Restart once to ensure written to disk.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              "kvstore_config_file=t/kv_non_existent_path.json",
                              false, false);

    check(h1->initialize(h, "kvstore_config_file=t/kv_non_existent_path.json")
          == ENGINE_FAILED, "Failed to fail to initialize");

    // This test will crash *after* this if it can't successfully
    // destroy the engine.
    return SUCCESS;
}

static enum test_result test_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(ENGINE_SUCCESS ==
          store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
          "Error setting.");
    return SUCCESS;
}

static enum test_result store_with_cksum(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
        const char *cookie,   
        ENGINE_STORE_OPERATION op, const std::string key,
        const std::string val, item *it) {
    DataIntegrity *di = DataIntegrity::getDi(DEFAULT_CRC_STR); 
    std::string cksum = di->getCksum(val.c_str(), val.size());
    std::stringstream ss;
    ss << "0002:" << cksum; 
    std::string out = ss.str();
    uint64_t cas = 0;
    uint16_t vb = 0;
    uint32_t flags  = 0;

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    rv = h1->allocate(h, cookie, &it,
            key.c_str(), key.size(),
            val.size(), flags, 3600, cksum.c_str(), cksum.size());
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, cookie, it, &info)) {
        abort();
    }

    memcpy(info.value[0].iov_base, val.c_str(), val.size());
    h1->item_set_cas(h, cookie, it, cas);

    rv = h1->store(h, cookie, it, &cas, op, vb);

    h1->release(h, NULL, it);
    return SUCCESS;
}

static enum test_result test_set_with_cksum(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(SUCCESS ==
          store_with_cksum(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i),
          "Error setting.");
    return SUCCESS;
}

struct handle_pair {
    ENGINE_HANDLE *h;
    ENGINE_HANDLE_V1 *h1;
};

typedef struct {
    ENGINE_HANDLE *h;
    ENGINE_HANDLE_V1 *h1;
    const key_spec_t *keyspec;
    const char *value;
    int iterations;
    uint32_t wait;
} set_key_thread_params;

typedef struct {
    ENGINE_HANDLE *h;
    ENGINE_HANDLE_V1 *h1;
    std::vector<std::string> *keys;
} random_set_key_thread_params;

typedef struct {
    ENGINE_HANDLE *h;
    ENGINE_HANDLE_V1 *h1;
    std::set<key_spec_t> *expectedKeys;
    std::string streamName;
    uint32_t wait;
} tap_stream_thread_params;

extern "C" {
    static void* conc_del_set_thread(void *arg) {
        struct handle_pair *hp = static_cast<handle_pair *>(arg);
        item *it = NULL;

        for (int i = 0; i < 5000; ++i) {
            store(hp->h, hp->h1, NULL, OPERATION_ADD,
                  "key", "somevalue", &it);
            usleep(10);
            check(ENGINE_SUCCESS ==
                  store(hp->h, hp->h1, NULL, OPERATION_SET,
                        "key", "somevalue", &it),
                  "Error setting.");
            usleep(10);
            // Ignoring the result here -- we're racing.
            hp->h1->remove(hp->h, NULL, "key", 3, 0, 0);
            usleep(10);
        }
        return NULL;
    }

    static void* conc_set_key_thread(void *arg) {
        set_key_thread_params *params = static_cast<set_key_thread_params *>(arg);
        const char *key = params->keyspec->key.c_str();
        uint16_t vbucketid = params->keyspec->vbucketid;
        item *it = NULL;

        usleep(params->wait);

        for (int i = 0; i < params->iterations; i++) {
            check(
                  store(params->h, params->h1, NULL, OPERATION_SET,
                        key, params->value, &it, 0, vbucketid) == ENGINE_SUCCESS,
                  "Thread failed to store an item");
        }

        return NULL;
    }

    static void* conc_del_key_thread(void *arg) {
        set_key_thread_params *params = static_cast<set_key_thread_params *>(arg);
        const char *key = params->keyspec->key.c_str();
        uint16_t vbucketid = params->keyspec->vbucketid;

        usleep(params->wait);

        check(
              params->h1->remove(params->h, NULL, key, strlen(key), 0, vbucketid)
              == ENGINE_SUCCESS,
              "Failed remove with value.");

        return NULL;
    }

    static void* tap_stream_thread(void *arg) {
        tap_stream_thread_params *params = static_cast<tap_stream_thread_params *>(arg);
        std::set<key_spec_t> *expectedKeys = params->expectedKeys;

        usleep(params->wait);

        std::set<uint16_t> vbuckets;
        for (std::set<key_spec_t>::iterator it = expectedKeys->begin();
             it != expectedKeys->end(); it++) {

            vbuckets.insert(it->vbucketid);
        }

        uint16_t *vbucketfilter = new uint16_t[1 + vbuckets.size()];
        vbucketfilter[0] = htons((uint16_t) vbuckets.size());
        off_t off = 1;

        for (std::set<uint16_t>::iterator it = vbuckets.begin();
             it != vbuckets.end(); it++) {

            vbucketfilter[off++] = htons(*it);
        }

        const void *cookie = testHarness.create_cookie();
        testHarness.lock_cookie(cookie);

        TAP_ITERATOR iter = params->h1->get_tap_iterator(params->h, cookie,
                                                         params->streamName.c_str(),
                                                         params->streamName.length(),
                                                         TAP_CONNECT_FLAG_LIST_VBUCKETS | TAP_CONNECT_SUPPORT_ACK,
                                                         static_cast<void *>(vbucketfilter),
                                                         sizeof(uint16_t) + (sizeof(uint16_t) * vbuckets.size()));
        check(iter != NULL, "Failed to create a tap iterator");

        item *it;
        Item *item;
        void *engine_specific;
        uint16_t nengine_specific;
        uint8_t ttl;
        uint16_t flags;
        uint32_t seqno;
        tap_event_t event;
        std::set<key_spec_t> keysReceived;
        bool done = false;
        uint16_t vbid;

        do {
            event = iter(params->h, cookie, &it, &engine_specific,
                         &nengine_specific, &ttl, &flags,
                         &seqno, &vbid);

            switch (event) {
            case TAP_PAUSE:
                done = (keysReceived.size() == expectedKeys->size());
                if (!done) {
                    testHarness.waitfor_cookie(cookie);
                }
                break;
            case TAP_NOOP:
            case TAP_OPAQUE:
                break;
            case TAP_MUTATION:
                item = reinterpret_cast<Item *>(it);

                check(vbuckets.find(item->getVBucketId()) != vbuckets.end(),
                      "Received an item for a vbucket we don't subscribe to");

                if (expectedKeys->find(*item) != expectedKeys->end()) {
                    keysReceived.insert(*item);
                }

                if (keysReceived.size() == expectedKeys->size()) {
                    // for testing purposes, only ACK at the end, to see if the
                    // implicit ACKing increments item replica count and unblocks
                    // SYNC on replication listeners
                    testHarness.unlock_cookie(cookie);
                    params->h1->tap_notify(params->h, cookie, NULL, 0, 0,
                                           PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                           TAP_ACK, seqno, NULL, 0, 0,
                                           0, 0, 0, NULL, 0, 0 , DI_CKSUM_DISABLED_STR);
                    testHarness.lock_cookie(cookie);
                }
                break;
            case TAP_DISCONNECT:
                done = true;
                break;
            default:
                std::cerr << "Unexpected event:  " << event << std::endl;
                done = true;
            }
        } while (!done);

        testHarness.unlock_cookie(cookie);
        params->h1->release(params->h, cookie, it);
        delete [] vbucketfilter;

        check(keysReceived.size() == expectedKeys->size(),
              "Didn't received all the expected items from the tap stream.");

        return NULL;
    }
}

static enum test_result test_conc_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {

    const int n_threads = 8;
    pthread_t threads[n_threads];
    struct handle_pair hp = {h, h1};

    wait_for_persisted_value(h, h1, "key", "value1");

    for (int i = 0; i < n_threads; i++) {
        int r = pthread_create(&threads[i], NULL, conc_del_set_thread, &hp);
        assert(r == 0);
    }

    for (int i = 0; i < n_threads; i++) {
        void *trv = NULL;
        int r = pthread_join(threads[i], &trv);
        assert(r == 0);
    }

    wait_for_flusher_to_settle(h, h1);

    // There should be no more newer items than deleted items.
    if (std::abs(get_int_stat(h, h1, "ep_total_new_items") -
                 get_int_stat(h, h1, "ep_total_del_items")) > 1) {
        std::cout << "new:       " << get_int_stat(h, h1, "ep_total_new_items") << std::endl
                  << "rm:        " << get_int_stat(h, h1, "ep_total_del_items") << std::endl
                  << "persisted: " << get_int_stat(h, h1, "ep_total_persisted") << std::endl
                  << "commits:   " << get_int_stat(h, h1, "ep_commit_num") << std::endl;
        abort();
    }

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true, false);

    assert(0 == get_int_stat(h, h1, "ep_warmed_dups"));

    return SUCCESS;
}

static enum test_result test_set_get_hit(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "store failure");
    check_key_value(h, h1, "key", "somevalue", 9);
    return SUCCESS;
}

static enum test_result test_set_get_hit_bin(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char binaryData[] = "abcdefg\0gfedcba";
    assert(sizeof(binaryData) != strlen(binaryData));

    item *i = NULL;
    check(ENGINE_SUCCESS ==
          storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                       binaryData, sizeof(binaryData), 82758, &i, 0, 0),
          "Failed to set.");
    return check_key_value(h, h1, "key", binaryData, sizeof(binaryData));
}

static enum test_result test_set_change_flags(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to set.");

    item_info info;
    uint32_t flags = 828258;
    check(get_value(h, h1, "key", &info), "Failed to get value.");
    assert(info.flags != flags);

    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                       "newvalue", strlen("newvalue"), flags, &i, 0, 0) == ENGINE_SUCCESS,
          "Failed to set again.");

    check(get_value(h, h1, "key", &info), "Failed to get value.");

    return info.flags == flags ? SUCCESS : FAIL;
}

static enum test_result test_cas(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to do initial set.");
    check(store(h, h1, NULL, OPERATION_CAS, "key", "failcas", &i) != ENGINE_SUCCESS,
          "Failed to fail initial CAS.");
    check_key_value(h, h1, "key", "somevalue", 9);

    check(h1->get(h, NULL, &i, "key", 3, 0) == ENGINE_SUCCESS,
          "Failed to get value.");

    item_info info;
    info.nvalue = 1;
    check(h1->get_item_info(h, NULL, i, &info), "Failed to get item info.");

    check(store(h, h1, NULL, OPERATION_CAS, "key", "winCas", &i,
                info.cas) == ENGINE_SUCCESS,
          "Failed to store CAS");
    check_key_value(h, h1, "key", "winCas", 6);

    uint64_t cval = 99999;
    check(store(h, h1, NULL, OPERATION_CAS, "non-existing", "winCas", &i,
                cval) == ENGINE_KEY_ENOENT,
          "CAS for non-existing key returned the wrong error code");
    return SUCCESS;
}

static enum test_result test_add(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_ADD,"key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to add value.");
    check(store(h, h1, NULL, OPERATION_ADD,"key", "somevalue", &i) == ENGINE_NOT_STORED,
          "Failed to fail to re-add value.");

    // This aborts on failure.
    check_key_value(h, h1, "key", "somevalue", 9);

    // Expiration above was an hour, so let's go to The Future
    testHarness.time_travel(3800);

    check(store(h, h1, NULL, OPERATION_ADD,"key", "newvalue", &i) == ENGINE_SUCCESS,
          "Failed to add value again.");

    return check_key_value(h, h1, "key", "newvalue", 8);
}

static enum test_result test_replace(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_REPLACE,"key", "somevalue", &i) == ENGINE_NOT_STORED,
          "Failed to fail to replace non-existing value.");
    check(store(h, h1, NULL, OPERATION_SET,"key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to set value.");
    check(store(h, h1, NULL, OPERATION_REPLACE,"key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed to replace existing value.");
    return check_key_value(h, h1, "key", "somevalue", 9);
}

static enum test_result test_incr_miss(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas = 0, result = 0;
    h1->arithmetic(h, NULL, "key", 3, true, false, 1, 0, 0,
                   &cas, &result,
                   0);
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected to not find key");
    return SUCCESS;
}

static enum test_result test_incr_default(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas = 0, result = 0;
    check(h1->arithmetic(h, NULL, "key", 3, true, true, 1, 1, 0,
                         &cas, &result,
                         0) == ENGINE_SUCCESS,
          "Failed first arith");
    check(result == 1, "Failed result verification.");

    check(h1->arithmetic(h, NULL, "key", 3, true, false, 1, 1, 0,
                         &cas, &result,
                         0) == ENGINE_SUCCESS,
          "Failed second arith.");
    check(result == 2, "Failed second result verification.");

    check(h1->arithmetic(h, NULL, "key", 3, true, true, 1, 1, 0,
                         &cas, &result,
                         0) == ENGINE_SUCCESS,
          "Failed third arith.");
    check(result == 3, "Failed third result verification.");

    return check_key_value(h, h1, "key", "3", 1);
}

static enum test_result test_append(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;

    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                       "\r\n", 2, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed set.");

    check(storeCasVb11(h, h1, NULL, OPERATION_APPEND, "key",
                       "foo\r\n", 5, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed append.");

    check_key_value(h, h1, "key", "\r\nfoo\r\n", 7);

    char binaryData1[] = "abcdefg\0gfedcba\r\n";
    char binaryData2[] = "abzdefg\0gfedcba\r\n";
    size_t dataSize = 20*1024*1024;
    char *bigBinaryData3 = new char[dataSize];
    memset(bigBinaryData3, '\0', dataSize);

    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                       binaryData1, sizeof(binaryData1) - 1, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed set.");

    check(storeCasVb11(h, h1, NULL, OPERATION_APPEND, "key",
                       bigBinaryData3, dataSize, 82758, &i, 0, 0)
          == ENGINE_E2BIG,
          "Expected append failure.");
    delete bigBinaryData3;

    check(storeCasVb11(h, h1, NULL, OPERATION_APPEND, "key",
                       binaryData2, sizeof(binaryData2) - 1, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed append.");

    std::string expected;
    expected.append(binaryData1, sizeof(binaryData1) - 1);
    expected.append(binaryData2, sizeof(binaryData2) - 1);

    return check_key_value(h, h1, "key", expected.data(), expected.length());
}

static enum test_result test_prepend(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;

    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                       "\r\n", 2, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed set.");

    check(storeCasVb11(h, h1, NULL, OPERATION_PREPEND, "key",
                       "foo\r\n", 5, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed append.");

    check_key_value(h, h1, "key", "foo\r\n\r\n", 7);

    char binaryData1[] = "abcdefg\0gfedcba\r\n";
    char binaryData2[] = "abzdefg\0gfedcba\r\n";
    size_t dataSize = 20*1024*1024;
    char *bigBinaryData3 = new char[dataSize];
    memset(bigBinaryData3, '\0', dataSize);

    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                       binaryData1, sizeof(binaryData1) - 1, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed set.");

    check(storeCasVb11(h, h1, NULL, OPERATION_PREPEND, "key",
                       bigBinaryData3, dataSize, 82758, &i, 0, 0)
          == ENGINE_E2BIG,
          "Expected prepend failure.");
    delete bigBinaryData3;

    check(storeCasVb11(h, h1, NULL, OPERATION_PREPEND, "key",
                       binaryData2, sizeof(binaryData2) - 1, 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed append.");

    std::string expected;
    expected.append(binaryData2, sizeof(binaryData2) - 1);
    expected.append(binaryData1, sizeof(binaryData1) - 1);

    return check_key_value(h, h1, "key", expected.data(), expected.length());
}

static enum test_result test_incr(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas = 0, result = 0;
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_ADD,"key", "1", &i) == ENGINE_SUCCESS,
          "Failed to add value.");

    check(h1->arithmetic(h, NULL, "key", 3, true, false, 1, 1, 0,
                         &cas, &result,
                         0) == ENGINE_SUCCESS,
          "Failed to incr value.");

    return check_key_value(h, h1, "key", "2", 1);
}

static enum test_result test_bug2799(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas = 0, result = 0;
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_ADD, "key", "1", &i) == ENGINE_SUCCESS,
          "Failed to add value.");

    check(h1->arithmetic(h, NULL, "key", 3, true, false, 1, 1, 0,
                         &cas, &result,
                         0) == ENGINE_SUCCESS,
          "Failed to incr value.");

    check_key_value(h, h1, "key", "2", 1);

    testHarness.time_travel(3617);

    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    return SUCCESS;
}

static enum test_result test_flush(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    // First try to delete something we know to not be there.
    check(h1->remove(h, NULL, "key", 3, 0, 0) == ENGINE_KEY_ENOENT,
          "Failed to fail initial delete.");
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    check_key_value(h, h1, "key", "somevalue", 9);

    // Try flush without enabling flush all.. Should fail
    check(h1->flush(h, NULL, 0) == ENGINE_ENOTSUP,
          "Failed to fail flush");

    set_flush_param(h, h1, "enable_flushall", "true");
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS,
          "Failed to flush");
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");

    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed post-flush set.");
    check_key_value(h, h1, "key", "somevalue", 9);

    return SUCCESS;
}

static enum test_result test_flush_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    int mem_used = get_int_stat(h, h1, "mem_used");
    int overhead = get_int_stat(h, h1, "ep_overhead");
    int cacheSize = get_int_stat(h, h1, "ep_total_cache_size");
    int nonResident = get_int_stat(h, h1, "ep_num_non_resident");

    int itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET, "key2", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

    check(ENGINE_SUCCESS == verify_key(h, h1, "key"), "Expected key");
    check(ENGINE_SUCCESS == verify_key(h, h1, "key2"), "Expected key2");

    check_key_value(h, h1, "key", "somevalue", 9);
    check_key_value(h, h1, "key2", "somevalue", 9);

    int mem_used2 = get_int_stat(h, h1, "mem_used");
    int overhead2 = get_int_stat(h, h1, "ep_overhead");
    int cacheSize2 = get_int_stat(h, h1, "ep_total_cache_size");
    int nonResident2 = get_int_stat(h, h1, "ep_num_non_resident");

    assert(mem_used2 > mem_used);
    // "mem_used2 - overhead2" (i.e., ep_kv_size) should be greater than the hashtable cache size
    // due to the checkpoint overhead
    assert(mem_used2 - overhead2 > cacheSize2);

    // Try flush without enabling flush all.. Should fail
    check(h1->flush(h, NULL, 0) == ENGINE_ENOTSUP,
          "Failed to fail flush");

    set_flush_param(h, h1, "enable_flushall", "true");
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS, "Failed to flush");
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key2"), "Expected missing key");

    wait_for_stat_match(h, h1, "curr_items_tot", "0");

    mem_used2 = get_int_stat(h, h1, "mem_used");
    overhead2 = get_int_stat(h, h1, "ep_overhead");
    cacheSize2 = get_int_stat(h, h1, "ep_total_cache_size");
    nonResident2 = get_int_stat(h, h1, "ep_num_non_resident");

    assert(mem_used2 == mem_used);
    assert(overhead2 == overhead);
    assert(nonResident2 == nonResident);
    assert(cacheSize2 == cacheSize);

    return SUCCESS;
}

static enum test_result test_flush_multiv(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(set_vbucket_state(h, h1, 2, vbucket_state_active), "Failed to set vbucket state.");
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    check(store(h, h1, NULL, OPERATION_SET, "key2", "somevalue", &i,
                0, 2) == ENGINE_SUCCESS,
          "Failed set in vb2.");

    check(ENGINE_SUCCESS == verify_key(h, h1, "key"), "Expected key");
    check(ENGINE_SUCCESS == verify_vb_key(h, h1, "key2", 2), "Expected key2");

    check_key_value(h, h1, "key", "somevalue", 9);
    check_key_value(h, h1, "key2", "somevalue", 9, 2);

    // Try flush without enabling flush all.. Should fail
    check(h1->flush(h, NULL, 0) == ENGINE_ENOTSUP,
          "Failed to fail flush");

    set_flush_param(h, h1, "enable_flushall", "true");
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS, "Failed to flush");

    vals.clear();
    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_flush_all") != vals.end(), "Failed to get the status of flush_all");

    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    check(ENGINE_KEY_ENOENT == verify_vb_key(h, h1, "key2", 2), "Expected missing key");

    return SUCCESS;
}

static enum test_result test_flush_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    // First try to delete something we know to not be there.
    check(h1->remove(h, NULL, "key", 3, 0, 0) == ENGINE_KEY_ENOENT,
          "Failed to fail initial delete.");
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    check_key_value(h, h1, "key", "somevalue", 9);

    wait_for_flusher_to_settle(h, h1);
    // Restart once to ensure written to disk.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true, false);

    // Read value from disk.
    check_key_value(h, h1, "key", "somevalue", 9);
    // Try flush without enabling flush all.. Should fail
    check(h1->flush(h, NULL, 0) == ENGINE_ENOTSUP,
          "Failed to fail flush");

    set_flush_param(h, h1, "enable_flushall", "true");
    // Flush
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS,
          "Failed to flush");

    check(store(h, h1, NULL, OPERATION_SET, "key2", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed post-flush set.");
    check_key_value(h, h1, "key2", "somevalue", 9);

    wait_for_flusher_to_settle(h, h1);
    // Restart again, ensure written to disk.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true, false);

    check(store(h, h1, NULL, OPERATION_SET, "key3", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed post-flush, post-restart set.");
    check_key_value(h, h1, "key3", "somevalue", 9);

    // Read value again, should not be there.
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    return SUCCESS;
}

static enum test_result test_flush_multiv_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(set_vbucket_state(h, h1, 2, vbucket_state_active), "Failed to set vbucket state.");
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, i);
    check(store(h, h1, NULL, OPERATION_SET, "key2", "somevalue", &i,
                0, 2) == ENGINE_SUCCESS,
          "Failed set in vb2.");
    h1->release(h, NULL, i);

    // Restart once to ensure written to disk.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true, false);

    // Read value from disk.
    check_key_value(h, h1, "key", "somevalue", 9);

    // Try flush without enabling flush all.. Should fail
    check(h1->flush(h, NULL, 0) == ENGINE_ENOTSUP,
          "Failed to fail flush");

    set_flush_param(h, h1, "enable_flushall", "true");
    // Flush
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS,
          "Failed to flush");

    // Restart again, ensure written to disk.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true, false);

    // Read value again, should not be there.
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    check(verify_vbucket_missing(h, h1, 2), "Bucket 2 came back.");
    return SUCCESS;
}

static enum test_result test_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    // First try to delete something we know to not be there.
    check(h1->remove(h, NULL, "key", 3, 0, 0) == ENGINE_KEY_ENOENT,
          "Failed to fail initial delete.");
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    check_key_value(h, h1, "key", "somevalue", 9);
    check(h1->remove(h, NULL, "key", 3, 0, 0) == ENGINE_SUCCESS,
          "Failed remove with value.");
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");

    // Can I time travel to an expired object and delete it?
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    testHarness.time_travel(3617);
    check(h1->remove(h, NULL, "key", 3, 0, 0) == ENGINE_KEY_ENOENT,
          "Did not get ENOENT removing an expired object.");
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");

    return SUCCESS;
}

static enum test_result test_set_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i) == ENGINE_SUCCESS,
          "Failed set.");
    check_key_value(h, h1, "key", "somevalue", 9);
    check(h1->remove(h, NULL, "key", 3, 0, 0) == ENGINE_SUCCESS,
          "Failed remove with value.");
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    wait_for_flusher_to_settle(h, h1);
    check(get_int_stat(h, h1, "curr_items") == 0, "Deleting left tombstone.");
    return SUCCESS;
}

static enum test_result test_bug2509(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    for (int j = 0; j < 10000; ++j) {
        item *itm = NULL;
        check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &itm)
              == ENGINE_SUCCESS, "Failed set.");
        usleep(10);
        check(h1->remove(h, NULL, "key", 3, 0, 0) == ENGINE_SUCCESS,
              "Failed remove with value.");
        usleep(10);
    }

    // Restart again, to verify we don't have any duplicates.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true, false);

    return get_int_stat(h, h1, "ep_warmup_dups") == 0 ? SUCCESS : FAIL;
}

static enum test_result test_bug2761(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    std::vector<std::string> keys;
    // Make a vbucket mess.
    for (int j = 0; j < 100; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }

    std::vector<std::string>::iterator it;
    for (int j = 0; j < 1000; ++j) {
        check(set_vbucket_state(h, h1, 0, vbucket_state_dead), "Failed set set vbucket 0 dead.");
        protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_DEL_VBUCKET, 0);
        check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
              "Failed to request delete bucket");
        check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
              "Expected vbucket deletion to work.");
        check(set_vbucket_state(h, h1, 0, vbucket_state_active), "Failed set set vbucket 0 active.");
        for (it = keys.begin(); it != keys.end(); ++it) {
            item *i;
            check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i)
                  == ENGINE_SUCCESS, "Failed to store a value");
        }
    }
    wait_for_flusher_to_settle(h, h1);

    set_flush_param(h, h1, "evict_min_blob_size", "1");
    for (it = keys.begin(); it != keys.end(); ++it) {
        evict_key(h, h1, it->c_str(), 0, "Ejected.");
    }
    check(set_vbucket_state(h, h1, 0, vbucket_state_dead), "Failed set set vbucket 0 dead.");
    sleep(1);
    check(set_vbucket_state(h, h1, 0, vbucket_state_active), "Failed set set vbucket 0 active.");
    for (it = keys.begin(); it != keys.end(); ++it) {
        check_key_value(h, h1, it->c_str(), it->data(), it->size(), 0);
    }
    return SUCCESS;
}

// bug 2830 related items

static void bug_2830_child(int reader, int writer) {
    alarm(60);

    sqlite3 *db;

    const char * fn = "/tmp/test.db-0.sqlite";
    if(sqlite3_open(fn, &db) !=  SQLITE_OK) {
        throw std::runtime_error("Error initializing sqlite3");
    }

    // This will immediately lock the database
    PreparedStatement pst(db, "begin immediate");
    assert(pst.execute() >= 0);

    // Signal we've got the txn so the parent can start trying to fail.
    char buf[1];
    buf[0] = 'x';
    assert(write(writer, buf, 1) == 1);

    // Wait for the signal that we've broken something
    assert(read(reader, buf, 1) == 1);

    // Let's go ahead and rollback before we close the DB.  Just to be nice.
    PreparedStatement pstrollback(db, "rollback");
    assert(pstrollback.execute() >= 0);
    sqlite3_close(db);
}

extern "C" {
    // This thread will watch for failures to begin a transaction, and
    // then signal the child that it's done enough damage so it can
    // exit.
    static void* bug2830_thread(void *arg) {
        ThreadData *td(static_cast<ThreadData*>(arg));

        const char *key = "key";
        const char *val = "value";
        int initial = get_int_stat(td->h, td->h1, "ep_item_begin_failed");

        item *i = NULL;
        check(store(td->h, td->h1, NULL, OPERATION_SET, key, val, &i, 0, 0) == ENGINE_SUCCESS,
              "Failed to store an item.");
        wait_for_stat_change(td->h, td->h1, "ep_item_begin_failed", initial);
        char buf[1];
        assert(write(td->extra, buf, 1) == 1);
        return NULL;
    }
}

static enum test_result test_bug2830(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // XXX:  Need to be able to detect the vb snapshot has run.  We can
    // do that once MB-2663 is done.  Until then, 100ms should do.
    usleep(100 * 1000);

    pid_t child;
    int p2c[2]; // parent to child
    int c2p[2]; // child to parent

    assert(pipe(p2c) == 0);
    assert(pipe(c2p) == 0);

    int childreader = p2c[0];
    int childwriter = c2p[1];

    int parentreader = c2p[0];
    int parentwriter = p2c[1];

    switch (child = fork()) {
    case 0:
        bug_2830_child(childreader, childwriter);
        exit(0);
        abort(); // not reached
    case -1:
        perror("fork");
        abort();
        break;
    }

    // Wait for the child to let us know we can start or work.
    char buf[1];
    assert(read(parentreader, buf, 1) == 1);

    // Start a thread to monitor stats and let us know when we've had
    // enough.
    ThreadData *td = new ThreadData(h, h1, parentwriter);
    pthread_t tid;
    if (pthread_create(&tid, NULL, bug2830_thread, td) != 0) {
        abort();
    }

    // Wait for the thread to indicate stuff's done.
    assert(pthread_join(tid, NULL) == 0);

    // And let us write out our data.
    wait_for_flusher_to_settle(h, h1);
    evict_key(h, h1, "key");
    check_key_value(h, h1, "key", "value", 5);

    // The child will die and we'll verify it does so safely.
    int status;
    assert(child == waitpid(child, &status, 0));
    assert(WIFEXITED(status));
    assert(WEXITSTATUS(status) == 0);

    // Verify we had a failure.
    assert(get_int_stat(h, h1, "ep_item_begin_failed") > 0);

    return SUCCESS;
}

// end of bug 2830 related items

static enum test_result test_delete_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    wait_for_persisted_value(h, h1, "key", "value1");

    check(h1->remove(h, NULL, "key", 3, 0, 0) == ENGINE_SUCCESS,
          "Failed remove with value.");

    wait_for_persisted_value(h, h1, "key", "value2");

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true, false);

    check_key_value(h, h1, "key", "value2", 6);
    check(h1->remove(h, NULL, "key", 3, 0, 0) == ENGINE_SUCCESS,
          "Failed remove with value.");

    wait_for_flusher_to_settle(h, h1);
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true, false);

    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");

    return SUCCESS;
}

static enum test_result test_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    size_t j;

    for (j=0; j<50000; j++) {
        std::stringstream sk, sv;
        sk<<"key_"<<j;
        sv<<"val_"<<j;
        item *i = NULL;

        check(store(h, h1, NULL, OPERATION_SET, sk.str().c_str(), sv.str().c_str(), &i) == ENGINE_SUCCESS,
              "Failed set.");
    }

    wait_for_flusher_to_settle(h, h1);
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              "kvstore_config_file=t/kv_multikv.json",
                              true, false);

    for (j=0; j<50000; j++) {
        std::stringstream sk, sv;
        sk<<"key_"<<j;
        sv<<"val_"<<j;
        check(check_key_value(h, h1, sk.str().c_str(), sv.str().c_str(), sv.str().length()) == SUCCESS,
                "key - value mismatch or not found");
    }

     return SUCCESS;
}

static enum test_result test_restart_bin_val(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {

    char binaryData[] = "abcdefg\0gfedcba";
    assert(sizeof(binaryData) != strlen(binaryData));

    item *i = NULL;
    check(storeCasVb11(h, h1, NULL, OPERATION_SET, "key",
                       binaryData, sizeof(binaryData), 82758, &i, 0, 0)
          == ENGINE_SUCCESS,
          "Failed set.");

    wait_for_flusher_to_settle(h, h1);
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true, false);

    return check_key_value(h, h1, "key", binaryData, sizeof(binaryData));
}

static enum test_result test_wrong_vb_get(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    check(ENGINE_NOT_MY_VBUCKET == verify_vb_key(h, h1, "key", 1),
          "Expected wrong bucket.");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_vb_get_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending), "Failed to set vbucket state.");
    const void *cookie = testHarness.create_cookie();
    testHarness.set_ewouldblock_handling(cookie, false);

    item *i = NULL;
    check(ENGINE_EWOULDBLOCK == h1->get(h, cookie, &i, "key", strlen("key"), 1),
          "Expected woodblock.");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_vb_get_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    check(ENGINE_NOT_MY_VBUCKET == verify_vb_key(h, h1, "key", 1),
          "Expected not my bucket.");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_wrong_vb_incr(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas, result;
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    check(h1->arithmetic(h, NULL, "key", 3, true, false, 1, 1, 0,
                         &cas, &result,
                         1) == ENGINE_NOT_MY_VBUCKET,
          "Expected not my vbucket.");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_vb_incr_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    testHarness.set_ewouldblock_handling(cookie, false);
    uint64_t cas, result;
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending), "Failed to set vbucket state.");
    check(h1->arithmetic(h, cookie, "key", 3, true, false, 1, 1, 0,
                         &cas, &result,
                         1) == ENGINE_EWOULDBLOCK,
          "Expected woodblock.");
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_vb_incr_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas, result;
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    check(h1->arithmetic(h, NULL, "key", 3, true, false, 1, 1, 0,
                         &cas, &result,
                         1) == ENGINE_NOT_MY_VBUCKET,
          "Expected not my bucket.");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_wrong_vb_set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_wrong_vb_mutation(h, h1, OPERATION_SET);
}

static enum test_result test_wrong_vb_cas(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_wrong_vb_mutation(h, h1, OPERATION_CAS);
}

static enum test_result test_wrong_vb_add(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_wrong_vb_mutation(h, h1, OPERATION_ADD);
}

static enum test_result test_wrong_vb_replace(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_wrong_vb_mutation(h, h1, OPERATION_REPLACE);
}

static enum test_result test_wrong_vb_append(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_wrong_vb_mutation(h, h1, OPERATION_APPEND);
}

static enum test_result test_wrong_vb_prepend(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_wrong_vb_mutation(h, h1, OPERATION_PREPEND);
}

static enum test_result test_wrong_vb_del(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    check(ENGINE_NOT_MY_VBUCKET == h1->remove(h, NULL, "key", 3, 0, 1),
          "Expected wrong bucket.");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_expiry(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char *key = "test_expiry";
    const char *data = "some test data here.";

    item *it = NULL;

    ENGINE_ERROR_CODE rv;
    rv = h1->allocate(h, NULL, &it, key, strlen(key), strlen(data), 0, 2, 0, 0);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(rv == ENGINE_SUCCESS, "Set failed.");
    check_key_value(h, h1, key, data, strlen(data));
    h1->release(h, NULL, it);

    testHarness.time_travel(3);

    assert(0 == get_int_stat(h, h1, "ep_expired"));

    check(h1->get(h, NULL, &it, key, strlen(key), 0) == ENGINE_KEY_ENOENT,
          "Item didn't expire");

    assert(1 == get_int_stat(h, h1, "ep_expired"));

    check(store(h, h1, NULL, OPERATION_SET, key, data, &it) == ENGINE_SUCCESS,
                "Failed set.");

    std::stringstream ss;
    ss << "curr_items stat should be still 1 after ";
    ss << "overwriting the key that was expired, but not purged yet";
    check(get_int_stat(h, h1, "curr_items") == 1, ss.str().c_str());

    return SUCCESS;
}

static enum test_result test_expiry_loader(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char *key = "test_expiry_loader";
    const char *data = "some test data here.";

    item *it = NULL;

    ENGINE_ERROR_CODE rv;
    rv = h1->allocate(h, NULL, &it, key, strlen(key), strlen(data), 0, 2, 0, 0);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(rv == ENGINE_SUCCESS, "Set failed.");
    check_key_value(h, h1, key, data, strlen(data));
    h1->release(h, NULL, it);

    testHarness.time_travel(3);

    check(h1->get(h, NULL, &it, key, strlen(key), 0) == ENGINE_KEY_ENOENT,
          "Item didn't expire");

    // Restart the engine to ensure the above expired item is not loaded
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true, false);
    assert(0 == get_int_stat(h, h1, "ep_warmed_up"));

    return SUCCESS;
}

static enum test_result test_expiry_flush(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char *key = "test_expiry_flush";

    item *it = NULL;

    ENGINE_ERROR_CODE rv;
    // Expiry time set to 2 seconds from now
    rv = h1->allocate(h, NULL, &it, key, strlen(key), 10, 0, 2, 0,0);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    uint64_t cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(rv == ENGINE_SUCCESS, "Set failed.");

    // wait for item expiry. Next fetch should fail
    testHarness.time_travel(3);
    check(h1->get(h, NULL, &it, key, strlen(key), 0) == ENGINE_KEY_ENOENT,
          "Item didn't expire");
    h1->release(h, NULL, it);

    // The above get should have queued a deletion that should be flushed down
    wait_for_flusher_to_settle(h, h1);
    verify_curr_items(h, h1, 0, "No items");

    return SUCCESS;
}

static enum test_result test_bug3454(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char *key = "test_expiry_duplicate_warmup";
    const char *data = "some test data here.";

    item *it = NULL;

    ENGINE_ERROR_CODE rv;
    rv = h1->allocate(h, NULL, &it, key, strlen(key), strlen(data), 0, 5, 0, 0);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(rv == ENGINE_SUCCESS, "Set failed.");
    check_key_value(h, h1, key, data, strlen(data));
    h1->release(h, NULL, it);
    wait_for_flusher_to_settle(h, h1);

    // Advance the ep_engine time by 10 sec for the above item to be expired.
    testHarness.time_travel(10);
    check(h1->get(h, NULL, &it, key, strlen(key), 0) == ENGINE_KEY_ENOENT,
          "Item didn't expire");

    rv = h1->allocate(h, NULL, &it, key, strlen(key), strlen(data), 0, 0, 0, 0);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    cas = 0;
    // Add a new item with the same key.
    rv = h1->store(h, NULL, it, &cas, OPERATION_ADD, 0);
    check(rv == ENGINE_SUCCESS, "Add failed.");
    check_key_value(h, h1, key, data, strlen(data));

    check(h1->get(h, NULL, &it, key, strlen(key), 0) == ENGINE_SUCCESS,
          "Item shouldn't expire");

    // Restart the engine to ensure the above unexpired new item is loaded
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true, false);
    assert(1 == get_int_stat(h, h1, "ep_warmed_up"));
    assert(0 == get_int_stat(h, h1, "ep_warmup_dups"));

    return SUCCESS;
}

static enum test_result test_bug3522(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const char *key = "test_expiry_no_items_warmup";
    const char *data = "some test data here.";

    item *it = NULL;

    ENGINE_ERROR_CODE rv;
    rv = h1->allocate(h, NULL, &it, key, strlen(key), strlen(data), 0, 0, 0,0);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    item_info info;
    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, data, strlen(data));

    uint64_t cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(rv == ENGINE_SUCCESS, "Set failed.");
    check_key_value(h, h1, key, data, strlen(data));
    h1->release(h, NULL, it);
    wait_for_flusher_to_settle(h, h1);

    // Add a new item with the same key and 2 sec of expiration.
    const char *new_data = "new data here.";
    rv = h1->allocate(h, NULL, &it, key, strlen(key), strlen(new_data), 0, 2, 0, 0);
    check(rv == ENGINE_SUCCESS, "Allocation failed.");

    info.nvalue = 1;
    if (!h1->get_item_info(h, NULL, it, &info)) {
        abort();
    }
    memcpy(info.value[0].iov_base, new_data, strlen(new_data));

    cas = 0;
    rv = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(rv == ENGINE_SUCCESS, "Set failed.");
    check_key_value(h, h1, key, new_data, strlen(new_data));
    h1->release(h, NULL, it);

    // time-travel 3 secs to give the item time to expire
    testHarness.time_travel(3);

    // Restart the engine.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true, false);
    assert(0 == get_int_stat(h, h1, "ep_warmed_up"));
    assert(0 == get_int_stat(h, h1, "ep_warmup_dups"));
    return SUCCESS;
}

static enum test_result test_vb_del_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    testHarness.set_ewouldblock_handling(cookie, false);
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending), "Failed to set vbucket state.");
    check(ENGINE_EWOULDBLOCK == h1->remove(h, cookie, "key", 3, 0, 1),
          "Expected woodblock.");
    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_vb_del_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    int numNotMyVBucket = get_int_stat(h, h1, "ep_num_not_my_vbuckets");
    check(ENGINE_NOT_MY_VBUCKET == h1->remove(h, NULL, "key", 3, 0, 1),
          "Expected not my vbucket.");
    wait_for_stat_change(h, h1, "ep_num_not_my_vbuckets", numNotMyVBucket);
    return SUCCESS;
}

static enum test_result test_touch(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char buffer[512];
    memset(buffer, 0, sizeof(buffer));
    protocol_binary_request_touch *req = reinterpret_cast<protocol_binary_request_touch *>(buffer);
    protocol_binary_request_header *request = reinterpret_cast<protocol_binary_request_header*>(req);

    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_TOUCH;
    req->message.header.request.extlen = 4;
    req->message.header.request.bodylen = htonl(4);
    req->message.body.expiration = ntohl(time(NULL) + 10);

    // key is a mandatory field!
    check(h1->unknown_command(h, NULL, request, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to call touch");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "Testing invalid arguments");

    // extlen is a mandatory field!
    req->message.header.request.extlen = 0;
    req->message.header.request.keylen = 4;
    check(h1->unknown_command(h, NULL, request, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to call touch");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "Testing invalid arguments");

    // Try to touch an unknown item...
    req->message.header.request.extlen = 4;
    req->message.header.request.keylen = htons(5);
    req->message.header.request.bodylen = htonl(4 + 5);
    memcpy(buffer + sizeof(req->bytes), "mykey", 5);

    check(h1->unknown_command(h, NULL, request, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to call touch");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Testing unknown key");

    // illegal vbucket
    req->message.header.request.vbucket = htons(5);
    check(h1->unknown_command(h, NULL, request, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to call touch");
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, "Testing illegal vbucket");
    req->message.header.request.vbucket = 0;

    // Store the item!
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "mykey", "somevalue", &itm) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, itm);

    check(check_key_value(h, h1, "mykey", "somevalue", strlen("somevalue")) == SUCCESS,
          "Failed to retrieve data");

    check(h1->unknown_command(h, NULL, request, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to call touch");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "touch mykey");

    // time-travel 9 secs..
    testHarness.time_travel(9);

    // The item should still exist
    check_key_value(h, h1, "mykey", "somevalue", 9);

    // time-travel 2 secs..
    testHarness.time_travel(2);

    // The item should have expired now...
    check(h1->get(h, NULL, &itm, "mykey", 5, 0) == ENGINE_KEY_ENOENT, "Item should be gone");

    return SUCCESS;
}

static enum test_result test_gat(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char buffer[512];
    memset(buffer, 0, sizeof(buffer));
    protocol_binary_request_gat *req = reinterpret_cast<protocol_binary_request_gat *>(buffer);
    protocol_binary_request_header *request = reinterpret_cast<protocol_binary_request_header*>(req);

    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_GAT;
    req->message.header.request.extlen = 4;
    req->message.header.request.bodylen = htonl(4);
    req->message.body.expiration = ntohl(10);

    // key is a mandatory field!
    check(h1->unknown_command(h, NULL, request, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to call gat");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "Testing invalid arguments");

    // extlen is a mandatory field!
    req->message.header.request.extlen = 0;
    req->message.header.request.keylen = 4;
    check(h1->unknown_command(h, NULL, request, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to call gat");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "Testing invalid arguments");

    // Try to touch an unknown item...
    req->message.header.request.extlen = 4;
    req->message.header.request.keylen = htons(5);
    req->message.header.request.bodylen = htonl(4 + 5);
    memcpy(buffer + sizeof(req->bytes), "mykey", 5);

    check(h1->unknown_command(h, NULL, request, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to call gat");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, "Testing unknown key");

    // illegal vbucket
    req->message.header.request.vbucket = htons(5);
    check(h1->unknown_command(h, NULL, request, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to call touch");

    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, "Testing illegal vbucket");
    req->message.header.request.vbucket = 0;

    // Store the item!
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "mykey", "somevalue", &itm) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, itm);

    check(check_key_value(h, h1, "mykey", "somevalue", strlen("somevalue")) == SUCCESS,
          "Failed to retrieve data");

    check(h1->unknown_command(h, NULL, request, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to call gat");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "gat mykey");
    check(memcmp(last_body, "somevalue", sizeof("somevalue")) == 0,
          "Invalid data returned");

    // time-travel 9 secs..
    testHarness.time_travel(9);

    // The item should still exist
    check_key_value(h, h1, "mykey", "somevalue", 9);

    // time-travel 2 secs..
    testHarness.time_travel(2);

    // The item should have expired now...
    check(h1->get(h, NULL, &itm, "mykey", 5, 0) == ENGINE_KEY_ENOENT, "Item should be gone");
    return SUCCESS;
}

static enum test_result test_gatq(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char buffer[512];
    memset(buffer, 0, sizeof(buffer));
    protocol_binary_request_gat *req = reinterpret_cast<protocol_binary_request_gat *>(buffer);
    protocol_binary_request_header *request = reinterpret_cast<protocol_binary_request_header*>(req);

    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_GATQ;
    req->message.header.request.extlen = 4;
    req->message.header.request.bodylen = htonl(4);
    req->message.body.expiration = ntohl(10);

    // key is a mandatory field!
    check(h1->unknown_command(h, NULL, request, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to call gat");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "Testing invalid arguments");

    // extlen is a mandatory field!
    req->message.header.request.extlen = 0;
    req->message.header.request.keylen = 4;
    check(h1->unknown_command(h, NULL, request, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to call gat");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "Testing invalid arguments");

    // Try to gat an unknown item...
    req->message.header.request.extlen = 4;
    req->message.header.request.keylen = htons(5);
    req->message.header.request.bodylen = htonl(4 + 5);
    memcpy(buffer + sizeof(req->bytes), "mykey", 5);

    last_status = static_cast<protocol_binary_response_status>(0xffff);
    check(h1->unknown_command(h, NULL, request, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to call gat");

    // We should not have sent any response!
    check(last_status == 0xffff, "Testing unknown key");

    // illegal vbucket
    req->message.header.request.vbucket = htons(5);
    check(h1->unknown_command(h, NULL, request, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to call touch");
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, "Testing illegal vbucket");
    req->message.header.request.vbucket = 0;

    // Store the item!
    item *itm = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "mykey", "somevalue", &itm) == ENGINE_SUCCESS,
          "Failed set.");
    h1->release(h, NULL, itm);

    check(check_key_value(h, h1, "mykey", "somevalue", strlen("somevalue")) == SUCCESS,
          "Failed to retrieve data");

    check(h1->unknown_command(h, NULL, request, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to call gat");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS, "gat mykey");
    check(memcmp(last_body, "somevalue", sizeof("somevalue")) == 0,
          "Invalid data returned");
    // time-travel 9 secs..
    testHarness.time_travel(9);

    // The item should still exist
    check_key_value(h, h1, "mykey", "somevalue", 9);

    // time-travel 2 secs..
    testHarness.time_travel(2);

    // The item should have expired now...
    check(h1->get(h, NULL, &itm, "mykey", 5, 0) == ENGINE_KEY_ENOENT, "Item should be gone");
    return SUCCESS;
}

static enum test_result test_alloc_limit(ENGINE_HANDLE *h,
                                         ENGINE_HANDLE_V1 *h1) {
    item *it = NULL;
    ENGINE_ERROR_CODE rv;

    rv = h1->allocate(h, NULL, &it, "key", 3, 20 * 1024 * 1024, 0, 0, 0, 0);
    check(rv == ENGINE_SUCCESS, "Allocated 20MB item");
    h1->release(h, NULL, it);

    rv = h1->allocate(h, NULL, &it, "key", 3, (20 * 1024 * 1024) + 1, 0, 0, 0,0);
    check(rv == ENGINE_E2BIG, "Object too big");

    return SUCCESS;
}

static enum test_result test_whitespace_db(ENGINE_HANDLE *h,
                                           ENGINE_HANDLE_V1 *h1) {
    vals.clear();
    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    if (vals["ep_dbname"] != std::string(WHITESPACE_DB)) {
        std::cerr << "Expected dbname = ``" << WHITESPACE_DB << "''"
                  << ", got ``" << vals["ep_dbname"] << "''" << std::endl;
        return FAIL;
    }

    check(remove(WHITESPACE_DB) == 0,
          "Error removing whitespace remanant.");
    remove(WHITESPACE_DB "-0.sqlite");
    remove(WHITESPACE_DB "-1.sqlite");
    remove(WHITESPACE_DB "-2.sqlite");
    remove(WHITESPACE_DB "-3.sqlite");

    return SUCCESS;
}

static enum test_result test_db_shards(ENGINE_HANDLE *h,
                                           ENGINE_HANDLE_V1 *h1) {
    vals.clear();
    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_dbname") != vals.end(), "Found no db name");
    std::string db_name = vals["ep_dbname"];
    int dbShards = get_int_stat(h, h1, "ep_dbshards");
    check(dbShards == 5, "Expected five shards for db store");

    check(remove(db_name.c_str()) == 0,
          "Error removing db file.");
    for (int i = 0; i < dbShards; ++i) {
        std::stringstream shard_name;
        shard_name << db_name << "-" << i << ".sqlite";
        std::string s_name = shard_name.str();
        std::string error_msg("Error removing ");
        error_msg.append(s_name);
        check(remove(s_name.c_str()) == 0, error_msg.c_str());
    }

    return SUCCESS;
}

static enum test_result test_single_db_strategy(ENGINE_HANDLE *h,
                                                ENGINE_HANDLE_V1 *h1) {
    vals.clear();
    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_dbname") != vals.end(), "Found no db name");
    check(vals.find("ep_db_strategy") != vals.end(), "Found no db strategy");
    std::string db_strategy = vals["ep_db_strategy"];
    assert(strcmp(db_strategy.c_str(), "singleDB") == 0);

    wait_for_persisted_value(h, h1, "key", "somevalue");
    evict_key(h, h1, "key", 0, "Ejected.");
    check_key_value(h, h1, "key", "somevalue", 9);

    return SUCCESS;
}

static enum test_result test_memory_limit(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    int used = get_int_stat(h, h1, "mem_used");
    int max = static_cast<int>(get_int_stat(h, h1, "ep_max_data_size") * 0.9);
    check(get_int_stat(h, h1, "ep_oom_errors") == 0 &&
          get_int_stat(h, h1, "ep_tmp_oom_errors") == 0, "Expected no OOM errors.");
    assert(used < max);
    int overhead = get_int_stat(h, h1, "eviction_headroom") + 2 * 1024 * 1024 * 3;

    char data[8192];
    memset(data, 'x', sizeof(data));
    size_t vlen = max - used - 192 - overhead;
    data[vlen] = 0x00;

    item *i = NULL;
    // So if we add an item,
    check(store(h, h1, NULL, OPERATION_SET, "key", data, &i) == ENGINE_SUCCESS,
          "store failure");
    check_key_value(h, h1, "key", data, vlen);
    h1->release(h, NULL, i);

    // There should be no room for another.
    ENGINE_ERROR_CODE second = store(h, h1, NULL, OPERATION_SET, "key2", data, &i);
    check(second == ENGINE_ENOMEM || second == ENGINE_TMPFAIL,
          "should have failed second set");
    h1->release(h, NULL, i);
    check(get_int_stat(h, h1, "ep_oom_errors") == 1 ||
          get_int_stat(h, h1, "ep_tmp_oom_errors") == 1, "Expected an OOM error.");

    ENGINE_ERROR_CODE overwrite = store(h, h1, NULL, OPERATION_SET, "key", data, &i);
    check(overwrite == ENGINE_ENOMEM || overwrite == ENGINE_TMPFAIL,
          "should have failed second override");
    h1->release(h, NULL, i);
    check(get_int_stat(h, h1, "ep_oom_errors") == 2 ||
          get_int_stat(h, h1, "ep_tmp_oom_errors") == 2, "Expected another OOM error.");
    check_key_value(h, h1, "key", data, vlen);
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key2"), "Expected missing key");

    int itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    // Until we remove that item
    check(h1->remove(h, NULL, "key", 3, 0, 0) == ENGINE_SUCCESS,
          "Failed remove with value.");
    check(ENGINE_KEY_ENOENT == verify_key(h, h1, "key"), "Expected missing key");
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

    check(store(h, h1, NULL, OPERATION_SET, "key2", "somevalue2", &i) == ENGINE_SUCCESS,
          "should have succeded on the last set");
    check_key_value(h, h1, "key2", "somevalue2", 10);

    return SUCCESS;
}

static enum test_result test_vbucket_get_miss(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return verify_vbucket_missing(h, h1, 1) ? SUCCESS : FAIL;
}

static enum test_result test_vbucket_get(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return verify_vbucket_state(h, h1, 0, vbucket_state_active) ? SUCCESS : FAIL;
}

static enum test_result test_vbucket_create(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    if (!verify_vbucket_missing(h, h1, 1)) {
        fprintf(stderr, "vbucket wasn't missing.\n");
        return FAIL;
    }

    if (!set_vbucket_state(h, h1, 1, vbucket_state_active)) {
        fprintf(stderr, "set state failed.\n");
        return FAIL;
    }

    return verify_vbucket_state(h, h1, 1, vbucket_state_active) ? SUCCESS : FAIL;
}

static enum test_result test_vbucket_destroy(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_DEL_VBUCKET, 1);
    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to request delete bucket");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected failure deleting active bucket.");

    pkt = createPacket(PROTOCOL_BINARY_CMD_DEL_VBUCKET, 2);
    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to request delete bucket");
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,
          "Expected failure deleting non-existent bucket.");

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead), "Failed set set vbucket 1 state.");

    pkt = createPacket(PROTOCOL_BINARY_CMD_DEL_VBUCKET, 1);
    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to delete dead bucket.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected failure deleting non-existent bucket.");

    check(verify_vbucket_missing(h, h1, 1),
          "vbucket 0 was not missing after deleting it.");

    return SUCCESS;
}

static enum test_result test_vbucket_destroy_stats(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {

    int mem_used = get_int_stat(h, h1, "mem_used");
    int cacheSize = get_int_stat(h, h1, "ep_total_cache_size");
    int overhead = get_int_stat(h, h1, "ep_overhead");
    int nonResident = get_int_stat(h, h1, "ep_num_non_resident");

    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    std::vector<std::string> keys;
    for (int j = 0; j < 2000; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }

    int itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i, 0, 1)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }
    wait_for_flusher_to_settle(h, h1);
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead), "Failed set set vbucket 1 state.");

    int vbucketDel = get_int_stat(h, h1, "ep_vbucket_del");

    protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_DEL_VBUCKET, 1);
    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to delete dead bucket.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected failure deleting non-existent bucket.");

    check(verify_vbucket_missing(h, h1, 1),
          "vbucket 1 was not missing after deleting it.");

    wait_for_stat_change(h, h1, "ep_vbucket_del", vbucketDel);

    int mem_used2 = get_int_stat(h, h1, "mem_used");
    int cacheSize2 = get_int_stat(h, h1, "ep_total_cache_size");
    int overhead2 = get_int_stat(h, h1, "ep_overhead");
    int nonResident2 = get_int_stat(h, h1, "ep_num_non_resident");

    check(mem_used2 == mem_used, "memory should be the same");
    check(cacheSize2 == cacheSize, "cache size should be the same");
    check(overhead2 == overhead, "overhead should be the same");
    check(nonResident2 == nonResident, "non resident count should be the same");

    return SUCCESS;
}

static enum test_result test_vbucket_destroy_restart(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_DEL_VBUCKET, 1);
    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to request delete bucket");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected failure deleting active bucket.");

    // Store a value so the restart will try to resurrect it.
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key", "somevalue", &i, 0, 1)
          == ENGINE_SUCCESS, "Failed to set a value");
    check_key_value(h, h1, "key", "somevalue", 9, 1);

    // Reload to get a flush forced.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true, false);

    check(verify_vbucket_state(h, h1, 1, vbucket_state_active),
          "Bucket state was not active after restart.");

    check_key_value(h, h1, "key", "somevalue", 9, 1);

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead), "Failed set set vbucket 1 state.");

    pkt = createPacket(PROTOCOL_BINARY_CMD_DEL_VBUCKET, 1);
    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to delete dead bucket.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected failure deleting non-existent bucket.");

    check(verify_vbucket_missing(h, h1, 1),
          "vbucket 1 was not missing after deleting it.");

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true, false);

    if (verify_vbucket_state(h, h1, 1, vbucket_state_pending, true)) {
        std::cerr << "Bucket came up in pending state after delete." << std::endl;
        abort();
    }

    check(verify_vbucket_missing(h, h1, 1),
          "vbucket 1 was not missing after restart.");

    return SUCCESS;
}

static enum test_result test_vb_set_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_pending_vb_mutation(h, h1, OPERATION_SET);
}

static enum test_result test_vb_add_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_pending_vb_mutation(h, h1, OPERATION_ADD);
}

static enum test_result test_vb_cas_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_pending_vb_mutation(h, h1, OPERATION_CAS);
}

static enum test_result test_vb_append_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_pending_vb_mutation(h, h1, OPERATION_APPEND);
}

static enum test_result test_vb_prepend_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_pending_vb_mutation(h, h1, OPERATION_PREPEND);
}

static enum test_result test_vb_set_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_replica_vb_mutation(h, h1, OPERATION_SET);
}

static enum test_result test_vb_replace_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_replica_vb_mutation(h, h1, OPERATION_REPLACE);
}

static enum test_result test_vb_replace_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_pending_vb_mutation(h, h1, OPERATION_REPLACE);
}

static enum test_result test_vb_add_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_replica_vb_mutation(h, h1, OPERATION_ADD);
}

static enum test_result test_vb_cas_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_replica_vb_mutation(h, h1, OPERATION_CAS);
}

static enum test_result test_vb_append_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_replica_vb_mutation(h, h1, OPERATION_APPEND);
}

static enum test_result test_vb_prepend_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    return test_replica_vb_mutation(h, h1, OPERATION_PREPEND);
}

static enum test_result test_tap_rcvr_mutate(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char eng_specific[64];
    memset(eng_specific, 0, sizeof(eng_specific));
    const void *cookie = testHarness.create_cookie();
    check(h1->tap_notify(h, cookie, NULL, 0, 0, 0, TAP_CONSUMER, 0, "consumer", 8, 0, 0, 0, 0, NULL, 0, 0, 0) == ENGINE_SUCCESS,
            "Failed to register tap consumer");

    for (size_t i = 0; i < 8192; ++i) {
        char *data = static_cast<char *>(malloc(i));
        memset(data, 'x', i);
        check(h1->tap_notify(h, cookie, eng_specific, sizeof(eng_specific),
                             1, 0, TAP_MUTATION, 1, "key", 3, 828, std::numeric_limits<uint32_t>::max(), 0, 0,
                             data, i, 0, DI_CKSUM_DISABLED_STR) == ENGINE_SUCCESS,
              "Failed tap notify.");
        std::stringstream ss;
        ss << "failed key at " << i;
        check(check_key_value(h, h1, "key", data, i) == SUCCESS,
              ss.str().c_str());
        free(data);
    }
    return SUCCESS;
}

static enum test_result test_tap_rcvr_checkpoint(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char eng_specific[64];
    memset(eng_specific, 0, sizeof(eng_specific));
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    const void *cookie = testHarness.create_cookie();
    check(h1->tap_notify(h, cookie, NULL, 0, 0, 0, TAP_CONSUMER, 0, "consumer", 8, 0, 0, 0, 0, NULL, 0, 0, 0) == ENGINE_SUCCESS,
            "Failed to register tap consumer");

    for (size_t i = 1; i < 10; ++i) {
        std::stringstream ss;
        ss << i;
        check(h1->tap_notify(h, cookie, eng_specific, sizeof(eng_specific),
                             1, 0, TAP_CHECKPOINT_START, 1, "", 0, 828, 0, 0, 0,
                             ss.str().c_str(), ss.str().length(), 1, DI_CKSUM_DISABLED_STR) == ENGINE_SUCCESS,
              "Failed tap notify.");
        check(h1->tap_notify(h, cookie, eng_specific, sizeof(eng_specific),
                             1, 0, TAP_CHECKPOINT_END, 1, "", 0, 828, 0, 0, 0,
                             ss.str().c_str(), ss.str().length(), 1, DI_CKSUM_DISABLED_STR) == ENGINE_SUCCESS,
              "Failed tap notify.");
    }
    return SUCCESS;
}

static enum test_result test_tap_rcvr_mutate_dead(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char eng_specific[1];
    const void *cookie = testHarness.create_cookie();
    check(h1->tap_notify(h, cookie, NULL, 0, 0, 0, TAP_CONSUMER, 0, "consumer", 8, 0, 0, 0, 0, NULL, 0, 0, 0) == ENGINE_SUCCESS,
            "Failed to register tap consumer");

    check(h1->tap_notify(h, cookie, eng_specific, 1,
                         1, 0, TAP_MUTATION, 1, "key", 3, 828, std::numeric_limits<uint32_t>::max(), 0, 0,
                         "data", 4, 1, DI_CKSUM_DISABLED_STR) == ENGINE_NOT_MY_VBUCKET,
          "Expected not my vbucket.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_mutate_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending), "Failed to set vbucket state.");
    char eng_specific[1];
    const void *cookie = testHarness.create_cookie();
    check(h1->tap_notify(h, cookie, NULL, 0, 0, 0, TAP_CONSUMER, 0, "consumer", 8, 0, 0, 0, 0, NULL, 0, 0, 0) == ENGINE_SUCCESS,
            "Failed to register tap consumer");

    check(h1->tap_notify(h, cookie, eng_specific, 1,
                         1, 0, TAP_MUTATION, 1, "key", 3, 828, std::numeric_limits<uint32_t>::max(), 0, 0,
                         "data", 4, 1, DI_CKSUM_DISABLED_STR) == ENGINE_SUCCESS,
          "Expected expected success.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_mutate_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    char eng_specific[1];
    const void *cookie = testHarness.create_cookie();
    check(h1->tap_notify(h, cookie, NULL, 0, 0, 0, TAP_CONSUMER, 0, "consumer", 8, 0, 0, 0, 0, NULL, 0, 0, 0) == ENGINE_SUCCESS,
            "Failed to register tap consumer");

    check(h1->tap_notify(h, cookie, eng_specific, 1,
                         1, 0, TAP_MUTATION, 1, "key", 3, 828, std::numeric_limits<uint32_t>::max(), 0, 0,
                         "data", 4, 1, DI_CKSUM_DISABLED_STR) == ENGINE_SUCCESS,
          "Expected expected success.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_delete(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    check(h1->tap_notify(h, cookie, NULL, 0, 0, 0, TAP_CONSUMER, 0, "consumer", 8, 0, 0, 0, 0, NULL, 0, 0, 0) == ENGINE_SUCCESS,
            "Failed to register tap consumer");

    check(h1->tap_notify(h, cookie, NULL, 0,
                         1, 0, TAP_DELETION, 0, "key", 3, 0, std::numeric_limits<uint32_t>::max(), 0, 0,
                         0, 0, 0, DI_CKSUM_DISABLED_STR) == ENGINE_SUCCESS,
          "Failed tap notify.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_delete_dead(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const void *cookie = testHarness.create_cookie();
    check(h1->tap_notify(h, cookie, NULL, 0, 0, 0, TAP_CONSUMER, 0, "consumer", 8, 0, 0, 0, 0, NULL, 0, 0, 0) == ENGINE_SUCCESS,
            "Failed to register tap consumer");

    check(h1->tap_notify(h, cookie, NULL, 0,
                         1, 0, TAP_DELETION, 1, "key", 3, 0, std::numeric_limits<uint32_t>::max(), 0, 0,
                         NULL, 0, 1, DI_CKSUM_DISABLED_STR) == ENGINE_NOT_MY_VBUCKET,
          "Expected not my vbucket.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_delete_pending(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_pending), "Failed to set vbucket state.");
    const void *cookie = testHarness.create_cookie();
    check(h1->tap_notify(h, cookie, NULL, 0, 0, 0, TAP_CONSUMER, 0, "consumer", 8, 0, 0, 0, 0, NULL, 0, 0, 0) == ENGINE_SUCCESS,
            "Failed to register tap consumer");

    check(h1->tap_notify(h, cookie, NULL, 0,
                         1, 0, TAP_DELETION, 1, "key", 3, 0, std::numeric_limits<uint32_t>::max(), 0, 0,
                         NULL, 0, 1, DI_CKSUM_DISABLED_STR) == ENGINE_SUCCESS,
          "Expected expected success.");
    return SUCCESS;
}

static enum test_result test_tap_rcvr_delete_replica(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    const void *cookie = testHarness.create_cookie();
    check(h1->tap_notify(h, cookie, NULL, 0, 0, 0, TAP_CONSUMER, 0, "consumer", 8, 0, 0, 0, 0, NULL, 0, 0, 0) == ENGINE_SUCCESS,
            "Failed to register tap consumer");

    check(h1->tap_notify(h, cookie, NULL, 0,
                         1, 0, TAP_DELETION, 1, "key", 3, 0, std::numeric_limits<uint32_t>::max(), 0, 0,
                         NULL, 0, 1, DI_CKSUM_DISABLED_STR) == ENGINE_SUCCESS,
          "Expected expected success.");
    return SUCCESS;
}

static enum test_result verify_item(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                    item *i,
                                    const char* key, size_t klen,
                                    const char* val, size_t vlen)
{
    item_info info;
    info.nvalue = 1;
    check(h1->get_item_info(h, NULL, i, &info), "get item info failed");
    check(info.nvalue == 1, "iovectors not supported");
    // We can pass in a NULL key to avoid the key check (for tap streams)
    if (key) {
        check(klen == info.nkey, "Incorrect key length");
        check(memcmp(info.key, key, klen) == 0, "Incorrect key value");
    }
    check(vlen == info.value[0].iov_len, "Incorrect value length");
    check(memcmp(info.value[0].iov_base, val, vlen) == 0,
          "Data mismatch");

    return SUCCESS;
}

static const void* createTapConn(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                                 const char *name) {
    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name,
                                             strlen(name),
                                             TAP_CONNECT_FLAG_DUMP, NULL,
                                             0);
    check(iter != NULL, "Failed to create a tap iterator");
    return cookie;
}

static enum test_result test_tap_agg_stats(ENGINE_HANDLE *h,
                                           ENGINE_HANDLE_V1 *h1) {
    std::vector<const void*> cookies;

    cookies.push_back(createTapConn(h, h1, "replica_a"));
    cookies.push_back(createTapConn(h, h1, "replica_b"));
    cookies.push_back(createTapConn(h, h1, "replica_c"));
    cookies.push_back(createTapConn(h, h1, "rebalance_a"));
    cookies.push_back(createTapConn(h, h1, "userconnn"));

    check(get_int_stat(h, h1, "replica:count", "tapagg _") == 3,
          "Incorrect replica count on tap agg");
    check(get_int_stat(h, h1, "rebalance:count", "tapagg _") == 1,
          "Incorrect rebalance count on tap agg");
    check(get_int_stat(h, h1, "_total:count", "tapagg _") == 5,
          "Incorrect total count on tap agg");

    std::for_each(cookies.begin(), cookies.end(),
                  std::ptr_fun((UNLOCK_COOKIE_T)testHarness.unlock_cookie));

    return SUCCESS;
}

static enum test_result test_tap_stream(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const int num_keys = 30;
    bool keys[num_keys];
    int initialPersisted = get_int_stat(h, h1, "ep_total_persisted");

    for (int ii = 0; ii < num_keys; ++ii) {
        keys[ii] = false;
        std::stringstream ss;
        ss << ii;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "value", NULL, 0, 0) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }

    useconds_t sleepTime = 128;
    while (get_int_stat(h, h1, "ep_total_persisted")
           < initialPersisted + num_keys) {
        decayingSleep(&sleepTime);
    }

    for (int ii = 0; ii < num_keys; ++ii) {
        std::stringstream ss;
        ss << ii;
        evict_key(h, h1, ss.str().c_str(), 0, "Ejected.");
    }

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    std::string name = "tap_client_thread";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_DUMP, NULL,
                                             0);
    check(iter != NULL, "Failed to create a tap iterator");

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    tap_event_t event;
    std::string key;

    uint16_t unlikely_vbucket_identifier = 17293;

    do {
        vbucket = unlikely_vbucket_identifier;
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        switch (event) {
        case TAP_PAUSE:
            testHarness.waitfor_cookie(cookie);
            break;
        case TAP_OPAQUE:
        case TAP_NOOP:
            break;
        case TAP_MUTATION:
            testHarness.unlock_cookie(cookie);
            check(get_key(h, h1, it, key), "Failed to read out the key");
            keys[atoi(key.c_str())] = true;
            assert(vbucket != unlikely_vbucket_identifier);
            check(verify_item(h, h1, it, NULL, 0, "value", 5) == SUCCESS,
                  "Unexpected item arrived on tap stream");
            h1->release(h, cookie, it);
            testHarness.lock_cookie(cookie);
            break;
        case TAP_DISCONNECT:
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }

    } while (event != TAP_DISCONNECT);

    for (int ii = 0; ii < num_keys; ++ii) {
        check(keys[ii], "Failed to receive key");
    }

    testHarness.unlock_cookie(cookie);
    check(get_int_stat(h, h1, "ep_tap_total_fetched", "tap") != 0,
          "http://bugs.northscale.com/show_bug.cgi?id=1695");
    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_tap_total_fetched", "tap") == 0,
          "Expected reset stats to clear ep_tap_total_fetched");

    return SUCCESS;
}

static enum test_result test_tap_takeover(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const int num_keys = 30;
    bool keys[num_keys];
    int initialPersisted = get_int_stat(h, h1, "ep_total_persisted");

    int initializedKeys = 0;

    memset(keys, 0, sizeof(keys));

    for (; initializedKeys < num_keys / 2; ++initializedKeys) {
        keys[initializedKeys] = false;
        std::stringstream ss;
        ss << initializedKeys;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "value", NULL, 0, 0) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }

    useconds_t sleepTime = 128;
    while (get_int_stat(h, h1, "ep_total_persisted")
           < initialPersisted + initializedKeys) {
        decayingSleep(&sleepTime);
    }

    for (int ii = 0; ii < initializedKeys; ++ii) {
        std::stringstream ss;
        ss << ii;
        evict_key(h, h1, ss.str().c_str(), 0, "Ejected.");
    }

    uint16_t vbucketfilter[2];
    vbucketfilter[0] = ntohs(1);
    vbucketfilter[1] = ntohs(0);

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    std::string name = "tap_client_thread";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_TAKEOVER_VBUCKETS |
                                             TAP_CONNECT_FLAG_LIST_VBUCKETS,
                                             static_cast<void*>(vbucketfilter),
                                             4);
    check(iter != NULL, "Failed to create a tap iterator");

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    tap_event_t event;
    std::string key;

    uint16_t unlikely_vbucket_identifier = 17293;
    bool allows_more_mutations(true);

    do {
        vbucket = unlikely_vbucket_identifier;
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        if (initializedKeys < num_keys) {
            keys[initializedKeys] = false;
            std::stringstream ss;
            ss << initializedKeys;
            check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                        "value", NULL, 0, 0) == ENGINE_SUCCESS,
                  "Failed to store an item.");
            ++initializedKeys;
        }

        switch (event) {
        case TAP_PAUSE:
            testHarness.waitfor_cookie(cookie);
            break;
        case TAP_NOOP:
            break;
        case TAP_MUTATION:
            // This will be false if we've seen a vbucket set state.
            assert(allows_more_mutations);

            check(get_key(h, h1, it, key), "Failed to read out the key");
            keys[atoi(key.c_str())] = true;
            assert(vbucket != unlikely_vbucket_identifier);
            check(verify_item(h, h1, it, NULL, 0, "value", 5) == SUCCESS,
                  "Unexpected item arrived on tap stream");
            h1->release(h, cookie, it);

            break;
        case TAP_DISCONNECT:
            break;
        case TAP_VBUCKET_SET:
            assert(nengine_specific == 4);
            vbucket_state_t state;
            memcpy(&state, engine_specific, nengine_specific);
            state = static_cast<vbucket_state_t>(ntohl(state));
            if (state == vbucket_state_active) {
                allows_more_mutations = false;
            }
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }

    } while (event != TAP_DISCONNECT);

    for (int ii = 0; ii < num_keys; ++ii) {
        check(keys[ii], "Failed to receive key");
    }

    testHarness.unlock_cookie(cookie);
    check(get_int_stat(h, h1, "ep_tap_total_fetched", "tap") != 0,
          "http://bugs.northscale.com/show_bug.cgi?id=1695");
    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_tap_total_fetched", "tap") == 0,
          "Expected reset stats to clear ep_tap_total_fetched");

    return SUCCESS;
}

static enum test_result test_tap_filter_stream(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    for (uint16_t vbid = 0; vbid < 4; ++vbid) {
        check(set_vbucket_state(h, h1, vbid, vbucket_state_active),
              "Failed to set vbucket state.");
    }

    const int num_keys = 40;
    bool keys[num_keys];
    for (int ii = 0; ii < num_keys; ++ii) {
        keys[ii] = false;
        std::stringstream ss;
        ss << ii;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "value", NULL, 0, ii % 4) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    uint16_t vbucketfilter[4];
    vbucketfilter[0] = htons(2);
    vbucketfilter[1] = htons(0);
    vbucketfilter[2] = htons(2);

    std::string name = "tap_client_thread";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_LIST_VBUCKETS,
                                             static_cast<void*>(vbucketfilter),
                                             6);
    check(iter != NULL, "Failed to create a tap iterator");

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;

    tap_event_t event;
    int found = 0;

    uint16_t unlikely_vbucket_identifier = 17293;
    std::string key;
    bool done = false;

    uint16_t vbid;

    do {
        vbucket = unlikely_vbucket_identifier;
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        switch (event) {
        case TAP_PAUSE:
            done = true;
            for (int ii = 0; ii < num_keys; ++ii) {
                if ((ii % 4) != 1 && !keys[ii]) {
                    done = false;
                    break;
                }
            }
            if (!done) {
                testHarness.waitfor_cookie(cookie);
            }
            break;
        case TAP_NOOP:
        case TAP_OPAQUE:
            break;

        case TAP_MUTATION:
            check(get_key(h, h1, it, key), "Failed to read out the key");
            vbid = atoi(key.c_str()) % 4;
            check(vbid == vbucket, "Incorrect vbucket id");
            check(vbid != 1,
                  "Received an item for a vbucket we don't subscribe to");
            keys[atoi(key.c_str())] = true;
            ++found;
            assert(vbucket != unlikely_vbucket_identifier);
            check(verify_item(h, h1, it, NULL, 0, "value", 5) == SUCCESS,
                  "Unexpected item arrived on tap stream");

            // We've got some of the elements.. Let's change the filter
            // and get the rest
            if (found == 10) {
                vbucketfilter[0] = htons(3);
                vbucketfilter[3] = htons(3);
                testHarness.unlock_cookie(cookie);
                iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                            name.length(),
                                            TAP_CONNECT_FLAG_LIST_VBUCKETS,
                                            static_cast<void*>(vbucketfilter),
                                            8);
                check(iter != NULL, "Failed to create a tap iterator");
                testHarness.lock_cookie(cookie);

            }

            break;
        case TAP_DISCONNECT:
            done = true;
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }
    } while (!done);

    testHarness.unlock_cookie(cookie);
    h1->release(h, cookie, it);

    check(get_int_stat(h, h1, "eq_tapq:tap_client_thread:qlen", "tap") == 0,
          "queue should be empty");

    return SUCCESS;
}

static enum test_result test_tap_config(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(h1->get_stats(h, NULL, "tap", 3, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_tap_backoff_period") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_interval") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_window_size") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_grace_period") != vals.end(), "Missing stat");
    std::string s = vals["ep_tap_backoff_period"];
    check(strcmp(s.c_str(), "0.05") == 0, "Incorrect backoff value");
    s = vals["ep_tap_ack_interval"];
    check(strcmp(s.c_str(), "10") == 0, "Incorrect interval value");
    s = vals["ep_tap_ack_window_size"];
    check(strcmp(s.c_str(), "2") == 0, "Incorrect window size value");
    s = vals["ep_tap_ack_grace_period"];
    check(strcmp(s.c_str(), "10") == 0, "Incorrect grace period value");
    return SUCCESS;
}

static enum test_result test_tap_default_config(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(h1->get_stats(h, NULL, "tap", 3, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_tap_backoff_period") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_interval") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_window_size") != vals.end(), "Missing stat");
    check(vals.find("ep_tap_ack_grace_period") != vals.end(), "Missing stat");

    std::string s = vals["ep_tap_backoff_period"];
    check(strcmp(s.c_str(), "5") == 0, "Incorrect backoff value");
    s = vals["ep_tap_ack_interval"];
    check(strcmp(s.c_str(), "1000") == 0, "Incorrect interval value");
    s = vals["ep_tap_ack_window_size"];
    check(strcmp(s.c_str(), "10") == 0, "Incorrect window size value");
    s = vals["ep_tap_ack_grace_period"];
    check(strcmp(s.c_str(), "300") == 0, "Incorrect grace period value");

    return SUCCESS;
}

static enum test_result test_tap_ack_stream(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    const int nkeys = 10;
    bool receivedKeys[nkeys];
    bool nackKeys[nkeys];

    for (int i = 0; i < nkeys; ++i) {
        nackKeys[i] = true;
        receivedKeys[i] = false;
        std::stringstream ss;
        ss << i;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "value", NULL, 0, 0) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }

    for (int i = 0; i < nkeys; ++i) {
        std::stringstream ss;
        ss << i;
        item_info info;
        check(get_value(h, h1, ss.str().c_str(), &info), "Verify items");
    }

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    uint16_t vbucketfilter[2];
    vbucketfilter[0] = htons(1);
    vbucketfilter[1] = htons(0);

    std::string name = "tap_ack";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_LIST_VBUCKETS |
                                             TAP_CONNECT_CHECKPOINT |
                                             TAP_CONNECT_SUPPORT_ACK |
                                             TAP_CONNECT_FLAG_DUMP,
                                             static_cast<void*>(vbucketfilter),
                                             4);
    check(iter != NULL, "Failed to create a tap iterator");

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    tap_event_t event;
    std::string key;
    bool done = false;
    int index;
    int numRollbacks = 1000;

    do {
        if (numRollbacks > 0) {
            if (random() % 4 == 0) {
                iter = NULL;
            }
        }

        if (iter == NULL) {
            testHarness.unlock_cookie(cookie);
            iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                        name.length(),
                                        TAP_CONNECT_FLAG_LIST_VBUCKETS |
                                        TAP_CONNECT_CHECKPOINT |
                                        TAP_CONNECT_SUPPORT_ACK |
                                        TAP_CONNECT_FLAG_DUMP,
                                        static_cast<void*>(vbucketfilter),
                                        4);
            check(iter != NULL, "Failed to create a tap iterator");
            testHarness.lock_cookie(cookie);
        }

        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        switch (event) {
        case TAP_PAUSE:
            testHarness.waitfor_cookie(cookie);
            break;
        case TAP_OPAQUE:
            if (numRollbacks > 0) {
                if (random() % 4 == 0) {
                    iter = NULL;
                    continue;
                }
            }
            testHarness.unlock_cookie(cookie);
            h1->tap_notify(h, cookie, NULL, 0, 0,
                           PROTOCOL_BINARY_RESPONSE_SUCCESS,
                           TAP_ACK, seqno, NULL, 0, 0,
                           0, 0, 0, NULL, 0, 0 , DI_CKSUM_DISABLED_STR);
            testHarness.lock_cookie(cookie);
            break;
        case TAP_NOOP:
            break;
        case TAP_MUTATION:
            check(get_key(h, h1, it, key), "Failed to read out key");
            index = atoi(key.c_str());
            check(index >= 0 && index <= nkeys, "Illegal key returned");

            testHarness.unlock_cookie(cookie);
            if (nackKeys[index]) {
                nackKeys[index] = false;
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
                               TAP_ACK, seqno, key.c_str(), key.length(),
                               0, 0, 0, 0, NULL, 0, 0 , DI_CKSUM_DISABLED_STR);
            } else {
                receivedKeys[index] = true;
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS,
                               TAP_ACK, seqno, key.c_str(), key.length(),
                               0, 0, 0, 0, NULL, 0, 0 , DI_CKSUM_DISABLED_STR);
            }
            testHarness.lock_cookie(cookie);
            h1->release(h, cookie, it);

            break;
        case TAP_CHECKPOINT_START:
            testHarness.unlock_cookie(cookie);
            h1->tap_notify(h, cookie, NULL, 0, 0,
                           PROTOCOL_BINARY_RESPONSE_SUCCESS,
                           TAP_ACK, seqno, key.c_str(), key.length(),
                           0, 0, 0, 0, NULL, 0, 0 , DI_CKSUM_DISABLED_STR);
            testHarness.lock_cookie(cookie);
            h1->release(h, cookie, it);
            break;
        case TAP_CHECKPOINT_END:
            testHarness.unlock_cookie(cookie);
            h1->tap_notify(h, cookie, NULL, 0, 0,
                           PROTOCOL_BINARY_RESPONSE_SUCCESS,
                           TAP_ACK, seqno, key.c_str(), key.length(),
                           0, 0, 0, 0, NULL, 0, 0 , DI_CKSUM_DISABLED_STR);
            testHarness.lock_cookie(cookie);
            h1->release(h, cookie, it);
            break;
        case TAP_DISCONNECT:
            done = true;
            break;
        default:
            std::cerr << "Unexpected event:  " << event << std::endl;
            return FAIL;
        }
    } while (!done);

    for (int ii = 0; ii < nkeys; ++ii) {
        check(receivedKeys[ii], "Did not receive all of the keys");
    }

    testHarness.unlock_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_tap_implicit_ack_stream(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    const int nkeys = 10;
    for (int i = 0; i < nkeys; ++i) {
        std::stringstream ss;
        ss << i;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(),
                    "value", NULL, 0, 0) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }

    for (int i = 0; i < nkeys; ++i) {
        std::stringstream ss;
        ss << i;
        item_info info;
        check(get_value(h, h1, ss.str().c_str(), &info), "Verify items");
    }

    const void *cookie = testHarness.create_cookie();
    testHarness.lock_cookie(cookie);
    uint16_t vbucketfilter[2];
    vbucketfilter[0] = htons(1);
    vbucketfilter[1] = htons(0);

    std::string name = "tap_ack";
    TAP_ITERATOR iter = h1->get_tap_iterator(h, cookie, name.c_str(),
                                             name.length(),
                                             TAP_CONNECT_FLAG_LIST_VBUCKETS |
                                             TAP_CONNECT_CHECKPOINT |
                                             TAP_CONNECT_SUPPORT_ACK |
                                             TAP_CONNECT_FLAG_DUMP,
                                             static_cast<void*>(vbucketfilter),
                                             4);
    check(iter != NULL, "Failed to create a tap iterator");

    item *it;
    void *engine_specific;
    uint16_t nengine_specific;
    uint8_t ttl;
    uint16_t flags;
    uint32_t seqno;
    uint16_t vbucket;
    tap_event_t event;
    std::string key;
    bool done = false;
    int mutations = 0;
    do {
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        if (event == TAP_PAUSE) {
            testHarness.waitfor_cookie(cookie);
        } else {
            if (event == TAP_MUTATION) {
                ++mutations;
            }
            if (seqno == static_cast<uint32_t>(4294967294UL)) {
                testHarness.unlock_cookie(cookie);
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS,
                               TAP_ACK, seqno, NULL, 0,
                               0, 0, 0, 0, NULL, 0, 0 , DI_CKSUM_DISABLED_STR);
                testHarness.lock_cookie(cookie);
            } else if (flags == TAP_FLAG_ACK) {
                testHarness.unlock_cookie(cookie);
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS,
                               TAP_ACK, seqno, NULL, 0,
                               0, 0, 0, 0, NULL, 0, 0 , DI_CKSUM_DISABLED_STR);
                testHarness.lock_cookie(cookie);
            }
        }
    } while (seqno < static_cast<uint32_t>(4294967295UL));

    do {
        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);

        if (event == TAP_PAUSE) {
            testHarness.waitfor_cookie(cookie);
        } else {
            if (event == TAP_MUTATION) {
                ++mutations;
            }
            if (seqno == 1) {
                testHarness.unlock_cookie(cookie);
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
                               TAP_ACK, seqno, key.c_str(), key.length(),
                               0, 0, 0, 0, NULL, 0, 0 , DI_CKSUM_DISABLED_STR);
                testHarness.lock_cookie(cookie);
            } else if (flags == TAP_FLAG_ACK) {
                testHarness.unlock_cookie(cookie);
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS,
                               TAP_ACK, seqno, NULL, 0,
                               0, 0, 0, 0, NULL, 0, 0 , DI_CKSUM_DISABLED_STR);
                testHarness.lock_cookie(cookie);
            }
        }
    } while (seqno < 1);

    /* Now just get the rest */
    do {

        event = iter(h, cookie, &it, &engine_specific,
                     &nengine_specific, &ttl, &flags,
                     &seqno, &vbucket);
        if (event == TAP_PAUSE) {
            testHarness.waitfor_cookie(cookie);
        } else {
            if (event == TAP_MUTATION) {
                ++mutations;
            } else if (event == TAP_DISCONNECT) {
                done = true;
            }
            if (flags == TAP_FLAG_ACK) {
                testHarness.unlock_cookie(cookie);
                h1->tap_notify(h, cookie, NULL, 0, 0,
                               PROTOCOL_BINARY_RESPONSE_SUCCESS,
                               TAP_ACK, seqno, NULL, 0,
                               0, 0, 0, 0, NULL, 0, 0 , DI_CKSUM_DISABLED_STR);
                testHarness.lock_cookie(cookie);
            }
        }
    } while (!done);
    testHarness.unlock_cookie(cookie);
    check(mutations == 11, "Expected 11 mutations to be returned");
    return SUCCESS;
}

static enum test_result test_set_tap_param(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    set_tap_param(h, h1, "tap_keepalive", "600");
    check(get_int_stat(h, h1, "ep_tap_keepalive") == 600,
          "Incorrect tap_keepalive value.");
    set_tap_param(h, h1, "tap_keepalive", "5000");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected an invalid value error due to exceeding a max value allowed");
    return SUCCESS;
}

static enum test_result test_tap_noop_config_default(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_tap_noop_interval", "tap") == 200,
          "Expected tap_noop_interval == 10");
    return SUCCESS;
}

static enum test_result test_tap_noop_config(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_tap_noop_interval", "tap") == 10,
          "Expected tap_noop_interval == 10");
    return SUCCESS;
}

static enum test_result test_tap_noop_config_deprecated(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_tap_noop_interval", "tap") == 10,
          "Expected tap_noop_interval == 10");
    return SUCCESS;
}

static enum test_result test_tap_notify(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    int ii = 0;
    char buffer[1024];
    ENGINE_ERROR_CODE r;

    const void *cookie = testHarness.create_cookie();
    check(h1->tap_notify(h, cookie, NULL, 0, 0, 0, TAP_CONSUMER, 0, "consumer", 8, 0, 0, 0, 0, NULL, 0, 0, 0) == ENGINE_SUCCESS,
            "Failed to register tap consumer");
    do {
        std::stringstream ss;
        ss << "Key-"<< ++ii;
        std::string key = ss.str();

        r = h1->tap_notify(h, cookie, NULL, 0, 1, 0, TAP_MUTATION, 0,
                           key.c_str(), key.length(), 0, std::numeric_limits<uint32_t>::max(), 0, 0, buffer, 1024, 0, DI_CKSUM_DISABLED_STR);
    } while (r == ENGINE_SUCCESS);
    check(r == ENGINE_DISCONNECT, "should disconnect non-acking streams");

    uint32_t auto_nack = htonl(TAP_OPAQUE_ENABLE_AUTO_NACK);
    r = h1->tap_notify(h, cookie, &auto_nack, sizeof(auto_nack), 1, 0,
                       TAP_OPAQUE, 0, NULL, 0, 0, 0, 0, 0, NULL, 0, 0 , DI_CKSUM_DISABLED_STR);
    check(r == ENGINE_SUCCESS, "Enable auto nack'ing");

    do {
        std::stringstream ss;
        ss << "Key-"<< ++ii;
        std::string key = ss.str();

        r = h1->tap_notify(h, cookie, NULL, 0, 1, 0, TAP_MUTATION, 0,
                           key.c_str(), key.length(), 0, std::numeric_limits<uint32_t>::max(), 0, 0, buffer, 1024, 0, DI_CKSUM_DISABLED_STR);
    } while (r == ENGINE_SUCCESS);
    check(r == ENGINE_TMPFAIL, "non-acking streams should etmpfail");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_novb0(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(verify_vbucket_missing(h, h1, 0), "vb0 existed and shouldn't have.");
    return SUCCESS;
}

static enum test_result test_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    vals.clear();
    check(h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.size() > 10, "Kind of expected more stats than that.");
    check(vals.find("ep_version") != vals.end(), "Found no ep_version.");

    return SUCCESS;
}

static enum test_result test_mem_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char value[2048];
    memset(value, 'b', sizeof(value));
    strcpy(value + sizeof(value) - 4, "\r\n");
    int itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    wait_for_persisted_value(h, h1, "key", value);
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);
    int mem_used = get_int_stat(h, h1, "mem_used");
    int cache_size = get_int_stat(h, h1, "ep_total_cache_size");
    int overhead = get_int_stat(h, h1, "ep_overhead");
    int value_size = get_int_stat(h, h1, "ep_value_size");
    check((mem_used - overhead) > cache_size,
          "ep_kv_size should be greater than the hashtable cache size due to the checkpoint overhead");
    evict_key(h, h1, "key", 0, "Ejected.");

    check(get_int_stat(h, h1, "ep_total_cache_size") == cache_size,
          "Evict a value shouldn't increase the total cache size");
    check(get_int_stat(h, h1, "mem_used") < mem_used,
          "Expected mem_used to decrease when an item is evicted");

    check_key_value(h, h1, "key", value, strlen(value), 0); // Load an item from disk again.

    check(get_int_stat(h, h1, "mem_used") == mem_used,
          "Expected mem_used to remain the same after an item is loaded from disk");
    check(get_int_stat(h, h1, "ep_value_size") == value_size,
          "Expected ep_value_size to remain the same after item is loaded from disk");

    return SUCCESS;
}

static enum test_result test_io_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_io_num_read") == 0,
          "Expected reset stats to set io_num_read to zero");
    check(get_int_stat(h, h1, "ep_io_num_write") == 0,
          "Expected reset stats to set io_num_write to zero");
    check(get_int_stat(h, h1, "ep_io_read_bytes") == 0,
          "Expected reset stats to set io_read_bytes to zero");
    check(get_int_stat(h, h1, "ep_io_write_bytes") == 0,
          "Expected reset stats to set io_write_bytes to zero");
    wait_for_persisted_value(h, h1, "a", "b\r\n");
    check(get_int_stat(h, h1, "ep_io_num_read") == 0 &&
          get_int_stat(h, h1, "ep_io_read_bytes") == 0,
          "Expected storing one value to not change the read counter");

    check(get_int_stat(h, h1, "ep_io_num_write") == 1 &&
          get_int_stat(h, h1, "ep_io_write_bytes") == 4,
          "Expected storing the key to update the write counter");
    evict_key(h, h1, "a", 0, "Ejected.");

    check_key_value(h, h1, "a", "b\r\n", 3, 0);

    check(get_int_stat(h, h1, "ep_io_num_read") == 1 &&
          get_int_stat(h, h1, "ep_io_read_bytes") == 4,
          "Expected reading the value back in to update the read counter");
    check(get_int_stat(h, h1, "ep_io_num_write") == 1 &&
          get_int_stat(h, h1, "ep_io_write_bytes") == 4,
          "Expected reading the value back in to not update the write counter");

    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_io_num_read") == 0,
          "Expected reset stats to set io_num_read to zero");
    check(get_int_stat(h, h1, "ep_io_num_write") == 0,
          "Expected reset stats to set io_num_write to zero");
    check(get_int_stat(h, h1, "ep_io_read_bytes") == 0,
          "Expected reset stats to set io_read_bytes to zero");
    check(get_int_stat(h, h1, "ep_io_write_bytes") == 0,
          "Expected reset stats to set io_write_bytes to zero");


    return SUCCESS;
}

static enum test_result test_bg_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    h1->reset_stats(h, NULL);
    wait_for_persisted_value(h, h1, "a", "b\r\n");
    evict_key(h, h1, "a", 0, "Ejected.");
    check_key_value(h, h1, "a", "b\r\n", 3, 0);

    check(get_int_stat(h, h1, "ep_bg_num_samples") == 1,
          "Expected one sample");

    check(vals.find("ep_bg_min_wait") != vals.end(), "Found no ep_bg_min_wait.");
    check(vals.find("ep_bg_max_wait") != vals.end(), "Found no ep_bg_max_wait.");
    check(vals.find("ep_bg_wait_avg") != vals.end(), "Found no ep_bg_wait_avg.");
    check(vals.find("ep_bg_min_load") != vals.end(), "Found no ep_bg_min_load.");
    check(vals.find("ep_bg_max_load") != vals.end(), "Found no ep_bg_max_load.");
    check(vals.find("ep_bg_load_avg") != vals.end(), "Found no ep_bg_load_avg.");

    evict_key(h, h1, "a", 0, "Ejected.");
    check_key_value(h, h1, "a", "b\r\n", 3, 0);
    check(get_int_stat(h, h1, "ep_bg_num_samples") == 2,
          "Expected one sample");

    return SUCCESS;
}

static enum test_result test_key_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;

    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed set vbucket 1 state.");

    // set (k1,v1) in vbucket 0
    check(store(h, h1, NULL, OPERATION_SET,"k1", "v1", &i, 0, 0) == ENGINE_SUCCESS,
          "Failed to store an item.");
    // set (k2,v2) in vbucket 1
    check(store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i, 0, 1) == ENGINE_SUCCESS,
          "Failed to store an item.");

    const void *cookie = testHarness.create_cookie();

    // stat for key "k1" and vbucket "0"
    const char *statkey1 = "key k1 0";
    vals.clear();
    check(h1->get_stats(h, cookie, statkey1, strlen(statkey1), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_dirtied") != vals.end(), "Found no key_dirtied");
    check(vals.find("key_data_age") != vals.end(), "Found no key_data_age");
    check(vals.find("key_last_modification_time") != vals.end(),
                    "Found no key_last_modification_time");
    check(vals.find("key_resident") != vals.end(), "Found no key_resident");

    // stat for key "k2" and vbucket "1"
    const char *statkey2 = "key k2 1";
    vals.clear();
    check(h1->get_stats(h, cookie, statkey2, strlen(statkey2), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_dirtied") != vals.end(), "Found no key_dirtied");
    check(vals.find("key_data_age") != vals.end(), "Found no key_data_age");
    check(vals.find("key_last_modification_time") != vals.end(),
                    "Found no key_last_modification_time");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_vkey_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed set vbucket 1 state.");

    wait_for_persisted_value(h, h1, "k1", "v1");
    wait_for_persisted_value(h, h1, "k2", "v2", 1);

    const void *cookie = testHarness.create_cookie();

    // stat for key "k1" and vbucket "0"
    const char *statkey1 = "vkey k1 0";
    check(h1->get_stats(h, cookie, statkey1, strlen(statkey1), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_dirtied") != vals.end(), "Found no key_dirtied");
    check(vals.find("key_data_age") != vals.end(), "Found no key_data_age");
    check(vals.find("key_last_modification_time") != vals.end(),
                    "Found no key_last_modification_time");
    check(vals.find("key_valid") != vals.end(), "Found no key_valid");

    // stat for key "k2" and vbucket "1"
    const char *statkey2 = "vkey k2 1";
    check(h1->get_stats(h, cookie, statkey2, strlen(statkey2), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("key_is_dirty") != vals.end(), "Found no key_is_dirty");
    check(vals.find("key_exptime") != vals.end(), "Found no key_exptime");
    check(vals.find("key_flags") != vals.end(), "Found no key_flags");
    check(vals.find("key_cas") != vals.end(), "Found no key_cas");
    check(vals.find("key_dirtied") != vals.end(), "Found no key_dirtied");
    check(vals.find("key_data_age") != vals.end(), "Found no key_data_age");
    check(vals.find("key_last_modification_time") != vals.end(),
                    "Found no key_last_modification_time");
    check(vals.find("key_valid") != vals.end(), "Found no key_valid");

    testHarness.destroy_cookie(cookie);
    return SUCCESS;
}

static enum test_result test_warmup_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *it = NULL;
    check(set_vbucket_state(h, h1, 0, vbucket_state_active), "Failed to set VB0 state.");
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set VB1 state.");

    for (int i = 0; i < 5000; ++i) {
        std::stringstream key;
        key << "key-" << i;
        check(ENGINE_SUCCESS ==
              store(h, h1, NULL, OPERATION_SET, key.str().c_str(), "somevalue", &it),
              "Error setting.");
    }

    // Restart the server.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true, false);

    useconds_t sleepTime = 128;
    while (h1->get_stats(h, NULL, NULL, 0, add_stats) == ENGINE_SUCCESS) {
        std::string s = vals["ep_warmup_thread"];
        if (strcmp(s.c_str(), "complete") == 0) {
            break;
        }
        decayingSleep(&sleepTime);
        vals.clear();
    }

    check(vals.find("ep_warmup_thread") != vals.end(), "Found no ep_warmup_thread");
    check(vals.find("ep_warmed_up") != vals.end(), "Found no ep_warmed_up");
    check(vals.find("ep_warmup_dups") != vals.end(), "Found no ep_warmup_dups");
    check(vals.find("ep_warmup_oom") != vals.end(), "Found no ep_warmup_oom");
    check(vals.find("ep_warmup_time") != vals.end(), "Found no ep_warmup_time");
    std::string warmup_time = vals["ep_warmup_time"];
    assert(atoi(warmup_time.c_str()) > 0);

    vals.clear();
    check(h1->get_stats(h, NULL, "prev-vbucket", 12, add_stats) == ENGINE_SUCCESS,
          "Failed to get the previous state of vbuckets");
    check(vals.find("vb_0") != vals.end(), "Found no previous state for VB0");
    check(vals.find("vb_1") != vals.end(), "Found no previous state for VB1");
    std::string vb0_prev_state = vals["vb_0"];
    std::string vb1_prev_state = vals["vb_1"];
    assert(strncmp(vb0_prev_state.c_str(), "active", 6) == 0);
    assert(strncmp(vb1_prev_state.c_str(), "replica", 7) == 0);

    return SUCCESS;
}

static enum test_result test_curr_items(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;

    // Verify initial case.
    verify_curr_items(h, h1, 0, "init");

    // Verify set and add case
    check(store(h, h1, NULL, OPERATION_ADD,"k1", "v1", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    check(store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    check(store(h, h1, NULL, OPERATION_SET,"k3", "v3", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    verify_curr_items(h, h1, 3, "three items stored");
    assert(3 == get_int_stat(h, h1, "ep_total_enqueued"));

    wait_for_flusher_to_settle(h, h1);

    // Verify delete case.
    check(h1->remove(h, NULL, "k1", 2, 0, 0) == ENGINE_SUCCESS,
          "Failed remove with value.");
    verify_curr_items(h, h1, 3, "one item deleted - not persisted");

    wait_for_stat_change(h, h1, "curr_items", 3);
    verify_curr_items(h, h1, 2, "one item deleted - persisted");

    set_flush_param(h, h1, "enable_flushall", "true");
    // Verify flush case (remove the two remaining from above)
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS,
          "Failed to flush");
    verify_curr_items(h, h1, 0, "flush");

    // Verify dead vbucket case.
    check(store(h, h1, NULL, OPERATION_SET,"k1", "v1", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    check(store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    check(store(h, h1, NULL, OPERATION_SET,"k3", "v3", &i) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    check(set_vbucket_state(h, h1, 0, vbucket_state_dead), "Failed set vbucket 0 state.");

    verify_curr_items(h, h1, 0, "dead vbucket");
    check(get_int_stat(h, h1, "curr_items_tot") == 0,
          "Expected curr_items_tot to be 0 with a dead vbucket");

    // Then resurrect.
    check(set_vbucket_state(h, h1, 0, vbucket_state_active), "Failed set vbucket 0 state.");

    verify_curr_items(h, h1, 3, "resurrected vbucket");

    // Now completely delete it.
    check(set_vbucket_state(h, h1, 0, vbucket_state_dead), "Failed set vbucket 0 state.");
    protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_DEL_VBUCKET, 0);
    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to delete dead bucket.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Expected success deleting vbucket.");
    verify_curr_items(h, h1, 0, "del vbucket");
    check(get_int_stat(h, h1, "curr_items_tot") == 0,
          "Expected curr_items_tot to be 0 after deleting a vbucket");

    return SUCCESS;
}


static enum test_result test_value_eviction(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    item *i = NULL;
    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_num_value_ejects") == 0,
          "Expected reset stats to set ep_num_value_ejects to zero");
    check(get_int_stat(h, h1, "ep_num_non_resident") == 0,
          "Expected all items to be resident");
    check(get_int_stat(h, h1, "ep_num_active_non_resident") == 0,
          "Expected all active vbucket items to be resident");

    check(store(h, h1, NULL, OPERATION_SET,"k1", "v1", &i, 0, 0) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    evict_key(h, h1, "k1", 0, "Can't eject: Dirty object.");
    check(store(h, h1, NULL, OPERATION_SET,"k2", "v2", &i, 0, 1) == ENGINE_SUCCESS,
          "Failed to fail to store an item.");
    evict_key(h, h1, "k2", 1, "Can't eject: Dirty object.");

    wait_for_flusher_to_settle(h, h1);
    evict_key(h, h1, "k1", 0, "Ejected.");
    evict_key(h, h1, "k2", 1, "Ejected.");

    check(get_int_stat(h, h1, "ep_num_non_resident") == 2,
          "Expected two non-resident items");
    check(get_int_stat(h, h1, "ep_num_active_non_resident") == 2,
          "Expected two non-resident items for active vbuckets");

    evict_key(h, h1, "k1", 0, "Already ejected.");
    evict_key(h, h1, "k2", 1, "Already ejected.");

    protocol_binary_request_header *pkt = create_packet(CMD_EVICT_KEY,
                                                        "missing-key", "");
    pkt->request.vbucket = htons(0);

    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to evict key.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
          "expected the key to be missing...");

    check(get_int_stat(h, h1, "ep_num_value_ejects") == 2,
          "Expected only two items to be ejected");

    h1->reset_stats(h, NULL);
    check(get_int_stat(h, h1, "ep_num_value_ejects") == 0,
          "Expected reset stats to set ep_num_value_ejects to zero");

    check_key_value(h, h1, "k1", "v1", 2);
    check(get_int_stat(h, h1, "ep_num_non_resident") == 1,
          "Expected only one item to be non-resident");
    check(get_int_stat(h, h1, "ep_num_active_non_resident") == 1,
          "Expected only one active vbucket item to be non-resident");

    check(set_vbucket_state(h, h1, 0, vbucket_state_replica), "Failed to set vbucket state.");
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    check(get_int_stat(h, h1, "ep_num_non_resident") == 1,
          "Expected only one item to be non-resident");
    check(get_int_stat(h, h1, "ep_num_active_non_resident") == 0,
          "Expected no non-resident items");

    return SUCCESS;
}

static enum test_result test_mb3169(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i = NULL;
    uint64_t cas(0);
    uint64_t result(0);
    check(store(h, h1, NULL, OPERATION_SET, "set", "value", &i, 0, 0)
          == ENGINE_SUCCESS, "Failed to store a value");
    check(store(h, h1, NULL, OPERATION_SET, "incr", "0", &i, 0, 0)
          == ENGINE_SUCCESS, "Failed to store a value");
    check(store(h, h1, NULL, OPERATION_SET, "delete", "0", &i, 0, 0)
          == ENGINE_SUCCESS, "Failed to store a value");
    check(store(h, h1, NULL, OPERATION_SET, "get", "getvalue", &i, 0, 0)
          == ENGINE_SUCCESS, "Failed to store a value");

    wait_for_flusher_to_settle(h, h1);

    evict_key(h, h1, "set", 0, "Ejected.");
    evict_key(h, h1, "incr", 0, "Ejected.");
    evict_key(h, h1, "delete", 0, "Ejected.");
    evict_key(h, h1, "get", 0, "Ejected.");

    check(get_int_stat(h, h1, "ep_num_non_resident") == 4,
          "Expected four items to be resident");

    check(store(h, h1, NULL, OPERATION_SET, "set", "value2", &i, 0, 0)
          == ENGINE_SUCCESS, "Failed to store a value");

    check(get_int_stat(h, h1, "ep_num_non_resident") == 3,
          "Expected mutation to mark item resident");

    check(h1->arithmetic(h, NULL, "incr", 4, true, false, 1, 1, 0,
                         &cas, &result,
                         0)  == ENGINE_SUCCESS, "Incr failed");

    check(get_int_stat(h, h1, "ep_num_non_resident") == 2,
          "Expected incr to mark item resident");

    check(h1->remove(h, NULL, "delete", 6, 0, 0) == ENGINE_SUCCESS,
          "Delete failed");

    check(get_int_stat(h, h1, "ep_num_non_resident") == 1,
          "Expected delete to remove non-resident item");

    check_key_value(h, h1, "get", "getvalue", 8);

    check(get_int_stat(h, h1, "ep_num_non_resident") == 0,
          "Expected all items to be resident");
    return SUCCESS;
}

static enum test_result test_queuedtime_tap(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    BlockTimerSimple tt;
    char eng_specific[64];
    int currTime = get_int_stat(h, h1, "ep_real_time");
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    memset(eng_specific, 0, sizeof(eng_specific));
    const void *cookie = testHarness.create_cookie();
    check(h1->tap_notify(h, cookie, NULL, 0, 0, 0, TAP_CONSUMER, 0, "consumer", 8, 0, 0, 0, 0, NULL, 0, 0, 0) == ENGINE_SUCCESS,
            "Failed to register tap consumer");

    for (int i = 0; i < 12; i++) {
        check(h1->tap_notify(h, cookie, eng_specific, sizeof(eng_specific), 1, 0, TAP_MUTATION, 1,
                             "key", 3, 828, currTime, 0, 0,
                             "value", 5, 0, DI_CKSUM_DISABLED_STR) == ENGINE_SUCCESS,
              "Failed tap notify.");
        sleep(1);
    }
    sleep(1);
    wait_for_flusher_to_settle(h, h1);
    // Give a gap of atleast 4 seconds because that's the resolution in stored value
    check(tt.getElapsedTime() >= 18, "Either min_data_age or queued_time not honoured");
    return SUCCESS;
}

static enum test_result test_min_data_age(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    BlockTimerSimple tt;
    for (int i = 0; i < 5; i++) {
        check(store(h, h1, NULL, OPERATION_SET, "key", "value", NULL) == ENGINE_SUCCESS,
              "Failed set.");
        sleep(1);
    }
    wait_for_flusher_to_settle(h, h1);
    check(get_int_stat(h, h1, "ep_total_persisted") == 1, "Persistence must have occured exactly once");
    // Give a gap of atleast 4 seconds because that's the resolution in stored value
    check(tt.getElapsedTime() > 15, "min_data_age not honoured");
    return SUCCESS;
}

static enum test_result test_duplicate_items_disk(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    std::vector<std::string> keys;
    for (int j = 0; j < 2000; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
            item *i;
            check(store(h, h1, NULL, OPERATION_SET, it->c_str(), "value", &i, 0, 1)
                  == ENGINE_SUCCESS, "Failed to store a value");
    }
    wait_for_flusher_to_settle(h, h1);

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead), "Failed set set vbucket 1 state.");
    int vb_del_num = get_int_stat(h, h1, "ep_vbucket_del");
    protocol_binary_request_header *pkt = createPacket(PROTOCOL_BINARY_CMD_DEL_VBUCKET, 1);
    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "Failed to delete dead bucket.");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failure deleting dead bucket.");
    check(verify_vbucket_missing(h, h1, 1),
          "vbucket 1 was not missing after deleting it.");

    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    for (it = keys.begin(); it != keys.end(); ++it) {
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i, 0, 1)
              == ENGINE_SUCCESS, "Failed to store a value");
    }
    wait_for_flusher_to_settle(h, h1);
    wait_for_stat_change(h, h1, "ep_vbucket_del", vb_del_num);

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true, false);
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");
    // Make sure that a key/value item is persisted correctly
    for (it = keys.begin(); it != keys.end(); ++it) {
        evict_key(h, h1, it->c_str(), 1, "Ejected.");
    }
    for (it = keys.begin(); it != keys.end(); ++it) {
        check_key_value(h, h1, it->c_str(), it->data(), it->size(), 1);
    }
    check(get_int_stat(h, h1, "ep_warmup_dups") == 0,
          "Expected no duplicate items from disk");

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_golden(ENGINE_HANDLE *h,
                                                ENGINE_HANDLE_V1 *h1) {
    // Check/grab initial state.
    int overhead = get_int_stat(h, h1, "ep_overhead");
    check(get_int_stat(h, h1, "ep_kv_size") > 0,
          "Initial kv_size should be greater than 0 due to the CHECKPOINT_START meta item.");

    int itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    // Store some data and check post-set state.
    wait_for_persisted_value(h, h1, "k1", "some value");
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

    assert(0 == get_int_stat(h, h1, "ep_bg_fetched"));
    assert(1 == get_int_stat(h, h1, "ep_total_enqueued"));
    int kv_size = get_int_stat(h, h1, "ep_kv_size");
    int mem_used = get_int_stat(h, h1, "mem_used");
    check(get_int_stat(h, h1, "ep_overhead") >= overhead,
          "Fell below initial overhead.");

    // Evict the data.
    evict_key(h, h1, "k1");

    int kv_size2 = get_int_stat(h, h1, "ep_kv_size");
    int mem_used2 = get_int_stat(h, h1, "mem_used");
    check(get_int_stat(h, h1, "ep_overhead") >= overhead,
          "Fell below initial overhead.");

    assert(kv_size2 < kv_size);
    assert(mem_used2 < mem_used);

    // Reload the data.
    check_key_value(h, h1, "k1", "some value", 10);

    int kv_size3 = get_int_stat(h, h1, "ep_kv_size");
    int mem_used3 = get_int_stat(h, h1, "mem_used");
    check(get_int_stat(h, h1, "ep_overhead") >= overhead,
          "Fell below initial overhead.");

    assert(1 == get_int_stat(h, h1, "ep_bg_fetched"));
    // Should not have marked the thing dirty.
    assert(1 == get_int_stat(h, h1, "ep_total_enqueued"));

    assert(kv_size == kv_size3);
    assert(mem_used == mem_used3);

    itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    // Delete the value and make sure things return correctly.
    int numStored = get_int_stat(h, h1, "ep_total_persisted");
    check(h1->remove(h, NULL, "k1", 2, 0, 0) == ENGINE_SUCCESS,
          "Failed remove with value.");
    wait_for_stat_change(h, h1, "ep_total_persisted", numStored);
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

    check(get_int_stat(h, h1, "ep_kv_size") > 0,
          "kv_size should be still greater than 0 due to the CHECKPOINT_START meta item.");
    check(get_int_stat(h, h1, "ep_overhead") == overhead,
          "Fell below initial overhead.");
    check(get_int_stat(h, h1, "mem_used") > overhead,
          "mem_used (ep_kv_size + ep_overhead) should be greater than ep_overhead");

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_paged_rm(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {
    // Check/grab initial state.
    int overhead = get_int_stat(h, h1, "ep_overhead");
    check(get_int_stat(h, h1, "ep_kv_size") > 0,
          "Initial kv_size should be greater than 0 due to the CHECKPOINT_START meta item.");

    // Store some data and check post-set state.
    wait_for_persisted_value(h, h1, "k1", "some value");
    assert(0 == get_int_stat(h, h1, "ep_bg_fetched"));
    assert(1 == get_int_stat(h, h1, "ep_total_enqueued"));
    check(get_int_stat(h, h1, "ep_overhead") >= overhead,
          "Fell below initial overhead.");

    // Evict the data.
    evict_key(h, h1, "k1");

    // Delete the value and make sure things return correctly.
    int itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");
    int numStored = get_int_stat(h, h1, "ep_total_persisted");
    check(h1->remove(h, NULL, "k1", 2, 0, 0) == ENGINE_SUCCESS,
          "Failed remove with value.");
    wait_for_stat_change(h, h1, "ep_total_persisted", numStored);
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);

    check(get_int_stat(h, h1, "ep_kv_size") > 0,
          "kv_size should be still greater than 0 due to the CHECKPOINT_START meta item.");
    check(get_int_stat(h, h1, "ep_overhead") == overhead,
          "Fell below initial overhead.");
    check(get_int_stat(h, h1, "mem_used") > overhead,
          "mem_used (ep_kv_size + ep_overhead) should be greater than ep_overhead");

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_incr(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {
    uint64_t cas = 0, result = 0;
    wait_for_persisted_value(h, h1, "k1", "13");

    evict_key(h, h1, "k1");

    check(h1->arithmetic(h, NULL, "k1", 2, true, false, 1, 1, 0,
                         &cas, &result,
                         0) == ENGINE_SUCCESS,
          "Failed to incr value.");

    check_key_value(h, h1, "k1", "14", 2);

    assert(1 == get_int_stat(h, h1, "ep_bg_fetched"));

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_update_paged_out(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    wait_for_persisted_value(h, h1, "k1", "some value");

    evict_key(h, h1, "k1");

    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "k1", "new value", &i) == ENGINE_SUCCESS,
          "Failed to update an item.");

    check_key_value(h, h1, "k1", "new value", 9);

    assert(0 == get_int_stat(h, h1, "ep_bg_fetched"));

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_delete_paged_out(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    wait_for_persisted_value(h, h1, "k1", "some value");

    evict_key(h, h1, "k1");

    check(h1->remove(h, NULL, "k1", 2, 0, 0) == ENGINE_SUCCESS,
          "Failed to delete.");

    check(verify_key(h, h1, "k1") == ENGINE_KEY_ENOENT, "Expected miss.");

    assert(0 == get_int_stat(h, h1, "ep_bg_fetched"));

    return SUCCESS;
}

extern "C" {
    static void* bg_set_thread(void *arg) {
        ThreadData *td(static_cast<ThreadData*>(arg));

        usleep(2600); // Exacerbate race condition.

        item *i = NULL;
        check(store(td->h, td->h1, NULL, OPERATION_SET,
                    "k1", "new value", &i) == ENGINE_SUCCESS,
              "Failed to update an item.");

        delete td;
        return NULL;
    }

    static void* bg_del_thread(void *arg) {
        ThreadData *td(static_cast<ThreadData*>(arg));

        usleep(2600); // Exacerbate race condition.

        check(td->h1->remove(td->h, NULL, "k1", 2, 0, 0) == ENGINE_SUCCESS,
              "Failed to delete.");

        delete td;
        return NULL;
    }

    static void* bg_incr_thread(void *arg) {
        ThreadData *td(static_cast<ThreadData*>(arg));

        usleep(2600); // Exacerbate race condition.

        uint64_t cas = 0, result = 0;
        check(td->h1->arithmetic(td->h, NULL, "k1", 2, true, false, 1, 1, 0,
                                 &cas, &result,
                                 0) == ENGINE_SUCCESS,
              "Failed to incr value.");

        delete td;
        return NULL;
    }

}

static enum test_result test_disk_gt_ram_set_race(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {
    wait_for_persisted_value(h, h1, "k1", "some value");

    set_flush_param(h, h1, "bg_fetch_delay", "3");

    evict_key(h, h1, "k1");

    pthread_t tid;
    if (pthread_create(&tid, NULL, bg_set_thread, new ThreadData(h, h1)) != 0) {
        abort();
    }

    check_key_value(h, h1, "k1", "new value", 9);

    // Should have bg_fetched, but discarded the old value.
    assert(1 == get_int_stat(h, h1, "ep_bg_fetched"));

    assert(pthread_join(tid, NULL) == 0);

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_incr_race(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    wait_for_persisted_value(h, h1, "k1", "13");
    assert(1 == get_int_stat(h, h1, "ep_total_enqueued"));

    set_flush_param(h, h1, "bg_fetch_delay", "3");

    evict_key(h, h1, "k1");

    pthread_t tid;
    if (pthread_create(&tid, NULL, bg_incr_thread, new ThreadData(h, h1)) != 0) {
        abort();
    }

    // Value is as it was before.
    check_key_value(h, h1, "k1", "13", 2);
    // Should have bg_fetched to retrieve it even with a concurrent
    // incr.  We *may* at this point have also completed the incr.
    // 1 == get only, 2 == get+incr.
    assert(get_int_stat(h, h1, "ep_bg_fetched") >= 1);

    // Give incr time to finish (it's doing another background fetch)
    wait_for_stat_change(h, h1, "ep_bg_fetched", 1);
    wait_for_stat_change(h, h1, "ep_total_enqueued", 1);

    // The incr mutated the value.
    check_key_value(h, h1, "k1", "14", 2);

    assert(pthread_join(tid, NULL) == 0);

    return SUCCESS;
}

static enum test_result test_disk_gt_ram_rm_race(ENGINE_HANDLE *h,
                                                 ENGINE_HANDLE_V1 *h1) {
    wait_for_persisted_value(h, h1, "k1", "some value");

    set_flush_param(h, h1, "bg_fetch_delay", "3");

    evict_key(h, h1, "k1");

    pthread_t tid;
    if (pthread_create(&tid, NULL, bg_del_thread, new ThreadData(h, h1)) != 0) {
        abort();
    }

    check(verify_key(h, h1, "k1") == ENGINE_KEY_ENOENT, "Expected miss.");

    // Should have bg_fetched, but discarded the old value.
    assert(1 == get_int_stat(h, h1, "ep_bg_fetched"));

    assert(pthread_join(tid, NULL) == 0);

    return SUCCESS;
}

static enum test_result test_multi_dispatcher_conf(ENGINE_HANDLE *h,
                                                   ENGINE_HANDLE_V1 *h1) {
    vals.clear();
    check(h1->get_stats(h, NULL, "dispatcher", strlen("dispatcher"),
                        add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    if (vals.find("ro_dispatcher0:status") == vals.end()) {
        std::cerr << "Expected ro_dispatcher to be running." << std::endl;
        return FAIL;
    }
    return SUCCESS;
}

static enum test_result test_not_multi_dispatcher_conf(ENGINE_HANDLE *h,
                                                       ENGINE_HANDLE_V1 *h1) {
    vals.clear();
    check(h1->get_stats(h, NULL, "dispatcher", strlen("dispatcher"),
                        add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    if (vals.find("ro_dispatcher0:status") != vals.end()) {
        std::cerr << "Expected ro_dispatcher to not be running." << std::endl;
        return FAIL;
    }
    return SUCCESS;
}

static bool epsilon(int val, int target, int ep=5) {
    return abs(val - target) < ep;
}

static enum test_result test_max_size_settings(ENGINE_HANDLE *h,
                                               ENGINE_HANDLE_V1 *h1) {
    check(get_int_stat(h, h1, "ep_max_data_size") == 1000, "Incorrect initial size.");
    check(epsilon(get_int_stat(h, h1, "ep_mem_low_wat"), 600),
          "Incorrect initial low wat.");
    check(epsilon(get_int_stat(h, h1, "ep_mem_high_wat"), 750),
          "Incorrect initial high wat.");

    set_flush_param(h, h1, "max_size", "1000000");

    check(get_int_stat(h, h1, "ep_max_data_size") == 1000000,
          "Incorrect new size.");
    check(epsilon(get_int_stat(h, h1, "ep_mem_low_wat"), 600000),
          "Incorrect larger low wat.");
    check(epsilon(get_int_stat(h, h1, "ep_mem_high_wat"), 750000),
          "Incorrect larger high wat.");

    set_flush_param(h, h1, "mem_low_wat", "700000");
    set_flush_param(h, h1, "mem_high_wat", "800000");

    check(get_int_stat(h, h1, "ep_mem_low_wat") == 700000,
          "Incorrect even larger low wat.");
    check(get_int_stat(h, h1, "ep_mem_high_wat") == 800000,
          "Incorrect even larger high wat.");

    set_flush_param(h, h1, "max_size", "100");

    check(get_int_stat(h, h1, "ep_max_data_size") == 100,
          "Incorrect smaller size.");
    check(epsilon(get_int_stat(h, h1, "ep_mem_low_wat"), 60),
          "Incorrect smaller low wat.");
    check(epsilon(get_int_stat(h, h1, "ep_mem_high_wat"), 75),
          "Incorrect smaller high wat.");

    set_flush_param(h, h1, "mem_low_wat", "50");
    set_flush_param(h, h1, "mem_high_wat", "70");

    check(get_int_stat(h, h1, "ep_mem_low_wat") == 50,
          "Incorrect even smaller low wat.");
    check(get_int_stat(h, h1, "ep_mem_high_wat") == 70,
          "Incorrect even smaller high wat.");

    return SUCCESS;
}

static enum test_result test_validate_engine_handle(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    (void)h;
    check(h1->get_stats_struct == NULL, "get_stats_struct member should be initialized to NULL");
    check(h1->aggregate_stats == NULL, "aggregate_stats member should be initialized to NULL");
    check(h1->unknown_command != NULL, "unknown_command member should be initialized to a non-NULL value");
    check(h1->tap_notify != NULL, "tap_notify member should be initialized to a non-NULL value");
    check(h1->get_tap_iterator != NULL, "get_tap_iterator member should be initialized to a non-NULL value");
    check(h1->errinfo == NULL, "errinfo member should be initialized to NULL");

    return SUCCESS;
}

static enum test_result test_kill9_bucket(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    std::vector<std::string> keys;
    for (int j = 0; j < 2000; ++j) {
        std::stringstream ss;
        ss << "key-0-" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }
    std::vector<std::string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i, 0, 0)
              == ENGINE_SUCCESS, "Failed to store a value");
    }

    // Last parameter indicates the force shutdown for the engine.
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              testHarness.default_engine_cfg,
                              true, true);

    keys.clear();
    for (int j = 0; j < 2000; ++j) {
        std::stringstream ss;
        ss << "key-1-" << j;
        std::string key(ss.str());
        keys.push_back(key);
    }
    for (it = keys.begin(); it != keys.end(); ++it) {
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, it->c_str(), it->c_str(), &i, 0, 0)
              == ENGINE_SUCCESS, "Failed to store a value");
    }
    for (it = keys.begin(); it != keys.end(); ++it) {
        check_key_value(h, h1, it->c_str(), it->data(), it->size(), 0);
    }

    return SUCCESS;
}


static protocol_binary_request_header*
create_sync_packet(uint32_t flags, uint16_t nkeys, const key_spec_t keyspecs[]) {
    std::stringstream body;

    uint32_t options = htonl(flags);
    body.write((char *) &options, sizeof(uint32_t));

    uint16_t keyCount = htons(nkeys);
    body.write((char *) &keyCount, sizeof(uint16_t));

    for (uint16_t i = 0; i < nkeys; i++) {
        std::string key = keyspecs[i].key;
        uint64_t cas = htonll(keyspecs[i].cas);
        uint16_t vbucketid = htons(keyspecs[i].vbucketid);
        uint16_t keylen = htons(key.length());

        body.write((char *) &cas, sizeof(uint64_t));
        body.write((char *) &vbucketid, sizeof(uint16_t));
        body.write((char *) &keylen, sizeof(uint16_t));
        body.write(key.c_str(), key.length());
    }

    char *pkt = (char *)
        calloc(1, sizeof(protocol_binary_request_header) + body.str().length());
    protocol_binary_request_header *req = (protocol_binary_request_header *) pkt;
    req->request.opcode = CMD_SYNC;
    req->request.bodylen = htonl(body.str().length());
    memcpy(pkt + sizeof(protocol_binary_request_header),
           body.str().c_str(), body.str().length());

    return req;
}

static std::list< std::pair<key_spec_t, uint8_t> > parse_sync_response(char *response) {
    std::list< std::pair<key_spec_t, uint8_t> > resp;
    uint16_t nkeys;
    size_t offset = 0;

    memcpy(&nkeys, response + offset, sizeof(uint16_t));
    nkeys = ntohs(nkeys);
    offset += sizeof(uint16_t);

    for (int i = 0; i < nkeys; i++) {
        uint64_t cas;
        uint16_t vbid;
        uint16_t keylen;
        uint8_t eventid;

        memcpy(&cas, response + offset, sizeof(uint64_t));
        cas = ntohll(cas);
        offset += sizeof(uint64_t);
        memcpy(&vbid, response + offset, sizeof(uint16_t));
        vbid = ntohs(vbid);
        offset += sizeof(uint16_t);
        memcpy(&keylen, response + offset, sizeof(uint16_t));
        keylen = ntohs(keylen);
        offset += sizeof(uint16_t);
        memcpy(&eventid, response + offset, sizeof(uint8_t));
        offset += sizeof(uint8_t);

        std::string key(response + offset, keylen);
        key_spec_t keyspec(cas, vbid, key);
        offset += keylen;

        std::pair<key_spec_t, uint8_t> p(keyspec, eventid);
        resp.push_back(p);
    }

    return resp;
}

static enum test_result test_sync_bad_flags(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const key_spec_t keyspecs[] = { key_spec_t(0, 0, "key1"), key_spec_t(0, 0, "key2") };
    const uint16_t nkeys = 2;
    protocol_binary_request_header *pkt;

    // persistence and mutation bits both set
    pkt = create_sync_packet(0x0000000c, nkeys, keyspecs);

    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS, "sync fail");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "sync fail");

    free(pkt);

    // no flags set
    pkt = create_sync_packet(0x00000000, nkeys, keyspecs);

    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS, "sync fail");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "sync fail");

    free(pkt);

    // 3 replicas plus mutation flag set
    pkt = create_sync_packet(0x00000034, nkeys, keyspecs);

    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS, "sync fail");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL, "sync fail");

    free(pkt);

    // 3 replicas -> replica count > 1 not supported for chain mode replication, which is
    //               the default in Membase deployments (ticket MB-3817)
    pkt = create_sync_packet((uint32_t) ((3 & 0x0f) << 4), nkeys, keyspecs);
    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "sync replica count > 1");
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, "sync replica count > 1");

    pkt = create_sync_packet((uint32_t) ((2 & 0x0f) << 4), nkeys, keyspecs);
    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "sync replica count > 1");
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, "sync replica count > 1");

    return SUCCESS;
}

/**
 * SYNC for persistence not yet supported (MB-3817 related).
 */
/*
static enum test_result test_sync_persistence(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const key_spec_t keyspecs[] = {
        key_spec_t(0, 0, "key1"), key_spec_t(0, 0, "key2"), key_spec_t(0, 0, "key3"),
        key_spec_t(0, 0, "key4"), key_spec_t(0, 0, "key5"), key_spec_t(666, 0, "key6"),
        key_spec_t(0, 0, "key7"), key_spec_t(0, 0, "key8"), key_spec_t(0, 0, "NonExistentKey")
    };
    const uint16_t nkeys = 9;
    pthread_t threads[nkeys];
    protocol_binary_request_header *pkt = create_sync_packet(0x00000008, nkeys, keyspecs);
    std::vector<set_key_thread_params*> params;

    set_flush_param(h, h1, "sync_cmd_timeout", "10000");

    for (int i = 0; i < (nkeys - 1); i++) {
        set_key_thread_params *p = (set_key_thread_params *) malloc(sizeof(set_key_thread_params));
        p->h = h;
        p->h1 = h1;
        p->keyspec = &keyspecs[i];
        p->value = "qwerty";
        p->iterations = 10;
        p->wait = 0;
        params.push_back(p);
    }

    for (int i = 0; i < (nkeys - 1); i++) {
        int r = pthread_create(&threads[i], NULL, conc_set_key_thread, params[i]);
        assert(r == 0);
    }

    for (int i = 0; i < (nkeys - 1); i++) {
        void *trv = NULL;
        int r = pthread_join(threads[i], &trv);
        assert(r == 0);
    }

    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "SYNC on persistence command success");

    // verify the response sent to the client is correct
    std::list< std::pair<key_spec_t, uint8_t> > resp = parse_sync_response(last_body);

    check(resp.size() == nkeys, "response has the same # of keys");

    std::set<key_spec_t> keyset;
    for (int i = 0; i < nkeys; i++) {
        keyset.insert(keyspecs[i]);
    }

    std::list< std::pair<key_spec_t, uint8_t> >::iterator itresp = resp.begin();

    for ( ; itresp != resp.end(); itresp++) {
        key_spec_t keyspec = itresp->first;
        uint8_t eventid = itresp->second;

        check(keyspec.vbucketid == 0, "right vbucket id");
        check(keyset.find(keyspec) != keyset.end(), "key sent in the request");

        if (keyspec.key == "key6") {
            check(keyspec.cas == 666, "right cas");
            check(eventid == SYNC_INVALID_CAS,
                  "right event id (SYNC_INVALID_CAS)");
        } else if (keyspec.key == "NonExistentKey") {
            check(keyspec.cas == 0, "right cas");
            check(eventid == SYNC_INVALID_KEY,
                  "right event id (SYNC_INVALID_KEY)");
        } else {
            check(eventid == SYNC_PERSISTED_EVENT,
                  "right event id (SYNC_PERSISTED_EVENT)");
        }
    }

    std::vector<set_key_thread_params*>::iterator it = params.begin();
    for ( ; it != params.end(); it++) {
        free(*it);
    }

    free(pkt);

    // test that sending a request with only invalid keys and/or mismatching key CAS'es
    // doesn't block the client forever
    const key_spec_t keyspecs2[] = {
        key_spec_t(0, 0, "fookey"), key_spec_t(999, 0, "key1")
    };
    const uint16_t nkeys2 = 2;

    pkt = create_sync_packet(0x00000008, nkeys2, keyspecs2);
    check(
          h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "expected engine success");

    // verify the response sent to the client is correct
    resp = parse_sync_response(last_body);

    check(resp.size() == nkeys2, "response has the same # of keys");

    keyset.clear();
    for (int i = 0; i < nkeys2; i++) {
        keyset.insert(keyspecs2[i]);
    }

    for (itresp = resp.begin(); itresp != resp.end(); itresp++) {
        key_spec_t keyspec = itresp->first;
        uint8_t eventid = itresp->second;

        check(keyspec.vbucketid == 0, "right vbucket id");
        check(keyset.find(keyspec) != keyset.end(), "key sent in the request");

        if (keyspec.key == "key1") {
            check(keyspec.cas == 999, "right cas");
            check(eventid == SYNC_INVALID_CAS,
                  "right event id (SYNC_INVALID_CAS)");
        } else if (keyspec.key == "fookey") {
            check(keyspec.cas == 0, "right cas");
            check(eventid == SYNC_INVALID_KEY,
                  "right event id (SYNC_INVALID_KEY)");
        } else {
            check(false, "received unpextec key in the response");
        }
    }

    free(pkt);

    return SUCCESS;
}
*/

static enum test_result test_sync_mutation(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const key_spec_t keyspecs[] = {
        key_spec_t(0, 0, "key1"), key_spec_t(0, 0, "key2"), key_spec_t(0, 0, "del_key"),
        key_spec_t(666, 0, "key_bad_cas"), key_spec_t(0, 0, "foo_key"),
    };
    const uint16_t nkeys = 5;
    pthread_t threads[nkeys];
    item *it = NULL;
    protocol_binary_request_header *pkt = create_sync_packet(0x00000004, nkeys, keyspecs);

    set_flush_param(h, h1, "sync_cmd_timeout", "10000");

    check(store(h, h1, NULL, OPERATION_SET, "key1", "foo", &it, 0, 0) == ENGINE_SUCCESS,
          "failed to store key1");
    check(store(h, h1, NULL, OPERATION_SET, "key2", "bar", &it, 0, 0) == ENGINE_SUCCESS,
          "failed to store key2");
    check(store(h, h1, NULL, OPERATION_SET, "key_bad_cas", "A", &it, 0, 0) == ENGINE_SUCCESS,
          "failed to store key_bad_cas");
    check(store(h, h1, NULL, OPERATION_SET, "del_key", "X", &it, 0, 0) == ENGINE_SUCCESS,
          "failed to store del_key");

    std::vector<set_key_thread_params*> params;

    for (int i = 0; i < (nkeys - 2); i++) {
        set_key_thread_params *p = (set_key_thread_params *) malloc(sizeof(set_key_thread_params));
        p->h = h;
        p->h1 = h1;
        p->keyspec = &keyspecs[i];
        p->value = "qwerty";
        p->iterations = 1;
        p->wait = 2000000;
        params.push_back(p);
    }

    for (int i = 0; i < (nkeys - 2); i++) {
        int r;

        if (keyspecs[i].key == "del_key") {
            r = pthread_create(&threads[i], NULL, conc_del_key_thread, params[i]);
        } else {
            r = pthread_create(&threads[i], NULL, conc_set_key_thread, params[i]);
        }

        assert(r == 0);
    }

    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "SYNC on mutation operation failed");

    for (int i = 0; i < (nkeys - 2); i++) {
        void *trv = NULL;
        int r = pthread_join(threads[i], &trv);
        assert(r == 0);
    }

    // verify the response sent to the client is correct
    std::list< std::pair<key_spec_t, uint8_t> > resp = parse_sync_response(last_body);

    check(resp.size() == nkeys, "response has the same # of keys");

    std::list< std::pair<key_spec_t, uint8_t> >::iterator itresp = resp.begin();

    for ( ; itresp != resp.end(); itresp++) {
        key_spec_t keyspec = itresp->first;
        uint8_t eventid = itresp->second;

        check(keyspec.vbucketid == 0, "right vbucket id");

        if (keyspec.key == "key1" || keyspec.key == "key2") {
            check(eventid == SYNC_MODIFIED_EVENT,
                  "right event id (SYNC_MODIFIED_EVENT)");
        } else if (keyspec.key == "del_key") {
            check(keyspec.cas == 0, "right cas");
            check(eventid == SYNC_DELETED_EVENT,
                  "right event id (SYNC_DELETED_EVENT)");
        } else if (keyspec.key == "key_bad_cas") {
            check(keyspec.cas == 666, "right cas");
            check(eventid == SYNC_INVALID_CAS,
                  "right event id (SYNC_INVALID_CAS)");
        } else if (keyspec.key == "foo_key") {
            check(keyspec.cas == 0, "right cas");
            check(eventid == SYNC_INVALID_KEY,
                  "right event id (SYNC_INVALID_KEY)");
        } else {
            check(false, "unexpected key in SYNC on mutation response");
        }
    }

    std::vector<set_key_thread_params*>::iterator itp = params.begin();
    for ( ; itp != params.end(); itp++) {
        free(*itp);
    }

    free(pkt);

    return SUCCESS;
}

static enum test_result test_sync_replication(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const uint16_t test_vbid = 307;
    check(set_vbucket_state(h, h1, test_vbid, vbucket_state_active),
          "Failed to set test vbucket state.");

    const uint8_t nReplicas = 1;
    const key_spec_t keyspecs[] = {
        key_spec_t(0, test_vbid, "key1"), key_spec_t(0, test_vbid, "key2"),
        key_spec_t(0, test_vbid, "key3"), key_spec_t(0, test_vbid, "key4"),
        key_spec_t(0, test_vbid, "bad_key")
    };
    const uint16_t nkeys = 5;

    set_flush_param(h, h1, "sync_cmd_timeout", "10000");

    for (int i = 0; i < (nkeys - 1); i++) {
        check(store(h, h1, NULL, OPERATION_SET, keyspecs[i].key.c_str(),
                    "foobar", NULL, 0, test_vbid) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }

    std::set<key_spec_t> *expectedKeyset = new std::set<key_spec_t>();
    for (int i = 0; i < (nkeys - 1); i++) {
        expectedKeyset->insert(keyspecs[i]);
    }

    pthread_t threads[nReplicas];
    std::vector<tap_stream_thread_params*> params;

    for (int i = 0; i < nReplicas; i++) {
        tap_stream_thread_params *p = new tap_stream_thread_params();
        std::stringstream ss;
        ss << "tap_stream_" << (char) ('0' + (i + 1));

        p->h = h;
        p->h1 = h1;
        p->expectedKeys = expectedKeyset;
        p->wait = 2000000;
        p->streamName = ss.str();
        params.push_back(p);
    }

    for (int i = 0; i < nReplicas; i++) {
        int r = pthread_create(&threads[i], NULL, tap_stream_thread, params[i]);
        assert(r == 0);
    }

    protocol_binary_request_header *pkt;
    pkt = create_sync_packet((uint32_t) ((nReplicas & 0x0f) << 4), nkeys, keyspecs);

    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "SYNC on replication operation failed");

    for (int i = 0; i < nReplicas; i++) {
        void *trv = NULL;
        int r = pthread_join(threads[i], &trv);
        assert(r == 0);
    }

    // verify the response sent to the client is correct
    std::list< std::pair<key_spec_t, uint8_t> > resp = parse_sync_response(last_body);

    check(resp.size() == nkeys, "response has the same # of keys");

    std::list< std::pair<key_spec_t, uint8_t> >::iterator itresp = resp.begin();

    for ( ; itresp != resp.end(); itresp++) {
        key_spec_t keyspec = itresp->first;
        uint8_t eventid = itresp->second;

        check(keyspec.vbucketid == test_vbid, "right vbucket id");

        if (keyspec.key == "bad_key") {
            check(keyspec.cas == 0, "right cas");
            check(eventid == SYNC_INVALID_KEY,
                  "right event id (SYNC_INVALID_KEY)");
        } else {
            check(eventid == SYNC_REPLICATED_EVENT,
                  "right event id (SYNC_REPLICATED_EVENT)");
        }
    }

    std::vector<tap_stream_thread_params*>::iterator it = params.begin();
    for ( ; it != params.end(); it++) {
        delete *it;
    }

    delete expectedKeyset;
    free(pkt);

    return SUCCESS;
}

/**
 * SYNC for persistence not yet supported (MB-3817 related).
 */
/*
static enum test_result test_sync_persistence_or_replication(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const key_spec_t keyspecs[] = {
        key_spec_t(0, 0, "key1"), key_spec_t(0, 0, "key2"),
        key_spec_t(0, 0, "key3"), key_spec_t(0, 0, "key4"),
        key_spec_t(0, 0, "key5"), key_spec_t(0, 0, "key6"),
        key_spec_t(0, 0, "key7"), key_spec_t(0, 0, "key8"),
        key_spec_t(0, 0, "key9"), key_spec_t(0, 0, "key10"),
        key_spec_t(0, 0, "key11"), key_spec_t(0, 0, "key12"),
        key_spec_t(0, 0, "key13"), key_spec_t(0, 0, "key14"),
        key_spec_t(666, 0, "bad_cas"), key_spec_t(0, 0, "NonExistentKey")
    };
    const uint16_t nkeys = sizeof(keyspecs) / sizeof(key_spec_t);
    const uint8_t nReplicas = 1;
    pthread_t threads[nReplicas + 1]; // 3 tap stream threads plus 1 key set thread
    set_key_thread_params* setKeyParams = new set_key_thread_params();
    std::vector<tap_stream_thread_params*> tapStreamParams;
    std::set<key_spec_t> *expectedKeyset = new std::set<key_spec_t>();

    set_flush_param(h, h1, "sync_cmd_timeout", "10000");

    for (int i = 0; i < (nkeys - 1); i++) {
        check(store(h, h1, NULL, OPERATION_SET, keyspecs[i].key.c_str(),
                    "qwerty", NULL, 0, keyspecs[i].vbucketid) == ENGINE_SUCCESS,
              "Failed to store an item.");
        expectedKeyset->insert(keyspecs[i]);
    }

    for (int i = 0; i < nReplicas; i++) {
        tap_stream_thread_params *p = new tap_stream_thread_params();
        std::stringstream ss;
        ss << "tap_stream_" << (char) ('0' + (i + 1));

        p->h = h;
        p->h1 = h1;
        p->expectedKeys = expectedKeyset;
        p->wait = (i + 1) * 100000;
        p->streamName = ss.str();
        tapStreamParams.push_back(p);
    }

    setKeyParams->h = h;
    setKeyParams->h1 = h1;
    setKeyParams->keyspec = &keyspecs[1];
    setKeyParams->value = "foobar";
    setKeyParams->iterations = 1;
    setKeyParams->wait = 200;

    for (int i = 0; i < nReplicas; i++) {
        assert(pthread_create(&threads[i], NULL, tap_stream_thread, tapStreamParams[i]) == 0);
    }

    assert(pthread_create(&threads[nReplicas], NULL, conc_set_key_thread, setKeyParams) == 0);

    protocol_binary_request_header *pkt;

    pkt = create_sync_packet((uint32_t) (((nReplicas & 0x0f) << 4) | 0x8), nkeys, keyspecs);

    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "SYNC on replication or persistence command success");

    for (int i = 0; i < (nReplicas + 1); i++) {
        void *trv = NULL;
        int r = pthread_join(threads[i], &trv);
        assert(r == 0);
    }

    // verify the response sent to the client is correct
    std::list< std::pair<key_spec_t, uint8_t> > resp = parse_sync_response(last_body);

    check(resp.size() >= nkeys, "response has at least the same # of keys");

    std::set<key_spec_t> keyset;
    for (int i = 0; i < nkeys; i++) {
        keyset.insert(keyspecs[i]);
    }

    for (std::list< std::pair<key_spec_t, uint8_t> >::iterator itresp = resp.begin();
         itresp != resp.end(); itresp++) {

        key_spec_t keyspec = itresp->first;
        uint8_t eventid = itresp->second;

        check(keyspec.vbucketid == 0, "right vbucket id");
        check(keyset.find(keyspec) != keyset.end(), "key sent in the request");

        if (keyspec.key == "bad_cas") {
            check(keyspec.cas == 666, "right cas");
            check(eventid == SYNC_INVALID_CAS,
                  "right event id (SYNC_INVALID_CAS)");
        } else if (keyspec.key == "NonExistentKey") {
            check(keyspec.cas == 0, "right cas");
            check(eventid == SYNC_INVALID_KEY,
                  "right event id (SYNC_INVALID_KEY)");
        } else {
            check((eventid == SYNC_REPLICATED_EVENT) || (eventid == SYNC_PERSISTED_EVENT),
                  "right event id (SYNC_REPLICATED_EVENT || SYNC_PERSISTED_EVENT)");
        }
    }

    // Assign new values to the keys, so that they're replica count is reset to zero,
    // wait a while so that they get persisted and sync again on "replication or persistence".
    // Also don't spawn tap stream threads, so that the event associated with each key will
    // be the "persisted" event.

    for (int i = 0; i < (nkeys - 1); i++) {
        check(store(h, h1, NULL, OPERATION_SET, keyspecs[i].key.c_str(),
                    "qwerty", NULL, 0, keyspecs[i].vbucketid) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }

    usleep(500000);

    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "SYNC on replication or persistence command success");

    resp = parse_sync_response(last_body);

    check(resp.size() == nkeys, "response has the same # of keys");

    for (std::list< std::pair<key_spec_t, uint8_t> >::iterator itresp = resp.begin();
         itresp != resp.end(); itresp++) {

        key_spec_t keyspec = itresp->first;
        uint8_t eventid = itresp->second;

        check(keyspec.vbucketid == 0, "right vbucket id");
        check(keyset.find(keyspec) != keyset.end(), "key sent in the request");

        if (keyspec.key == "bad_cas") {
            check(keyspec.cas == 666, "right cas");
            check(eventid == SYNC_INVALID_CAS,
                  "right event id (SYNC_INVALID_CAS)");
        } else if (keyspec.key == "NonExistentKey") {
            check(keyspec.cas == 0, "right cas");
            check(eventid == SYNC_INVALID_KEY,
                  "right event id (SYNC_INVALID_KEY)");
        } else {
            check(eventid == SYNC_PERSISTED_EVENT,
                  "right event id (SYNC_PERSISTED_EVENT)");
        }
    }

    for (std::vector<tap_stream_thread_params*>::iterator it = tapStreamParams.begin();
         it != tapStreamParams.end(); it++) {
        delete *it;
    }

    free(pkt);
    delete setKeyParams;
    delete expectedKeyset;

    return SUCCESS;
}
*/

/**
 * SYNC for persistence not yet supported (MB-3817 related).
 */
/*
static enum test_result test_sync_persistence_and_replication(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const key_spec_t keyspecs[] = {
        key_spec_t(0, 0, "key1"), key_spec_t(0, 0, "key2"),
        key_spec_t(0, 0, "key3"), key_spec_t(0, 0, "key4"),
        key_spec_t(0, 0, "key5"), key_spec_t(0, 0, "key6"),
        key_spec_t(0, 0, "key7"), key_spec_t(0, 0, "key8"),
        key_spec_t(0, 0, "key9"), key_spec_t(0, 0, "key10"),
        key_spec_t(0, 0, "key11"), key_spec_t(0, 0, "key12"),
        key_spec_t(0, 0, "key13"), key_spec_t(0, 0, "key14"),
        key_spec_t(666, 0, "bad_cas"), key_spec_t(0, 0, "NonExistentKey")
    };
    const uint16_t nkeys = sizeof(keyspecs) / sizeof(key_spec_t);
    const uint8_t nReplicas = 1;
    // 3 tap stream threads plus 1 set key thread for each existent key
    pthread_t threads[nReplicas + nkeys - 1];
    std::vector<set_key_thread_params*> setKeyParams;
    std::vector<tap_stream_thread_params*> tapStreamParams;
    std::set<key_spec_t> *expectedKeyset = new std::set<key_spec_t>();

    set_flush_param(h, h1, "sync_cmd_timeout", "10000");

    for (int i = 0; i < (nkeys - 1); i++) {
        check(store(h, h1, NULL, OPERATION_SET, keyspecs[i].key.c_str(),
                    "qwerty", NULL, 0, keyspecs[i].vbucketid) == ENGINE_SUCCESS,
              "Failed to store an item.");
        expectedKeyset->insert(keyspecs[i]);
    }

    for (int i = 0; i < nReplicas; i++) {
        tap_stream_thread_params *p = new tap_stream_thread_params();
        std::stringstream ss;
        ss << "tap_stream_" << (char) ('0' + (i + 1));

        p->h = h;
        p->h1 = h1;
        p->expectedKeys = expectedKeyset;
        p->wait = (i + 1) * 100000;
        p->streamName = ss.str();
        tapStreamParams.push_back(p);
    }

    for (int i = 0; i < (nkeys - 1); i++) {
        set_key_thread_params *p = new set_key_thread_params();

        p->h = h;
        p->h1 = h1;
        p->keyspec = &keyspecs[i];
        p->value = "foobar";
        p->iterations = 1;
        p->wait = 200;
        setKeyParams.push_back(p);
    }

    for (int i = 0; i < nReplicas; i++) {
        assert(pthread_create(&threads[i], NULL, tap_stream_thread, tapStreamParams[i]) == 0);
    }

    for (int i = 0; i < (nkeys - 1); i++) {
        int r = pthread_create(&threads[nReplicas + i], NULL, conc_set_key_thread, setKeyParams[i]);
        assert(r == 0);
    }

    protocol_binary_request_header *pkt;

    pkt = create_sync_packet((uint32_t) (((nReplicas & 0x0f) << 4) | 0x8 | 0x2), nkeys, keyspecs);

    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          "SYNC on replication and persistence command success");

    for (int i = 0; i < (nReplicas + nkeys - 1); i++) {
        void *trv = NULL;
        assert(pthread_join(threads[i], &trv) == 0);
    }

    // verify the response sent to the client is correct
    std::list< std::pair<key_spec_t, uint8_t> > resp = parse_sync_response(last_body);

    check(resp.size() == ((2 * nkeys) - 2), "response has right # of keys");

    std::set<key_spec_t> keyset;
    for (int i = 0; i < nkeys; i++) {
        keyset.insert(keyspecs[i]);
    }

    std::set< std::pair<std::string, uint8_t> > keyEvents;

    for (std::list< std::pair<key_spec_t, uint8_t> >::iterator itresp = resp.begin();
         itresp != resp.end(); itresp++) {

        key_spec_t keyspec = itresp->first;
        uint8_t eventid = itresp->second;

        check(keyspec.vbucketid == 0, "right vbucket id");
        check(keyset.find(keyspec) != keyset.end(), "key sent in the request");

        if (keyspec.key == "bad_cas") {
            check(keyspec.cas == 666, "right cas");
            check(eventid == SYNC_INVALID_CAS,
                  "right event id (SYNC_INVALID_CAS)");
        } else if (keyspec.key == "NonExistentKey") {
            check(keyspec.cas == 0, "right cas");
            check(eventid == SYNC_INVALID_KEY,
                  "right event id (SYNC_INVALID_KEY)");
        } else {
            check((eventid == SYNC_REPLICATED_EVENT) || (eventid == SYNC_PERSISTED_EVENT),
                  "right event id (SYNC_REPLICATED_EVENT || SYNC_PERSISTED_EVENT)");
        }

        std::pair<std::string, uint8_t> keyEv(keyspec.key, eventid);

        if (keyEvents.find(keyEv) != keyEvents.end()) {
            check(false, "duplicated keyspec in the response");
        } else {
            keyEvents.insert(keyEv);
        }
    }

    check(keyEvents.size() == ((2 * nkeys) - 2), "response has right # of keys");

    for (int i = 0; i < nkeys; i++) {
        std::string k = keyspecs[i].key;

        if ((k == "bad_cas") || (k == "NonExistentKey")) {
            continue;
        }

        std::pair<std::string, uint8_t> persistedEv(k, SYNC_PERSISTED_EVENT);
        check(keyEvents.find(persistedEv) != keyEvents.end(),
              "response has persisted event for the keyspec");

        std::pair<std::string, uint8_t> replicatedEv(k, SYNC_REPLICATED_EVENT);
        check(keyEvents.find(replicatedEv) != keyEvents.end(),
              "response has replicated event for the keyspec");
    }

    for (std::vector<tap_stream_thread_params*>::iterator it = tapStreamParams.begin();
         it != tapStreamParams.end(); it++) {
        delete *it;
    }

    for (std::vector<set_key_thread_params*>::iterator it = setKeyParams.begin();
         it != setKeyParams.end(); it++) {
        delete *it;
    }

    free(pkt);
    delete expectedKeyset;

    return SUCCESS;
}
*/

static enum test_result test_sync_timeout(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const uint16_t test_vbid = 307;
    check(set_vbucket_state(h, h1, test_vbid, vbucket_state_active),
          "Failed to set test vbucket state.");

    const uint8_t nReplicas = 1;
    const key_spec_t keyspecs[] = {
        key_spec_t(0, test_vbid, "key1"), key_spec_t(0, test_vbid, "key2"),
        key_spec_t(0, test_vbid, "key3"), key_spec_t(0, test_vbid, "key4"),
        key_spec_t(0, test_vbid, "bad_key")
    };
    const uint16_t nkeys = 5;

    for (int i = 0; i < (nkeys - 1); i++) {
        check(store(h, h1, NULL, OPERATION_SET, keyspecs[i].key.c_str(),
                    "foobar", NULL, 0, test_vbid) == ENGINE_SUCCESS,
              "Failed to store an item.");
    }

    protocol_binary_request_header *pkt;
    pkt = create_sync_packet((uint32_t) ((nReplicas & 0x0f) << 4), nkeys, keyspecs);

    // We didn't start a tap consumer for the vbucket, so it will timeout after
    // about 2.5 seconds.
    ENGINE_ERROR_CODE r = h1->unknown_command(h, NULL, pkt, add_response, NULL);
    check( r == ENGINE_SUCCESS, "The server should know the command");
    check(last_status == PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
          "Abort SYNC request if it takes too long");
    free(pkt);

    return SUCCESS;
}

static protocol_binary_request_header* create_restore_file_packet(const char *fnm)
{
    protocol_binary_request_header *header;
    uint32_t len = strlen(fnm);
    header = (protocol_binary_request_header *)calloc(sizeof(*header), + len);
    header->request.opcode = CMD_RESTORE_FILE;
    header->request.keylen = htons((uint16_t)len);
    header->request.bodylen = htonl(len);
    memcpy(header + 1, fnm, len);
    return header;
}

static enum test_result test_restore_not_enabled(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    protocol_binary_request_header *req = create_restore_file_packet("foo");
    ENGINE_ERROR_CODE r = h1->unknown_command(h, NULL, req, add_response, NULL);
    check(r == ENGINE_SUCCESS, "The server should know the command");
    check(last_status == PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED,
          "The server should not allow restore to be initiated");
    free(req);
    return SUCCESS;
}

static enum test_result test_restore_no_such_file(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    check(set_flush_param(h, h1, "start_restore_vb", "0") == true, "Unable to start restore mode");
    protocol_binary_request_header *req = create_restore_file_packet("@@@no-such-file@@@");
    ENGINE_ERROR_CODE r = h1->unknown_command(h, NULL, req, add_response, NULL);
    check(r == ENGINE_SUCCESS, "The server should know the command");
    check(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
          "The server should check if the file exists");
    free(req);
    check(set_flush_param(h, h1, "complete_restore_vb", "0") == true, "Unable to start restore mode");
    return SUCCESS;
}

static void waitfor_restore_state(ENGINE_HANDLE *h,
                                  ENGINE_HANDLE_V1 *h1,
                                  const char *state,
                                  uint32_t sampletime = 250)
{
    do {
        usleep(sampletime);
        vals.clear();
        check(h1->get_stats(h, NULL, "restore", 7, add_stats) == ENGINE_SUCCESS,
              "Failed to get stats.");
    } while (vals["ep_restore:state"] != state);
    vals.clear();
}

static enum test_result test_restore_invalid_file(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char cwd[1024];
    if (!getcwd(cwd, sizeof(cwd))) {
        fprintf(stderr, "Invoking getcwd failed!!!\n");
        return FAIL;
    }
    strcat(cwd, "/sizes");
    check(access(cwd, F_OK) == 0, "Could not find sizes");
    check(set_flush_param(h, h1, "start_restore_vb", "0") == true, "Unable to start restore mode");
    protocol_binary_request_header *req = create_restore_file_packet(cwd);
    ENGINE_ERROR_CODE r = h1->unknown_command(h, NULL, req, add_response, NULL);
    check(r == ENGINE_SUCCESS, "The server should know the command");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "The server should start the backup");
    free(req);
    waitfor_restore_state(h, h1, "zombie");
    vals.clear();
    check(h1->get_stats(h, NULL, "restore", 7, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_restore:last_error") != vals.end(),
          "Expected the restore manager to set an error message");
    check(vals["ep_restore:number_busy"] == "0", "Expected no data change");
    check(vals["ep_restore:number_skipped"] == "0", "Expected no data change");
    check(vals["ep_restore:number_restored"] == "0", "Expected no data change");
    check(vals["ep_restore:number_wrong_vbucket"] == "0", "Expected no data change");

    check(set_flush_param(h, h1, "complete_restore_vb", "0") == true, "Unable to start restore mode");
    return SUCCESS;
}

static enum test_result test_restore_data_miss(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    item *it = NULL;
    ENGINE_ERROR_CODE r;

    check(set_flush_param(h, h1, "start_restore_vb", "0") == true, "Unable to start restore mode");
    r = h1->get(h, NULL, &it, "foo", 3, 0);
    check(r == ENGINE_TMPFAIL, "Data miss should be tmpfail");

    r = h1->allocate(h, NULL, &it, "foo", 3, 100, 0, 0, 0, 0);
    check(r == ENGINE_SUCCESS, "Allocation failed.");

    uint64_t cas;
    r = h1->store(h, NULL, it, &cas, OPERATION_ADD, 0);
    check(r == ENGINE_TMPFAIL, "Add shouldn't work during restore.");
    r = h1->store(h, NULL, it, &cas, OPERATION_REPLACE, 0);
    check(r == ENGINE_TMPFAIL, "Replace shouldn't work for a missing item during restore.");
    r = h1->remove(h, NULL, "foobar", 6, 0, 0);
    check(r == ENGINE_KEY_ENOENT, "Delete of non-existing item should work during restore");
    r = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(r == ENGINE_SUCCESS, "Set should work.");
    r = h1->store(h, NULL, it, &cas, OPERATION_REPLACE, 0);
    check(r == ENGINE_SUCCESS, "Replace should work for existing objects.");
    r = h1->remove(h, NULL, "foo", 3, 0, 0);
    check(r == ENGINE_SUCCESS, "Delete of existing object should work");
    h1->release(h, NULL, it);

    uint64_t value;
    r = h1->arithmetic(h, NULL, "bar", 3, true, false, 1, 0, 0,
                       &cas, &value, 0);
    check(r == ENGINE_TMPFAIL, "incr of nonexistsing key (without create) should be tmpfail");
    r = h1->arithmetic(h, NULL, "bar", 3, true, true, 1, 0, 0,
                       &cas, &value, 0);
    check(r == ENGINE_TMPFAIL, "incr of nonexistsing key (with create) should tmpfail");

    check(set_flush_param(h, h1, "complete_restore_vb", "0") == true, "Unable to complete restore mode");
    // Now we should be in operational mode...
    r = h1->get(h, NULL, &it, "foo", 3, 0);
    check(r == ENGINE_KEY_ENOENT, "Key shouldn't be there");

    r = h1->allocate(h, NULL, &it, "foo", 3, 100, 0, 0, 0, 0);
    check(r == ENGINE_SUCCESS, "Allocation failed.");

    r = h1->store(h, NULL, it, &cas, OPERATION_ADD, 0);
    check(r == ENGINE_SUCCESS, "Add should work for missing items.");
    r = h1->remove(h, NULL, "foo", 3, 0, 0);
    check(r == ENGINE_SUCCESS, "Delete of existing object should work");
    r = h1->store(h, NULL, it, &cas, OPERATION_REPLACE, 0);
    check(r == ENGINE_NOT_STORED, "Replace shouldn't work for a missing item.");
    r = h1->arithmetic(h, NULL, "bar", 3, true, false, 1, 0, 0,
                       &cas, &value, 0);
    check(r == ENGINE_KEY_ENOENT, "incr of nonexistsing key (without create) shouldn't work");
    r = h1->arithmetic(h, NULL, "bar", 3, true, true, 1, 0, 0,
                       &cas, &value, 0);
    check(r == ENGINE_SUCCESS, "incr of nonexistsing key (with create) should work");
    return SUCCESS;
}

static void ensure_file(const char *fname)
{
    if (access(fname, F_OK) != 0) {
        std::stringstream ss;
        ss << "No such file: " << fname;
        check(access(fname, F_OK) == 0, ss.str().c_str());
    }
}

static enum test_result test_restore_clean(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1)
{
    char cwd[1024];
    if (!getcwd(cwd, sizeof(cwd))) {
        fprintf(stderr, "Invoking getcwd failed!!!\n");
        return FAIL;
    }
    strcat(cwd, "/mbbackup-0001.mbb");
    ensure_file(cwd);

    for (uint16_t ii = 0; ii < 1000; ++ ii) {
        check(set_vbucket_state(h, h1, ii, vbucket_state_active), "Failed to activate vbucket");
    }

    check(set_flush_param(h, h1, "start_restore_vb", "0") == true, "Unable to start restore mode");
    protocol_binary_request_header *req = create_restore_file_packet(cwd);
    ENGINE_ERROR_CODE r = h1->unknown_command(h, NULL, req, add_response, NULL);
    check(r == ENGINE_SUCCESS, "The server should know the command");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "The server should start the backup");
    free(req);
    waitfor_restore_state(h, h1, "zombie", 2000);
    vals.clear();
    check(h1->get_stats(h, NULL, "restore", 7, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_restore:last_error") == vals.end(),
          "I shouldn't get an error message");

    check(vals["ep_restore:number_skipped"] == "6", "Expected no data change");
    check(vals["ep_restore:number_restored"] == "9988", "We have one vbucket");
    check(vals["ep_restore:number_wrong_vbucket"] == "0", "We don't have all vbuckets");
    check(vals["ep_restore:number_expired"] == "1", "We don't have all vbuckets");
    check(set_flush_param(h, h1, "complete_restore_vb", "0") == true, "Unable to complete restore mode");
    return SUCCESS;
}

static enum test_result test_restore_clean_vbucket_subset(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char cwd[1024];
    if (!getcwd(cwd, sizeof(cwd))) {
        fprintf(stderr, "Invoking getcwd failed!!!\n");
        return FAIL;
    }
    strcat(cwd, "/mbbackup-0002.mbb");
    ensure_file(cwd);
    check(set_flush_param(h, h1, "start_restore_vb", "0") == true, "Unable to start restore mode");
    protocol_binary_request_header *req = create_restore_file_packet(cwd);
    ENGINE_ERROR_CODE r = h1->unknown_command(h, NULL, req, add_response, NULL);
    check(r == ENGINE_SUCCESS, "The server should know the command");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "The server should start the backup");
    free(req);
    waitfor_restore_state(h, h1, "zombie", 2000);
    vals.clear();
    check(h1->get_stats(h, NULL, "restore", 7, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_restore:last_error") == vals.end(),
          "I shouldn't get an error message");

    check(vals["ep_restore:number_skipped"] == "0", "Expected no data change");
    check(vals["ep_restore:number_restored"] == "448", "We have one vbucket");
    check(vals["ep_restore:number_wrong_vbucket"] == "9054", "We don't have all vbuckets");
    check(vals["ep_restore:number_expired"] == "1", "We don't have all vbuckets");
    check(set_flush_param(h, h1, "complete_restore_vb", "0") == true, "Unable to complete restore mode");
    return SUCCESS;
}

#ifdef future
static enum test_result test_restore_multi(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    for (uint16_t ii = 0; ii < 1000; ++ ii) {
        check(set_vbucket_state(h, h1, ii, vbucket_state_active), "Failed to activate vbucket");
    }

    for (int ii = 2; ii > 0; ii--) {
        char cwd[1024];
        if (!getcwd(cwd, sizeof(cwd))) {
            fprintf(stderr, "Invoking getcwd failed!!!\n");
            return FAIL;
        }
        sprintf(cwd + strlen(cwd), "/mbbackup-%04d.mbb", ii);
        ensure_file(cwd);
        protocol_binary_request_header *req = create_restore_file_packet(cwd);
        ENGINE_ERROR_CODE r = h1->unknown_command(h, NULL, req, add_response, NULL);
        check(r == ENGINE_SUCCESS, "The server should know the command");
        check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
              "The server should start the backup");
        free(req);
        waitfor_restore_state(h, h1, "zombie", 2000);
    }

    vals.clear();
    dump_stats = true;
    check(h1->get_stats(h, NULL, "restore", 7, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_restore:last_error") == vals.end(),
          "I shouldn't get an error message");
    check(vals["ep_restore:number_skipped"] == "5", "Expected no data change");
    check(vals["ep_restore:number_restored"] == "18118", "We have one vbucket");
    check(vals["ep_restore:number_wrong_vbucket"] == "0", "We don't have all vbuckets");
    check(vals["ep_restore:number_expired"] == "2", "We don't have all vbuckets");
    check(set_flush_param(h, h1, "complete_restore_vb", "0") == true, "Unable to complete restore mode");
    return SUCCESS;
}
#endif

static enum test_result test_restore_with_data(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char cwd[1024];
    if (!getcwd(cwd, sizeof(cwd))) {
        fprintf(stderr, "Invoking getcwd failed!!!\n");
        return FAIL;
    }
    strcat(cwd, "/mbbackup-0001.mbb");
    ensure_file(cwd);
    protocol_binary_request_header *req = create_restore_file_packet(cwd);

    for (uint16_t ii = 0; ii < 1000; ++ ii) {
        check(set_vbucket_state(h, h1, ii, vbucket_state_active), "Failed to activate vbucket");
    }
    item *it = NULL;
    uint64_t cas;
    ENGINE_ERROR_CODE r;

    r = h1->allocate(h, NULL, &it, "mykey1", 6, 100, 0, 0, 0, 0);
    check(r == ENGINE_SUCCESS, "Allocation failed.");
    r = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(r == ENGINE_SUCCESS, "Set should work.");
    h1->release(h, NULL, it);
    r = h1->allocate(h, NULL, &it, "mykey2", 6, 100, 0, 0, 0, 0);
    check(r == ENGINE_SUCCESS, "Allocation failed.");
    r = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(r == ENGINE_SUCCESS, "Set should work.");
    r = h1->remove(h, NULL, "mykey2", 6, 0, 0);
    check(r == ENGINE_SUCCESS, "Delete of existing object should work");
    h1->release(h, NULL, it);
    r = h1->allocate(h, NULL, &it, "mykey3", 6, 100, 0, 0, 0, 0);
    check(r == ENGINE_SUCCESS, "Allocation failed.");
    r = h1->store(h, NULL, it, &cas, OPERATION_SET, 0);
    check(r == ENGINE_SUCCESS, "Set should work.");
    h1->release(h, NULL, it);
    check(set_flush_param(h, h1, "start_restore_vb", "0") == true, "Unable to start restore mode");
    r = h1->unknown_command(h, NULL, req, add_response, NULL);
    check(r == ENGINE_SUCCESS, "The server should know the command");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "The server should start the backup");
    free(req);
    waitfor_restore_state(h, h1, "zombie", 2000);
    vals.clear();
    check(h1->get_stats(h, NULL, "restore", 7, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_restore:last_error") == vals.end(),
          "I shouldn't get an error message");
    check(vals["ep_restore:number_skipped"] == "8", "Expected no data change");
    check(vals["ep_restore:number_restored"] == "9986", "We have one vbucket");
    check(vals["ep_restore:number_wrong_vbucket"] == "0", "We don't have all vbuckets");
    check(set_flush_param(h, h1, "complete_restore_vb", "0") == true, "Unable to complete restore mode");
    return SUCCESS;
}

static enum test_result test_get_last_closed_checkpoint_id(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    // Number of max items allowed per checkpoint is set to 500. Inserting more than 500 items will cause
    // a new open checkpoint to be created.
    for (int j = 0; j < 600; ++j) {
        std::stringstream ss;
        ss << "key" << j;
        item *i;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), ss.str().c_str(), &i, 0, 0)
              == ENGINE_SUCCESS, "Failed to store a value");
        h1->release(h, NULL, i);
    }

    protocol_binary_request_get_vbucket req;
    protocol_binary_request_header *pkt;
    pkt = reinterpret_cast<protocol_binary_request_header*>(&req);
    memset(&req, 0, sizeof(req));

    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = CMD_LAST_CLOSED_CHECKPOINT;
    req.message.header.request.vbucket = htons(0);

    ENGINE_ERROR_CODE errcode = h1->unknown_command(h, NULL, pkt, add_response, NULL);
    if (errcode != ENGINE_SUCCESS) {
        fprintf(stderr, "Error code when getting the last closed checkpoint Id: %d\n", errcode);
        return FAIL;
    }

    if (last_status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        fprintf(stderr, "Last protocol status was %d (%s)\n",
                last_status, last_body ? last_body : "unknown");
        return FAIL;
    }

    uint64_t checkpointId;
    memcpy(&checkpointId, last_body, sizeof(checkpointId));
    checkpointId = static_cast<uint64_t>(ntohll(checkpointId));
    check(checkpointId == 1, "Last closed checkpoint Id for VB 0 should be 1");
    return SUCCESS;
}

static enum test_result test_eviction_switch(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    int pager_time = 1;
    char buffer[20];

    sprintf(buffer, "%d", pager_time);
    set_flush_param(h, h1, "exp_pager_stime", buffer);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set exp_pager_stime param");

    set_flush_param(h, h1, "eviction_policy", "lru");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set eviction_policy param");

    sleep(2 * pager_time);

    vals.clear();
    check(h1->get_stats(h, NULL, "eviction", strlen("eviction"), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("eviction_policy") != vals.end(), "Missing stat");
    check(vals["eviction_policy"] == "lru", "Expected policy name 'lru'");

    set_flush_param(h, h1, "eviction_policy", "bgeviction");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set eviction_policy param");

    sleep(2 * pager_time);

    vals.clear();
    check(h1->get_stats(h, NULL, "eviction", strlen("eviction"), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("eviction_policy") != vals.end(), "Missing stat");
    check(vals["eviction_policy"] == "bgeviction", "Expected policy name 'bgeviction'");

    return SUCCESS;
}

static enum test_result test_eviction_pause(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    int pager_time = 1;
    char buffer[20];

    set_flush_param(h, h1, "lru_mem_threshold_percent", "0");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set lru_mem_threshold_percent param");

    sprintf(buffer, "%d", pager_time);
    set_flush_param(h, h1, "lru_rebuild_stime", buffer);
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set lru_rebuild_stime param");

    set_flush_param(h, h1, "eviction_policy", "lru");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set eviction_policy param");

    set_flush_param(h, h1, "enable_eviction_job", "0");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set enable_eviction_job param");

    sleep(2 * pager_time);

    vals.clear();
    check(h1->get_stats(h, NULL, "eviction", strlen("eviction"), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("evpolicy_job_start_timestamp") != vals.end(), "Missing stat");
    std::string timestart = vals["evpolicy_job_start_timestamp"];
    check(vals.find("evpolicy_job_end_timestamp") != vals.end(), "Missing stat");
    std::string timeend = vals["evpolicy_job_end_timestamp"];

    sleep(2 * pager_time);

    vals.clear();
    check(h1->get_stats(h, NULL, "eviction", strlen("eviction"), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("evpolicy_job_start_timestamp") != vals.end(), "Missing stat");
    check(vals["evpolicy_job_start_timestamp"] == timestart, "Expected unchanged value for start timestamp");
    check(vals.find("evpolicy_job_end_timestamp") != vals.end(), "Missing stat");
    check(vals["evpolicy_job_end_timestamp"] == timeend, "Expected unchanged value for end timestamp");

    set_flush_param(h, h1, "enable_eviction_job", "1");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set enable_eviction_job param");

    sleep(2 * pager_time);

    vals.clear();
    check(h1->get_stats(h, NULL, "eviction", strlen("eviction"), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("evpolicy_job_start_timestamp") != vals.end(), "Missing stat");
    check(vals["evpolicy_job_start_timestamp"] != timestart, "Expected changed value for start timestamp");
    check(vals.find("evpolicy_job_end_timestamp") != vals.end(), "Missing stat");
    check(vals["evpolicy_job_end_timestamp"] != timeend, "Expected changed value for end timestamp");

    return SUCCESS;
}

static enum test_result test_validate_checkpoint_params(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    set_flush_param(h, h1, "chk_max_items", "1000");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set checkpoint_max_item param");
    set_flush_param(h, h1, "chk_period", "100");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set checkpoint_period param");
    set_flush_param(h, h1, "max_checkpoints", "2");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set max_checkpoints param");

    set_flush_param(h, h1, "chk_max_items", "50");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected to have an invalid value error for checkpoint_max_items param");
    set_flush_param(h, h1, "chk_period", "10");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected to have an invalid value error for checkpoint_period param");
    set_flush_param(h, h1, "max_checkpoints", "6");
    check(last_status == PROTOCOL_BINARY_RESPONSE_EINVAL,
          "Expected to have an invalid value error for max_checkpoints param");

    return SUCCESS;
}

static enum test_result test_bug_10443(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    /**
     * NOTE: When an open checkpoint is present, set a key with expiry one
     * second. The membase is started with inconsistent_slave_check = true,
     * min_data_age=2. Close the checkpoint immediately after the set. When the
     * flusher processes the item after meeting min_data_age, the
     * fetchValidValue() method tries to queue a delete mutation since item has
     * already expired. But, no open checkpoint is present and hence membase
     * crashes due to assert.
     */
    char eng_specific[64];
    uint64_t checkpointId = htonll(5);
    memset(eng_specific, 0, sizeof(eng_specific));
    item *it;
    uint64_t cas = 0;
    const void *cookie = testHarness.create_cookie();
    check(h1->tap_notify(h, cookie, NULL, 0, 0, 0, TAP_CONSUMER, 0, "consumer", 8, 0, 0, 0, 0, NULL, 0, 0, 0) == ENGINE_SUCCESS,
            "Failed to register tap consumer");

    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to set vbucket state.");

    //Allocate an item with expiry one second
    check(h1->allocate(h, NULL, &it, "key", 3, 100, 0, 1, 0, 0) == ENGINE_SUCCESS,
            "Allocation failed");

    check(h1->tap_notify(h, cookie, eng_specific, sizeof(eng_specific),
                         1, 0, TAP_CHECKPOINT_START, 1, "", 0, 828, 0, 0, 0,
                         &checkpointId, sizeof(checkpointId), 0, DI_CKSUM_DISABLED_STR) == ENGINE_SUCCESS,
          "Failed tap notify.");

    //Store when an open checkpoint is present
    check(h1->store(h, NULL, it, &cas, OPERATION_SET, 0) == ENGINE_SUCCESS, "Store failed");
    check(h1->tap_notify(h, cookie, eng_specific, sizeof(eng_specific),
                             1, 0, TAP_CHECKPOINT_END, 1, "", 0, 828, 0, 0, 0,
                             &checkpointId, sizeof(checkpointId), 0, DI_CKSUM_DISABLED_STR) == ENGINE_SUCCESS,
              "Failed tap notify.");
    h1->release(h, NULL, it);
    sleep(10);

    return SUCCESS;
}

static enum test_result test_lru_queue(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {

    uint16_t vbucketId = 0;
    std::string s;

    set_flush_param(h, h1, "lru_mem_threshold_percent", "0");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set lru_mem_threshold_percent param");
    set_flush_param(h, h1, "lru_rebuild_stime", "1");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set lru_rebuild_stime param");

    check(store(h, h1, NULL, OPERATION_SET, "key1", "data1", NULL, 0, vbucketId)
                == ENGINE_SUCCESS, "Failed to store an item.");
    check(store(h, h1, NULL, OPERATION_SET, "key2", "data2", NULL, 0, vbucketId)
                == ENGINE_SUCCESS, "Failed to store an item.");

    wait_for_flusher_to_settle(h, h1);
    sleep(2);

    vals.clear();
    check(h1->get_stats(h, NULL, "eviction", strlen("eviction"), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("lru_policy_evictable_items") != vals.end(), "Missing stat");
    s = vals["lru_policy_evictable_items"];
    check(strcmp(s.c_str(), "2") == 0, "Incorrect eviction queue count");

    evict_key(h, h1, "key1");
    sleep(2);

    vals.clear();
    check(h1->get_stats(h, NULL, "eviction", strlen("eviction"), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("lru_policy_evictable_items") != vals.end(), "Missing stat");
    s = vals["lru_policy_evictable_items"];
    check(strcmp(s.c_str(), "1") == 0, "Incorrect evictable item count");

    return SUCCESS;
}

static void check_eviction_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, int expected) {
    std::string s;
    int val;

    vals.clear();
    check(h1->get_stats(h, NULL, "eviction", strlen("eviction"), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("eviction_keys_evicted") != vals.end(), "Missing stat");
    s = vals["eviction_keys_evicted"];
    val = strtol(s.c_str(), NULL, 10);
    check(val == expected, "Incorrect evicted keys count");
}

/*
 * Ensure eviction is triggered under the right conditions. 
 * Evict if used + required > (total - (headroom  * 3 + total * 0.1))
 */
static enum test_result test_lru_eviction(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
#define BUFFER_SIZE 5000000

    char keyname[5];
    uint16_t vbucketId = 0;
    std::string s;
    char buffer[BUFFER_SIZE];
    memset(buffer, 'a', BUFFER_SIZE);

    set_flush_param(h, h1, "lru_mem_threshold_percent", "0");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "Failed to set lru_mem_threshold_percent param");

    set_flush_param(h, h1, "enable_flushall", "true");
    check(h1->flush(h, NULL, 0) == ENGINE_SUCCESS, "Failed to flush");

#define PARALLELISM 4
#define MAX_PER_THREAD (2 * 1024 * 1024)

    int remaining = (get_int_stat(h, h1, "ep_max_data_size") * 9 / 10)
        - (get_int_stat(h, h1, "eviction_headroom") + MAX_PER_THREAD * (PARALLELISM - 1))
        - get_int_stat(h, h1, "mem_used");

    int buf_size = remaining / 2 - 300; // delta for metadata
    buffer[buf_size] = '\0';
    int itemsRemoved = get_int_stat(h, h1, "ep_items_rm_from_checkpoints");

    for (int i = 0; i < 2; ++i ) {
        snprintf(keyname, 5, "key%d", i);
        check(store(h, h1, NULL, OPERATION_SET, keyname, buffer, NULL, 0, vbucketId)
              == ENGINE_SUCCESS, "Failed to store an item.");
    }
    check_eviction_stats(h, h1, 0);

    wait_for_flusher_to_settle(h, h1);
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);
    
    snprintf(keyname, 5, "key%d", 2);
    buffer[buf_size/2 - 200] = '\0';
    check(store(h, h1, NULL, OPERATION_SET, keyname, buffer, NULL, 0, vbucketId)
          == ENGINE_SUCCESS, "Failed to store an item.");
    check_eviction_stats(h, h1, 1);

    snprintf(keyname, 5, "key%d", 3);
    check(store(h, h1, NULL, OPERATION_SET, keyname, buffer, NULL, 0, vbucketId)
          == ENGINE_SUCCESS, "Failed to store an item.");
    check_eviction_stats(h, h1, 1);

    wait_for_flusher_to_settle(h, h1);
    testHarness.time_travel(65);
    wait_for_stat_change(h, h1, "ep_items_rm_from_checkpoints", itemsRemoved);
    sleep(5);

    snprintf(keyname, 5, "key%d", 4);
    buffer[buf_size/2 - 200] = 'a';
    check(store(h, h1, NULL, OPERATION_SET, keyname, buffer, NULL, 0, vbucketId)
          == ENGINE_SUCCESS, "Failed to store an item.");
    check_eviction_stats(h, h1, 2);

    return SUCCESS;
}

static enum test_result test_kvconf_basic(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    vals.clear();
    check(h1->get_stats(h, NULL, "kvstore", strlen("kvstore"), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    std::map<std::string, std::string> tvals(vals);
    check(tvals.find("num_kvstores") != tvals.end(), "Missing stat");
    check(tvals.find("kvstore:dbname") != tvals.end(), "Missing stat");
    check(tvals.find("kvstore:shardpattern") != tvals.end(), "Missing stat");
    check(tvals.find("kvstore:initfile") != tvals.end(), "Missing stat");
    check(tvals.find("kvstore:postInitfile") != tvals.end(), "Missing stat");
    check(tvals.find("kvstore:db_strategy") != tvals.end(), "Missing stat");
    check(tvals.find("kvstore:data_dbs") != tvals.end(), "Missing stat");

    int shards = get_int_stat(h, h1, "kvstore:db_shards", "kvstore");
    std::string key("kvstore:db_shard");
    for (int i = 0; i < shards; i++) {
        check(tvals.find(key + char('0' + i)) != tvals.end(), "Missing stat");
    }
    return SUCCESS;
}

static enum test_result test_kvconf_multikv(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    vals.clear();
    check(h1->get_stats(h, NULL, "kvstore", strlen("kvstore"), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    std::map<std::string, std::string> tvals(vals);
    check(tvals.find("num_kvstores") != tvals.end(), "Missing stat");
    check(tvals["num_kvstores"] == "3", "Wrong stat value");
    check(tvals.find("kvstore1:dbname") != tvals.end(), "Missing stat");
    check(tvals.find("kvstore2:dbname") != tvals.end(), "Missing stat");
    check(tvals.find("kvstore3:dbname") != tvals.end(), "Missing stat");

    int shards = get_int_stat(h, h1, "kvstore1:db_shards", "kvstore");
    std::string key("kvstore2:db_shard");
    std::set<std::string> val;
    for (int i = 0; i < shards; i++) {
        check(tvals.find(key + char('0' + i)) != tvals.end(), "Missing stat");
        val.insert(tvals[key + char('0' + i)]);
    }
    check((int) val.size() == shards, "db_shard names not unique");
    return SUCCESS;
}

static enum test_result test_kvconf_fail(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path, NULL,
                              false, false);

    check(h1->initialize(h, "kvstore_config_file=t/kv_multikv_fail.json")
          == ENGINE_FAILED, "Failed to fail to initialize");
    return SUCCESS;
}

static enum test_result test_tap_rcv_backfill(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char eng_specific[64];
    uint32_t tap_flag;
    memset(eng_specific, 0, sizeof(eng_specific));
    const void *cookie = testHarness.create_cookie();
    check(h1->tap_notify(h, cookie, NULL, 0, 0, 0, TAP_CONSUMER, 0, "consumer", 8, 0, 0, 0, 0, NULL, 0, 0, 0) == ENGINE_SUCCESS,
            "Failed to register tap consumer");

    tap_flag = htonl(TAP_OPAQUE_ENABLE_CHECKPOINT_SYNC);
    check(h1->tap_notify(h, cookie, &tap_flag, sizeof(uint32_t),1,
            0, TAP_OPAQUE, 1, "", 0, 828, 0, 0, 0, "",0, 0, DI_CKSUM_DISABLED_STR) == ENGINE_SUCCESS,
        "Failed tap notify.");

    tap_flag = htonl(TAP_OPAQUE_INITIAL_VBUCKET_STREAM);
    check(h1->tap_notify(h, cookie, &tap_flag, sizeof(uint32_t),1,
            0, TAP_OPAQUE, 1, "", 0, 828, 0, 0, 0, "",0, 0, DI_CKSUM_DISABLED_STR) == ENGINE_SUCCESS,
        "Failed tap notify.");

    check(h1->get_stats(h, NULL, "checkpoint", strlen("checkpoint"), add_stats) == ENGINE_SUCCESS,
            "Failed to get stats.");
    check(vals["vb_0:backfillphase"] == "true", "Expected backfill mode");

    for (size_t i = 0; i < 8192; ++i) {
        std::stringstream ss;
        ss<<"key_"<<i;
        char *data = static_cast<char *>(malloc(i));
        memset(data, 'x', i);
        check(h1->tap_notify(h, cookie, eng_specific, sizeof(eng_specific),
                             1, 0, TAP_MUTATION, 1, ss.str().c_str(), ss.str().length(), 828, std::numeric_limits<uint32_t>::max(), 0, 0,
                             data, i, 0, DI_CKSUM_DISABLED_STR) == ENGINE_SUCCESS,
              "Failed tap notify.");
        free(data);
    }

    tap_flag = htonl(TAP_OPAQUE_CLOSE_BACKFILL);
    check(h1->tap_notify(h, cookie, &tap_flag, sizeof(uint32_t),1,
            0, TAP_OPAQUE, 1, "", 0, 828, 0, 0, 0, "",0, 0, DI_CKSUM_DISABLED_STR) == ENGINE_SUCCESS,
        "Failed tap notify.");

    check(h1->get_stats(h, NULL, "checkpoint", strlen("checkpoint"), add_stats) == ENGINE_SUCCESS,
            "Failed to get stats.");
    check(vals["vb_0:backfillphase"] == "false", "Expected checkpoint mode");

    wait_for_flusher_to_settle(h, h1);
    check(get_int_stat(h, h1, "ep_total_persisted") == 8192, "Expected to persist all backfilled items");
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}

static enum test_result test_restore_vb0_persistence(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    char cwd[1024];
    if (!getcwd(cwd, sizeof(cwd))) {
        fprintf(stderr, "Invoking getcwd failed!!!\n");
        return FAIL;
    }
    strcat(cwd, "/mbbackup-0003.mbb");
    ensure_file(cwd);
    protocol_binary_request_header *req = create_restore_file_packet(cwd);

    check(set_flush_param(h, h1, "start_restore_vb", "0") == true, "Unable to start restore mode");
    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          "The server should start the backup");

    ENGINE_ERROR_CODE r;
    r = h1->unknown_command(h, NULL, req, add_response, NULL);
    check(r == ENGINE_SUCCESS, "The server should know the command");

    free(req);
    waitfor_restore_state(h, h1, "zombie", 2000);

    vals.clear();
    check(h1->get_stats(h, NULL, "restore", 7, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    check(vals.find("ep_restore:last_error") == vals.end(),
          "I shouldn't get an error message");

    int restored = atoi(vals["ep_restore:number_restored"].c_str());
    check(set_flush_param(h, h1, "complete_restore_vb", "0") == true, "Unable to complete restore mode");
    wait_for_flusher_to_settle(h, h1);
    check(get_int_stat(h, h1, "ep_total_persisted") == restored, "Expected to persist all restored items");

    return SUCCESS;
}

static int get_persisted_cpid_vb0_fromdb() {

    sqlite3 *db;
    const char * fn = "/tmp/test.db";
    if(sqlite3_open_v2(fn, &db, SQLITE_OPEN_READONLY, NULL) !=  SQLITE_OK) {
        throw std::runtime_error("Error initializing sqlite3");
    }

    PreparedStatement pst(db, "select checkpoint_id from vbucket_states where vbid=0;");
    if (pst.fetch()) {
        return pst.column_int64(0);
    }
    sqlite3_close(db);

    return -1;
}

static enum test_result test_checkpoint_vb0_persistence(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t checkpointId;

    char eng_specific[64];
    memset(eng_specific, 0, sizeof(eng_specific));
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Failed to set vbucket state.");
    const void *cookie = testHarness.create_cookie();
    check(h1->tap_notify(h, cookie, NULL, 0, 0, 0, TAP_CONSUMER, 0, "consumer", 8, 0, 0, 0, 0, NULL, 0, 0, 0) == ENGINE_SUCCESS,
            "Failed to register tap consumer");
    for (int i = 1; i < 5; ++i) {
        checkpointId = htonll(i);

        check(h1->tap_notify(h, cookie, eng_specific, sizeof(eng_specific),
                             1, 0, TAP_CHECKPOINT_START, 1, "", 0, 828, 0, 0, 0,
                             &checkpointId, sizeof(checkpointId), 0, DI_CKSUM_DISABLED_STR) == ENGINE_SUCCESS,
              "Failed tap notify.");

        for (size_t j = 0; j < 100; ++j) {
            std::stringstream ss;
            ss << j;
            check(h1->tap_notify(h, cookie, eng_specific, sizeof(eng_specific),
                                 1, 0, TAP_MUTATION, 1, ss.str().c_str(), ss.str().length(), 828, std::numeric_limits<uint32_t>::max(), 0, 0,
                                 "test_value", 10, 0, DI_CKSUM_DISABLED_STR) == ENGINE_SUCCESS,
                  "Failed tap notify.");
        }
        wait_for_flusher_to_settle(h, h1);
        wait_for_flusher_to_settle(h, h1);
        vals.clear();
        check(h1->get_stats(h, cookie, "checkpoint", strlen("checkpoint"), add_stats) == ENGINE_SUCCESS,
            "Failed to get stats.");
        int x = 0;
        for (; x < 10; ++x) {
            if (atoi(vals["vb_0:persisted_checkpoint_id"].c_str()) == i) {
                break;
            }
            usleep(10);
        }

        check(atoi(vals["vb_0:persisted_checkpoint_id"].c_str()) == i, "checkpoint did not persist after flusher runs");

        check(get_persisted_cpid_vb0_fromdb() == i, "vbucket_states table should be updated with latest persisted checkpoint_id");

        check(h1->tap_notify(h, cookie, eng_specific, sizeof(eng_specific),
                             1, 0, TAP_CHECKPOINT_END, 1, "", 0, 828, 0, 0, 0,
                             &checkpointId, sizeof(checkpointId), 0, DI_CKSUM_DISABLED_STR) == ENGINE_SUCCESS,
              "Failed tap notify.");
    }

    return SUCCESS;
}

static void switch_persistence(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, bool on) {

    std::string error;
    protocol_binary_request_header *pkt;

    if (on) {
        error = "Unable to start persistence";
        pkt = create_packet(CMD_START_PERSISTENCE, "", "");
    } else {
        error = "Unable to stop persistence";
        pkt = create_packet(CMD_STOP_PERSISTENCE, "", "");
    }

    check(h1->unknown_command(h, NULL, pkt, add_response, NULL) == ENGINE_SUCCESS,
          error.c_str());

    check(last_status == PROTOCOL_BINARY_RESPONSE_SUCCESS,
          error.c_str());
}

static enum test_result test_ep_overhead_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    int overhead1, overhead2;

    switch_persistence(h, h1, false);
    wait_for_stat_match(h, h1, "ep_flusher_state", "paused");

    overhead1 = get_int_stat(h, h1, "ep_overhead");
    for (size_t i=0; i<100; i++) {
        std::stringstream ss;
        ss<<"key_"<<i;
        item *itm = NULL;
        check(store(h, h1, NULL, OPERATION_SET, ss.str().c_str(), "somevalue", &itm) == ENGINE_SUCCESS,
              "Failed set.");
        h1->release(h, NULL, itm);
    }

    overhead2 = get_int_stat(h, h1, "ep_overhead");
    check(overhead2 > overhead1, "memory overhead should increase");
    switch_persistence(h, h1, true);

    wait_for_stat_match(h, h1, "ep_flusher_state", "running");
    wait_for_flusher_to_settle(h, h1);
    overhead1 = get_int_stat(h, h1, "ep_overhead");
    check(overhead1 < overhead2, "memory overhead should come down");

    return SUCCESS;
}

static enum test_result test_evict_bgfetch(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    size_t j;

    for (j=0; j<50000; j++) {
        std::stringstream sk, sv;
        sk<<"key_"<<j;
        sv<<"val_"<<j;
        item *i = NULL;

        check(store(h, h1, NULL, OPERATION_SET, sk.str().c_str(), sv.str().c_str(), &i) == ENGINE_SUCCESS,
              "Failed set.");
    }

    wait_for_flusher_to_settle(h, h1);
    for (j=0; j<50000; j++) {
        std::stringstream sk;
        sk<<"key_"<<j;
        evict_key(h, h1, sk.str().c_str());
    }

    for (j=0; j<50000; j++) {
        std::stringstream sk, sv;
        sk<<"key_"<<j;
        sv<<"val_"<<j;
        check(check_key_value(h, h1, sk.str().c_str(), sv.str().c_str(), sv.str().length()) == SUCCESS,
                "key - value mismatch or not found");
    }

    check(get_int_stat(h, h1, "ep_bg_fetched") == 50000, "Bgfetch count should be 50000");

    return SUCCESS;
}

static enum test_result test_kvstore_flusher_count(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    vals.clear();
    check(h1->get_stats(h, NULL, "kvstore", strlen("kvstore"), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    int kvstore_count = atoi(vals["num_kvstores"].c_str());
    check(kvstore_count == 3, "Number of kvstores should be 3");

    check(h1->get_stats(h, NULL, 0, 0, add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");

    for (int i = 0; i < kvstore_count; i++) {
        std::stringstream ss;
        ss<<"ep_flusher_state_"<<i;
        check(vals.find(ss.str()) != vals.end(), "Expects flusher to be present");
    }

    return SUCCESS;

}

static enum test_result test_persistence_flusher_states(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    vals.clear();
    check(h1->get_stats(h, NULL, "kvstore", strlen("kvstore"), add_stats) == ENGINE_SUCCESS,
          "Failed to get stats.");
    int kvstore_count = atoi(vals["num_kvstores"].c_str());
    switch_persistence(h, h1, false);

    for (int i = 0; i < kvstore_count; i++) {
        std::stringstream ss;
        ss<<"ep_flusher_state_"<<i;
        wait_for_stat_match(h, h1, ss.str(), "paused");
    }

    switch_persistence(h, h1, true);
    for (int i = 0; i < kvstore_count; i++) {
        std::stringstream ss;
        ss<<"ep_flusher_state_"<<i;
        wait_for_stat_match(h, h1, ss.str(), "running");
    }

    return SUCCESS;
}

static void shuffle_keys(std::vector<std::string> &keys) {
    for (size_t i = 0; i < keys.size(); i++) {
        size_t pos = rand() % (i+1);
        std::string tmp = keys.at(i);
        keys.at(i) = keys.at(pos);
        keys.at(pos) = tmp;
    }
}

static void* random_order_set_thread(void *arg) {
    random_set_key_thread_params *params = static_cast<random_set_key_thread_params *>(arg);
    item *itm = NULL;
    std::vector<std::string> keys = *(params->keys);
    shuffle_keys(keys);

    std::vector<std::string>::iterator it = keys.begin();
    for (; it != keys.end(); it++) {
        check(
              store(params->h, params->h1, NULL, OPERATION_SET,
                    (*it).c_str(), "value", &itm, 0, 0) == ENGINE_SUCCESS,
              "Thread failed to store an item");
        (params->h1)->release(params->h, NULL, itm);
        itm = NULL;
    }

    return NULL;
}

static enum test_result test_conc_set_mkv(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {

    const int n_threads = 10;
    pthread_t threads[n_threads];
    std::vector<std::string> keylist;
    for (int i = 0; i < 10000; i++) {
        std::stringstream ss;
        ss<<"key_"<<i;
        keylist.push_back(ss.str());
    }

    random_set_key_thread_params hp = {h, h1, &keylist};

    srand(time(NULL));
    for (int i = 0; i < n_threads; i++) {
        int r = pthread_create(&threads[i], NULL, random_order_set_thread, &hp);
        assert(r == 0);
    }

    for (int i = 0; i < n_threads; i++) {
        void *trv = NULL;
        int r = pthread_join(threads[i], &trv);
        assert(r == 0);
    }
    wait_for_flusher_to_settle(h, h1);

    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              "kvstore_config_file=t/kv_multikv.json",
                              true, false);

    check(0 == get_int_stat(h, h1, "ep_warmed_dups"), "There should not be any duplicates");
    check(10000 == get_int_stat(h, h1, "curr_items_tot"), "Expects curr_items to be 10000");

    return SUCCESS;
}

static enum test_result test_kvstore_mapping(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    int size[3] = {0, 0, 0};
    for (uint16_t ii = 0; ii < 300; ++ ii) {
        check(set_vbucket_state(h, h1, ii, vbucket_state_active), "Failed to activate vbucket");
    }

    check(h1->get_stats(h, NULL, "vbucket", 7, add_stats) == ENGINE_SUCCESS,
          "Failed to get the state of vbuckets");

    for (uint16_t ii = 0; ii < 300; ++ ii) {
        std::stringstream ss;
        int kvid;
        ss<<"vb_"<<ii;
        check(vals.find(ss.str()) != vals.end(), "VB info not found");
        std::string vbstate = vals[ss.str()];
        int curr_items;
        sscanf(vbstate.c_str(), "active curr_items %d kvstore %d", &curr_items, &kvid);
        size[kvid]++;
    }

    for (int i=0; i < 3; i++) {
        check(size[i] == 100, "Unbalanced vbucket to kvstore mapping");
    }
    return SUCCESS;
}

static enum test_result test_kvstore_online_offline(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_active), "Failed to activate vbucket 1");
    check(set_vbucket_state(h, h1, 2, vbucket_state_active), "Failed to activate vbucket 2");
    item *i = NULL;
    check(store(h, h1, NULL, OPERATION_SET, "key1", "somevalue", &i,
                0, 1) == ENGINE_SUCCESS,
          "Failed set in vb1.");
    h1->release(h, NULL, i);

    check(store(h, h1, NULL, OPERATION_SET, "key2", "somevalue", &i,
                0, 1) == ENGINE_SUCCESS,
          "Failed set in vb1.");
    h1->release(h, NULL, i);
    wait_for_flusher_to_settle(h, h1);

    evict_key(h, h1,"key1", 1);
    check(get_int_stat(h, h1, "ep_num_non_resident") == 1, "key1 should be evicted");
    vals.clear();
    check(h1->get_stats(h, NULL, "kvstore", strlen("kvstore"), add_stats) == ENGINE_SUCCESS,
          "Failed to get kvstore stats.");
    check(vals["kvstore1:status"] == "online", "Expects kvstore1 to be online");

    check(set_flush_param(h, h1, "kvstore_offline", "0") == true, "Unable to make kvstore offline");
    check(set_vbucket_state(h, h1, 3, vbucket_state_active), "Failed to activate vbucket 3");
    check(set_flush_param(h, h1, "kvstore_offline", "1") == true, "Unable to make kvstore offline");
    check(set_flush_param(h, h1, "kvstore_offline", "2") == true, "Unable to make kvstore offline");


    vals.clear();
    check(h1->get_stats(h, NULL, "kvstore", strlen("kvstore"), add_stats) == ENGINE_SUCCESS,
          "Failed to get kvstore stats.");
    check(vals["kvstore1:status"] == "offline", "Expects kvstore1 to be offline");

    check(set_vbucket_state(h, h1, 4, vbucket_state_active) == false, "vbucket activation should fail if kvstores are not available");
    check(verify_vb_key(h, h1, "key1", 1) == ENGINE_TMPFAIL, "BGfetch should fail");
    check(verify_vb_key(h, h1, "key2", 1) == ENGINE_SUCCESS, "Get from memory should not fail");

    check(set_vbucket_state(h, h1, 1, vbucket_state_dead), "Failed to deactivate vbucket 1");
    check(set_flush_param(h, h1, "kvstore_online", "0") == true, "Unable to make kvstore online");
    check(set_vbucket_state(h, h1, 5, vbucket_state_active), "Failed to activate vbucket 5");

    vals.clear();
    check(h1->get_stats(h, NULL, "vbucket", 7, add_stats) == ENGINE_SUCCESS,
          "Failed to get the state of vbuckets");
    check(vals.find("vb_5") != vals.end(), "VB 5 info not found");
    std::string vbstate = vals["vb_5"];
    int kvid, curr_items;
    sscanf(vbstate.c_str(), "active curr_items %d kvstore %d", &curr_items, &kvid);
    check(kvid == 0, "vbucket 5 should be mapping to kvstore 0");
    check(vals.size() == 3, "Membase should be holding 3 vbuckets after kvstore online");
    check(verify_vb_key(h, h1, "key1", 1) == ENGINE_KEY_ENOENT, "key1 should not exist");

    check(store(h, h1, NULL, OPERATION_SET, "key3", "somevalue", &i,
                0, 5) == ENGINE_SUCCESS,
          "Failed set in vb5.");
    evict_key(h, h1,"key3", 5);
    check(verify_vb_key(h, h1, "key3", 5) == ENGINE_SUCCESS, "Get from memory should succeed");

    return SUCCESS;
}

static enum test_result test_tap_rcv_backfill_multivb(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    check(set_vbucket_state(h, h1, 1, vbucket_state_replica), "Unable to set vbucket state");;
    check(set_vbucket_state(h, h1, 2, vbucket_state_replica), "Unable to set vbucket state");;
    check(set_vbucket_state(h, h1, 3, vbucket_state_replica), "Unable to set vbucket state");;

    char eng_specific[64];
    uint32_t tap_flag;
    memset(eng_specific, 0, sizeof(eng_specific));
    const void *cookie = testHarness.create_cookie();
    check(h1->tap_notify(h, cookie, NULL, 0, 0, 0, TAP_CONSUMER, 0, "consumer", 8, 0, 0, 0, 0, NULL, 0, 0, 0) == ENGINE_SUCCESS,
            "Failed to register tap consumer");

    for (int vb = 1 ; vb < 4; vb++) {
        tap_flag = htonl(TAP_OPAQUE_ENABLE_CHECKPOINT_SYNC);
        check(h1->tap_notify(h, cookie, &tap_flag, sizeof(uint32_t),1,
                0, TAP_OPAQUE, 1, "", 0, 828, 0, 0, 0, "",0, vb, DI_CKSUM_DISABLED_STR) == ENGINE_SUCCESS,
            "Failed tap notify.");

        tap_flag = htonl(TAP_OPAQUE_INITIAL_VBUCKET_STREAM);
        check(h1->tap_notify(h, cookie, &tap_flag, sizeof(uint32_t),1,
                0, TAP_OPAQUE, 1, "", 0, 828, 0, 0, 0, "",0, vb, DI_CKSUM_DISABLED_STR) == ENGINE_SUCCESS,
            "Failed tap notify.");

        check(h1->get_stats(h, NULL, "checkpoint", strlen("checkpoint"), add_stats) == ENGINE_SUCCESS,
                "Failed to get stats.");
        std::stringstream ssvb;
        ssvb<<"vb_"<<vb<<":backfillphase";
        check(vals[ssvb.str()] == "true", "Expected backfill mode");

        for (size_t i = 0; i < 100; ++i) {
            std::stringstream ss;
            ss<<vb<<" key_"<<i;
            char *data = static_cast<char *>(malloc(i));
            memset(data, 'x', i);
            check(h1->tap_notify(h, cookie, eng_specific, sizeof(eng_specific),
                                 1, 0, TAP_MUTATION, 1, ss.str().c_str(), ss.str().length(), 828, std::numeric_limits<uint32_t>::max(), 0, 0,
                                 data, i, vb, DI_CKSUM_DISABLED_STR) == ENGINE_SUCCESS,
                  "Failed tap notify.");
            free(data);
        }

        tap_flag = htonl(TAP_OPAQUE_CLOSE_BACKFILL);
        check(h1->tap_notify(h, cookie, &tap_flag, sizeof(uint32_t),1,
                0, TAP_OPAQUE, 1, "", 0, 828, 0, 0, 0, "",0, vb, DI_CKSUM_DISABLED_STR) == ENGINE_SUCCESS,
            "Failed tap notify.");

        check(h1->get_stats(h, NULL, "checkpoint", strlen("checkpoint"), add_stats) == ENGINE_SUCCESS,
                "Failed to get stats.");
        check(vals[ssvb.str()] == "false", "Expected checkpoint mode");

        wait_for_flusher_to_settle(h, h1);

    }
    check(get_int_stat(h, h1, "curr_items_tot") == 300, "Expected 300 items");
    testHarness.destroy_cookie(cookie);

    return SUCCESS;
}


static int do_fill(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, uint32_t keys,
                    uint32_t blobsize) {
    uint32_t i = 0;
    char *value = NULL;
    char keyname[32];

    printf ("Starting fill for %d keys , blobsize = %d\n", keys, blobsize);
    value = (char *)malloc (blobsize * sizeof(char));
    memset(value, 3, blobsize - 1);
    value[blobsize] = '\0';


    int max_tries = 100;
    ENGINE_ERROR_CODE ret=ENGINE_SUCCESS;
    time_t end_time(0), start_time = time(NULL);
    int tmp_fails = 0;
    for (; i < keys; ++i) {
        snprintf(keyname, 32, "key-%d", i);
        int num_tries = 0;
        while (((ret = store(h, h1, NULL, OPERATION_SET, keyname, value, NULL, 0, 0, 0))
                 == ENGINE_TMPFAIL) && num_tries <= max_tries) {
            printf ("Set for %s failed with tmpooom. Retrying after 1 sec\n", keyname);
            tmp_fails++;
            sleep(1);
            if (num_tries > 60) {
                printf("Flusher todo: %d\n", get_int_stat(h, h1, "flusher_todo"));
                printf("ep_queue_size = %d\n", get_int_stat(h, h1, "ep_queue_size"));
                printf("ep_uncommitted_items = %d\n", get_int_stat(h, h1, "ep_uncommitted_items"));
                printf("Eviction stats:\n");
                vals.clear();
                check(h1->get_stats(h, NULL, "eviction", 8, add_stats) == ENGINE_SUCCESS,
                "Failed to get stats.");

                std::map<std::string, std::string>::iterator it = vals.begin();
                for (; it!=vals.end(); it++) {
                    std::cout<<(*it).first<<" = "<<(*it).second<<std::endl;
                }
            }
        }
        if (ret != ENGINE_SUCCESS) {
            printf("\nStore() returncode = %d\n", ret);
            printf("Flusher todo: %d\n", get_int_stat(h, h1, "flusher_todo"));
            printf("Eviction stats:\n");
            vals.clear();
            check(h1->get_stats(h, NULL, "eviction", 8, add_stats) == ENGINE_SUCCESS,
            "Failed to get stats.");

            std::map<std::string, std::string>::iterator it = vals.begin();
            for (; it!=vals.end(); it++) {
                std::cout<<(*it).first<<" = "<<(*it).second<<std::endl;
            }
            break;
        }

        if (i % 1000000 == 0) {
            end_time = time(NULL);
            printf("Filled up to %d items - Last 1M took %lu seconds\n", i, (long unsigned int)end_time-start_time);
            start_time = end_time;
        }

    }
    printf("Total tmp failures were %d\n", tmp_fails);
    if (ret != ENGINE_SUCCESS) {
        printf ("Set failed with error %d. Nothing to do\n", ret);
        return -1;
    }

    printf("Fill completed. Waiting for flusher to settle\n");
    wait_for_flusher_to_settle(h, h1);

    uint32_t actual_keys = get_int_stat(h, h1, "curr_items");
    if (actual_keys != keys) {
        printf("Store of %d keys failed. Stored only %d\n", keys, actual_keys);
    } else {
        printf ("Store Success\n");
        printf(" Non resident = %d\n", get_int_stat(h, h1, "ep_num_non_resident"));
    }
    free(value);
    return 0;
}

static int do_warmup(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, uint32_t keys) {
    uint32_t i = 0;
    char keyname[32];

    printf ("Starting warmup for %d keys\n", keys);
    item *itm = NULL;

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    for (; i < keys && ret == ENGINE_SUCCESS; ++i) {
        snprintf(keyname, 32, "key-%d", i);
        ret = h1->get(h, NULL, &itm, keyname, strlen(keyname), 0);
        h1->release(h, NULL, itm);
    }
    if (ret != ENGINE_SUCCESS) {
        printf ("Get failed with error %d. Nothing to do\n", ret);
        return -1;
    }

    if (i != keys) {
        printf("Warmup of %d keys failed. Performed get on only %d keys\n", keys, i);
    } else {
        printf ("Warmup on %d keys success\n", keys);
        printf(" Non resident = %d\n", get_int_stat(h, h1, "ep_num_non_resident"));
    }
    return 0;
}

static int run_pattern_load(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                BaseLoadPattern *generator, uint32_t blobsize, uint32_t set_r, uint32_t get_r) {

    std::cout<<"Blob size:"<<blobsize<<" set-get="<<set_r<<"/"<<get_r<<std::endl;
    char *value;
    item *it;

    value = (char *)malloc (blobsize * sizeof(char));
    memset(value, 3, blobsize - 1);
    value[blobsize] = '\0';

    while (1) {
        for (uint32_t i=0; i<set_r; i++) {
            std::string key;
            if (generator->getNextKey(key) > 0) {
                check(store(h, h1, NULL, OPERATION_SET, key.c_str(), value, NULL, 0, 0, 0) == ENGINE_SUCCESS, "Failed to store key");
            } else {
                return 0;
            }
        }

        for (uint32_t i=0; i<get_r; i++) {
            std::string key;
            if (generator->getNextKey(key) > 0) {
                check(h1->get(h, NULL, &it, key.c_str(), key.length(), 0) == ENGINE_SUCCESS, "Failed to read out key");
                h1->release(h, NULL, it);
            } else {
                return 0;
            }
        }

    }

}

static BaseLoadPattern *get_load_pattern(std::string type, uint32_t numkeys,
        uint64_t timeout = 0, uint32_t maxops = 0, int skey = 0, int param1 = 1, int param2 = 1, int param3 = 1) {
    if (type == "even") {
        return new EvenKeysPattern(numkeys, timeout, maxops, skey);
    } else if (type == "polynomial") {
        return new PolynomialPattern(numkeys, timeout, maxops, skey, param1, param2, param3);
    } else if (type == "random") {
        return new RandomPattern(numkeys, timeout, maxops, skey, param1);
    } else {
        std::cout<<"Invalid load pattern"<<std::endl;
        assert(0);
    }

}

static enum test_result run_flusher_perf_test(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    /* Generate these from the input parameters */
    std::map<std::string, std::string> *conf = parsePerfTestConfig();
    uint32_t num_keys = atoi((*conf)["keys"].c_str());
    uint32_t blob_size = atoi((*conf)["blobsize"].c_str());
    std::string load_pattern = (*conf)["load_pattern"];
    uint64_t load_timeout = atoll((*conf)["load_timeout"].c_str());
    uint32_t load_opsmax = atol((*conf)["load_opsmax"].c_str());
    uint32_t load_ratio_sets = atoi((*conf)["load_ratio_sets"].c_str());
    uint32_t load_ratio_gets = atoi((*conf)["load_ratio_gets"].c_str());
    uint32_t run_load = atoi((*conf)["run_load"].c_str());
    uint64_t max_size = atoll((*conf)["max_size"].c_str());
    int max_evict_entries = atoi((*conf)["max_evict_entries"].c_str());
    std::string kvs_config = (*conf)["kvs_config"];
    std::string other_config;
    int load_param1(1), load_param2(1), load_param3(1), load_start_key(0);
    int load_num_keys = num_keys;

    if ((*conf).find("load_param1") != (*conf).end()) {
        load_param1 = atoi((*conf)["load_param1"].c_str());
    }

    if ((*conf).find("load_param2") != (*conf).end()) {
        load_param2 = atoi((*conf)["load_param2"].c_str());
    }

    if ((*conf).find("load_param3") != (*conf).end()) {
        load_param3 = atoi((*conf)["load_param3"].c_str());
    }

    if ((*conf).find("load_start_key") != (*conf).end()) {
        load_start_key = atoi((*conf)["load_start_key"].c_str());
    }

    if ((*conf).find("load_num_keys") != (*conf).end()) {
        load_num_keys = atoi((*conf)["load_num_keys"].c_str());
    }

    if ((*conf).find("other_config") != (*conf).end()) {
        other_config = (*conf)["other_config"];
    }

    // Generate new config and restart the engine
    int warmup;
    std::string newConf = gen_new_config(h, h1, warmup, num_keys, blob_size, max_size, 1, max_evict_entries, kvs_config, other_config);
    testHarness.reload_engine(&h, &h1,
                              testHarness.engine_path,
                              newConf.c_str(),
                              true, false);


    BlockTimerSimple tt;
    check(do_fill(h, h1, num_keys, blob_size) == 0, "Filling failed");
    printf("Fill took %llu seconds\n", (unsigned long long int)tt.getElapsedTime());
    tt.reset();
    check(do_warmup(h, h1, warmup) == 0, "Warmup failed");
    printf("Warmup took %llu seconds\n", (unsigned long long int)tt.getElapsedTime());
    if (run_load) {
        BaseLoadPattern *pattern = get_load_pattern(load_pattern, load_num_keys, load_timeout, load_opsmax, load_start_key, load_param1, load_param2, load_param3);
        tt.reset();
        check(run_pattern_load(h, h1, pattern, blob_size, load_ratio_sets, load_ratio_gets) == 0, "Pattern based loading failed");
        printf("Performance run took %llu seconds\n", (unsigned long long int)tt.getElapsedTime());
        delete pattern;
    }

    delete conf;
    return SUCCESS;
}


MEMCACHED_PUBLIC_API
engine_test_t* get_tests(void) {

    static engine_test_t tests[]  = {
        {"validate engine handle", test_validate_engine_handle, NULL, teardown,
         "kvstore_config_file=t/kv_single_memory.json"},
        // basic tests
        {"test alloc limit", test_alloc_limit, NULL, teardown, NULL},
        {"test init failure", test_init_fail, NULL, teardown, NULL},
        {"test total memory limit", test_memory_limit, NULL, teardown,
         "max_size=7875000;ht_locks=1;ht_size=3;chk_remover_stime=1;chk_period=60;mutation_mem_threshold=0.9"},
        {"test max_size changes", test_max_size_settings, NULL, teardown,
         "max_size=1000;ht_locks=1;ht_size=3"},
        {"test whitespace dbname", test_whitespace_db, NULL, teardown,
         "kvstore_config_file=t/kv_white.json;ht_locks=1;ht_size=3"},
        {"test db shards", test_db_shards, NULL, teardown,
         "kvstore_config_file=t/kv_multi_5.json"},
        {"test single db strategy", test_single_db_strategy,
         NULL, teardown, "kvstore_config_file=t/kv_single_memory.json"},
        {"test single in-memory db strategy", test_single_db_strategy,
         NULL, teardown, "kvstore_config_file=t/kv_single_memory.json"},
        {"get miss", test_get_miss, NULL, teardown, NULL},
        {"set", test_set, NULL, teardown, NULL},
        {"set with cksum", test_set_with_cksum, NULL, teardown, NULL},
        {"concurrent set", test_conc_set, NULL, teardown, NULL},
        {"test concurrent sets multikv", test_conc_set_mkv, NULL, teardown, "kvstore_config_file=t/kv_multikv.json"},
        {"set+get hit", test_set_get_hit, NULL, teardown, NULL},
        {"set+get hit with max_txn_size", test_set_get_hit, NULL, teardown,
         "ht_locks=1;ht_size=3;max_txn_size=10"},
        {"getl", test_getl, NULL, teardown, NULL},
        {"unl",  test_unl, NULL, teardown, NULL},
        {"set+get hit (bin)", test_set_get_hit_bin, NULL, teardown, NULL},
        {"set+change flags", test_set_change_flags, NULL, teardown, NULL},
        {"add", test_add, NULL, teardown, NULL},
        {"cas", test_cas, NULL, teardown, NULL},
        {"append", test_append, NULL, teardown, NULL},
        {"prepend", test_prepend, NULL, teardown, NULL},
        {"replace", test_replace, NULL, teardown, NULL},
        {"incr miss", test_incr_miss, NULL, teardown, NULL},
        {"incr", test_incr, NULL, teardown, NULL},
        {"incr with default", test_incr_default, NULL, teardown, NULL},
        {"incr expiry", test_bug2799, NULL, teardown, NULL},
        {"test touch", test_touch, NULL, teardown, NULL},
        {"test gat", test_gat, NULL, teardown, NULL},
        {"test gatq", test_gatq, NULL, teardown, NULL},
        {"delete", test_delete, NULL, teardown, NULL},
        {"set/delete", test_set_delete, NULL, teardown, NULL},
        {"delete/set/delete", test_delete_set, NULL, teardown, NULL},
        {"retain rowid over a soft delete", test_bug2509,
         NULL, teardown, NULL},
        {"vbucket deletion doesn't affect new data", test_bug2761,
         NULL, teardown, NULL},
        {"vbucket deletion doesn't affect new data (table per vbucket)", test_bug2761,
         NULL, teardown, "kvstore_config_file=t/kv_multimtvb.json"},
        {"start transaction failure handling", test_bug2830, NULL, teardown,
         "ht_size=13;ht_locks=7;kvstore_config_file=t/kv_multi_1.json"},
        {"non-resident decrementers", test_mb3169, NULL, teardown, NULL},
        {"flush", test_flush, NULL, teardown, NULL},
        {"flush with stats", test_flush_stats, NULL, teardown, "chk_remover_stime=1;chk_period=60"},
        {"flush multi vbuckets", test_flush_multiv, NULL, teardown, NULL},
        {"flush multi vbuckets single mt", test_flush_multiv, NULL, teardown,
         "kvstore_config_file=t/kv_singlemt.json;max_vbuckets=16;ht_size=7;ht_locks=3"},
        {"flush multi vbuckets multi mt", test_flush_multiv, NULL, teardown,
         "kvstore_config_file=t/kv_multimt.json;max_vbuckets=16;ht_size=7;ht_locks=3"},
        {"flush multi vbuckets multi mt vb", test_flush_multiv, NULL, teardown,
         "kvstore_config_file=t/kv_multimtvb.json;max_vbuckets=16;ht_size=7;ht_locks=3"},
        {"expiry", test_expiry, NULL, teardown, NULL},
        {"expiry_loader", test_expiry_loader, NULL, teardown, NULL},
        {"expiry_flush", test_expiry_flush, NULL, teardown, NULL},
        {"expiry_duplicate_warmup", test_bug3454, NULL, teardown, NULL},
        {"expiry_no_items_warmup", test_bug3522, NULL, teardown, NULL},
        // Stats tests
        {"stats", test_stats, NULL, teardown, NULL},
        {"io stats", test_io_stats, NULL, teardown, NULL},
        {"bg stats", test_bg_stats, NULL, teardown, NULL},
        {"mem stats", test_mem_stats, NULL, teardown, "chk_remover_stime=1;chk_period=60"},
        {"stats key", test_key_stats, NULL, teardown, NULL},
        {"stats vkey", test_vkey_stats, NULL, teardown, NULL},
        {"warmup stats", test_warmup_stats, NULL, teardown, NULL},
        {"stats curr_items", test_curr_items, NULL, teardown, NULL},
        // kvstore config tests
        {"kvstore config: config generation test", test_kvconf_basic, NULL, teardown, NULL},
        {"kvstore config: multiple kvstore config", test_kvconf_multikv, NULL, teardown, "kvstore_config_file=t/kv_multikv.json"},
        {"kvstore config: unique dbname failure", test_kvconf_fail, NULL, teardown, NULL},
        // eviction
        {"value eviction", test_value_eviction, NULL, teardown, NULL},
        {"eviction: lru evictions", test_lru_eviction, NULL, teardown, 
         "eviction_policy=lru;max_size=10000000;lru_rebuild_stime=1;eviction_headroom=1000;chk_period=60;ht_size=3;ht_locks=1"},
        {"eviction: lru queue", test_lru_queue, NULL, teardown, "eviction_policy=lru"},
        {"eviction: pause", test_eviction_pause, NULL, teardown, NULL},
        {"eviction: switch", test_eviction_switch, NULL, teardown, NULL},
        // min_data_age
        {"min_data_age tests: basic", test_min_data_age, NULL, teardown, "min_data_age=20"},
        {"min_data_age tests: queued_time tap propogation", test_queuedtime_tap, NULL, teardown, "min_data_age=12"},
        // duplicate items on disk
        {"duplicate items on disk", test_duplicate_items_disk, NULL, teardown, NULL},
        // tap tests
        {"set tap param", test_set_tap_param, NULL, teardown, NULL},
        {"tap_noop_interval default config", test_tap_noop_config_default,
         NULL, teardown, NULL },
        {"tap_noop_interval config", test_tap_noop_config, NULL, teardown,
         "tap_noop_interval=10"},
        {"tap_noop_interval config compat", test_tap_noop_config_deprecated,
         NULL, teardown,
         "tap_idle_timeout=30"},
        {"tap receiver mutation", test_tap_rcvr_mutate, NULL, teardown, NULL},
        {"tap receiver checkpoint start/end", test_tap_rcvr_checkpoint, NULL, teardown, NULL},
        {"tap receiver mutation (dead)", test_tap_rcvr_mutate_dead,
         NULL, teardown, NULL},
        {"tap receiver mutation (pending)", test_tap_rcvr_mutate_pending,
         NULL, teardown, NULL},
        {"tap receiver mutation (replica)", test_tap_rcvr_mutate_replica,
         NULL, teardown, NULL},
        {"tap receiver delete", test_tap_rcvr_delete, NULL, teardown, NULL},
        {"tap receiver delete (dead)", test_tap_rcvr_delete_dead,
         NULL, teardown, NULL},
        {"tap receiver delete (pending)", test_tap_rcvr_delete_pending,
         NULL, teardown, NULL},
        {"tap receiver delete (replica)", test_tap_rcvr_delete_replica,
         NULL, teardown, NULL},
        {"tap stream", test_tap_stream, NULL, teardown, NULL},
        {"tap agg stats", test_tap_agg_stats, NULL, teardown, NULL},
        {"tap takeover (with concurrent mutations)", test_tap_takeover, NULL, teardown, NULL},
        {"tap filter stream", test_tap_filter_stream, NULL, teardown,
         "tap_keepalive=100;ht_size=129;ht_locks=3"},
        {"tap default config", test_tap_default_config, NULL,
         teardown, NULL },
        {"tap config", test_tap_config, NULL, teardown,
         "tap_backoff_period=0.05;tap_ack_interval=10;tap_ack_window_size=2;tap_ack_grace_period=10"},
        {"tap acks stream", test_tap_ack_stream, NULL, teardown,
         "tap_keepalive=100;ht_size=129;ht_locks=3;tap_backoff_period=0.05;chk_max_items=500"},
        {"tap implicit acks stream", test_tap_implicit_ack_stream, NULL, teardown,
         "tap_keepalive=100;ht_size=129;ht_locks=3;tap_backoff_period=0.05;tap_ack_initial_sequence_number=4294967290;chk_max_items=500"},
        {"tap notify", test_tap_notify, NULL, teardown,
         "max_size=1048576"},
        {"tap backfill: receive and persist data", test_tap_rcv_backfill, NULL, teardown,
        "inconsistent_slave_chk=true"},
        {"tap backfill multivb: receive data from 3 vbs", test_tap_rcv_backfill_multivb, NULL, teardown,
        "kvstore_map_vbuckets=true"},
        // restart tests
        {"test restart", test_restart, NULL, teardown, "kvstore_config_file=t/kv_multikv.json"},
        {"set+get+restart+hit (bin)", test_restart_bin_val, NULL, teardown, NULL},
        {"flush+restart", test_flush_restart, NULL, teardown, NULL},
        {"flush multiv+restart", test_flush_multiv_restart, NULL, teardown, NULL},
        {"test kill -9 bucket", test_kill9_bucket, NULL, teardown, NULL},
        // disk>RAM tests
        {"verify not multi dispatcher", test_not_multi_dispatcher_conf, NULL, teardown,
         NULL},
        {"disk>RAM golden path", test_disk_gt_ram_golden, NULL, teardown,
         "chk_remover_stime=1;chk_period=60"},
        {"disk>RAM paged-out rm", test_disk_gt_ram_paged_rm, NULL, teardown,
         "chk_remover_stime=1;chk_period=60"},
        {"disk>RAM update paged-out", test_disk_gt_ram_update_paged_out, NULL,
         teardown, NULL},
        {"disk>RAM delete paged-out", test_disk_gt_ram_delete_paged_out, NULL,
         teardown, NULL},
        {"disk>RAM paged-out incr", test_disk_gt_ram_incr, NULL,
         teardown, NULL},
        {"disk>RAM set bgfetch race", test_disk_gt_ram_set_race, NULL,
         teardown, NULL},
        {"disk>RAM incr bgfetch race", test_disk_gt_ram_incr_race, NULL,
         teardown, NULL},
        {"disk>RAM delete bgfetch race", test_disk_gt_ram_rm_race, NULL,
         teardown, NULL},
        // disk>RAM tests with WAL
        {"verify multi dispatcher", test_multi_dispatcher_conf, NULL, teardown,
         MULTI_DISPATCHER_CONFIG},
        {"verify multi dispatcher override",
         test_not_multi_dispatcher_conf, NULL, teardown,
         MULTI_DISPATCHER_CONFIG ";concurrentDB=false"},
        {"disk>RAM golden path (wal)", test_disk_gt_ram_golden, NULL, teardown,
         MULTI_DISPATCHER_CONFIG},
        {"disk>RAM paged-out rm (wal)", test_disk_gt_ram_paged_rm, NULL, teardown,
         MULTI_DISPATCHER_CONFIG},
        {"disk>RAM update paged-out (wal)", test_disk_gt_ram_update_paged_out, NULL,
         teardown, MULTI_DISPATCHER_CONFIG},
        {"disk>RAM delete paged-out (wal)", test_disk_gt_ram_delete_paged_out, NULL,
         teardown, MULTI_DISPATCHER_CONFIG},
        {"disk>RAM paged-out incr (wal)", test_disk_gt_ram_incr, NULL,
         teardown, MULTI_DISPATCHER_CONFIG},
        {"disk>RAM set bgfetch race (wal)", test_disk_gt_ram_set_race, NULL,
         teardown, MULTI_DISPATCHER_CONFIG},
        {"disk>RAM incr bgfetch race (wal)", test_disk_gt_ram_incr_race, NULL,
         teardown, MULTI_DISPATCHER_CONFIG},
        {"disk>RAM delete bgfetch race (wal)", test_disk_gt_ram_rm_race, NULL,
         teardown, MULTI_DISPATCHER_CONFIG},
        // vbucket negative tests
        {"vbucket incr (dead)", test_wrong_vb_incr, NULL, teardown, NULL},
        {"vbucket incr (pending)", test_vb_incr_pending, NULL, teardown, NULL},
        {"vbucket incr (replica)", test_vb_incr_replica, NULL, teardown, NULL},
        {"vbucket get (dead)", test_wrong_vb_get, NULL, teardown, NULL},
        {"vbucket get (pending)", test_vb_get_pending, NULL, teardown, NULL},
        {"vbucket get (replica)", test_vb_get_replica, NULL, teardown, NULL},
        {"vbucket getl (dead)", NULL, NULL, teardown, NULL},
        {"vbucket getl (pending)", NULL, NULL, teardown, NULL},
        {"vbucket getl (replica)", NULL, NULL, teardown, NULL},
        {"vbucket set (dead)", test_wrong_vb_set, NULL, teardown, NULL},
        {"vbucket set (pending)", test_vb_set_pending, NULL, teardown, NULL},
        {"vbucket set (replica)", test_vb_set_replica, NULL, teardown, NULL},
        {"vbucket replace (dead)", test_wrong_vb_replace, NULL, teardown, NULL},
        {"vbucket replace (pending)", test_vb_replace_pending, NULL, teardown, NULL},
        {"vbucket replace (replica)", test_vb_replace_replica, NULL, teardown, NULL},
        {"vbucket add (dead)", test_wrong_vb_add, NULL, teardown, NULL},
        {"vbucket add (pending)", test_vb_add_pending, NULL, teardown, NULL},
        {"vbucket add (replica)", test_vb_add_replica, NULL, teardown, NULL},
        {"vbucket cas (dead)", test_wrong_vb_cas, NULL, teardown, NULL},
        {"vbucket cas (pending)", test_vb_cas_pending, NULL, teardown, NULL},
        {"vbucket cas (replica)", test_vb_cas_replica, NULL, teardown, NULL},
        {"vbucket append (dead)", test_wrong_vb_append, NULL, teardown, NULL},
        {"vbucket append (pending)", test_vb_append_pending, NULL, teardown, NULL},
        {"vbucket append (replica)", test_vb_append_replica, NULL, teardown, NULL},
        {"vbucket prepend (dead)", test_wrong_vb_prepend, NULL, teardown, NULL},
        {"vbucket prepend (pending)", test_vb_prepend_pending, NULL, teardown, NULL},
        {"vbucket prepend (replica)", test_vb_prepend_replica, NULL, teardown, NULL},
        {"vbucket del (dead)", test_wrong_vb_del, NULL, teardown, NULL},
        {"vbucket del (pending)", test_vb_del_pending, NULL, teardown, NULL},
        {"vbucket del (replica)", test_vb_del_replica, NULL, teardown, NULL},
        // Vbucket management tests
        {"no vb0 at startup", test_novb0, NULL, teardown, "vb0=false"},
        {"test vbucket get", test_vbucket_get, NULL, teardown, NULL},
        {"test vbucket get missing", test_vbucket_get_miss, NULL, teardown, NULL},
        {"test vbucket create", test_vbucket_create, NULL, teardown, NULL},
        {"test vbucket destroy", test_vbucket_destroy, NULL, teardown, NULL},
        {"test vbucket destroy (multitable)", test_vbucket_destroy, NULL, teardown,
         "kvstore_config_file=t/kv_multimtvb.json;max_vbuckets=16;ht_size=7;ht_locks=3"},
        {"test vbucket destroy stats", test_vbucket_destroy_stats,
         NULL, teardown, "chk_remover_stime=1;chk_period=60"},
        {"test vbucket destroy restart", test_vbucket_destroy_restart,
         NULL, teardown, NULL},
        {"sync bad flags", test_sync_bad_flags, NULL, teardown, NULL},
        // Temporarily disabled (MB-3817)
        // {"sync persistence", test_sync_persistence, NULL, teardown, NULL},
        {"sync mutation", test_sync_mutation, NULL, teardown, NULL},
        {"sync replication", test_sync_replication, NULL, teardown, NULL},
        // Temporarily disabled (MB-3817)
        // {"sync persistence or replication", test_sync_persistence_or_replication, NULL, teardown, NULL},
        // {"sync persistence and replication", test_sync_persistence_and_replication, NULL, teardown, NULL},
        {"sync timeout", test_sync_timeout, NULL, teardown, NULL},

        // checkpoint tests
        {"checkpoint: get last closed checkpoint Id", test_get_last_closed_checkpoint_id,
         NULL, teardown, "chk_max_items=500"},
        {"checkpoint: validate checkpoint config params", test_validate_checkpoint_params,
         NULL, teardown, NULL},

        // Restore tests
        {"restore: not enabled", test_restore_not_enabled, NULL, teardown, NULL},
        {"restore: no such file", test_restore_no_such_file, NULL, teardown,
         "restore_mode=true"},
        {"restore: invalid file", test_restore_invalid_file, NULL, teardown,
         "restore_mode=true"},
        {"restore: data miss during restore", test_restore_data_miss, NULL, teardown,
         "restore_mode=true"},
        {"restore: no data in there", test_restore_clean, NULL, teardown,
         "restore_mode=true"},
        {"restore: no data in there (with partial vbucket list)",
         test_restore_clean_vbucket_subset, NULL, teardown,
         "restore_mode=true"},
        {"restore: with keys", test_restore_with_data, NULL, teardown,
         "restore_mode=true"},
#ifdef future
        {"restore: multiple incrementalfiles", test_restore_multi, NULL, teardown,
         "kvstore_config_file=t/kv_single_memory.json;restore_mode=true"},
#endif
        {"restore: vb0 mode with persistence", test_restore_vb0_persistence, NULL, teardown,
         "restore_mode=true"},

        {"persistence: vb0 checkpoint_id persistence (stats & vbucket_states table)", test_checkpoint_vb0_persistence, NULL, teardown,
        "inconsistent_slave_chk=true"},
        {"persistence: verify start/stop persistence, ep_overhead stats", test_ep_overhead_stats, NULL, teardown, NULL},
        {"persistence: flusher states", test_persistence_flusher_states, NULL, teardown, "kvstore_config_file=t/kv_multikv.json"},
        {"evict bgfetch", test_evict_bgfetch, NULL, teardown, "kvstore_config_file=t/kv_multikv.json"},
        {"kvstore: flusher count", test_kvstore_flusher_count, NULL, teardown, "kvstore_config_file=t/kv_multikv.json"},
        // Multi vbuckets support with kvstore
        {"multivbuckets: kvstore vb assignment", test_kvstore_mapping, NULL, teardown, "kvstore_config_file=t/kv_multikv.json;kvstore_map_vbuckets=true"},
        {"multivbuckets: kvstore online/offline", test_kvstore_online_offline, NULL, teardown, "kvstore_config_file=t/kv_multikv.json;kvstore_map_vbuckets=true"},
        // Flusher Perf tests
        {"flusher perf test", run_flusher_perf_test, NULL, teardown, 
        "max_size=400000000;eviction_headroom=10000000;ht_size=1572869;chk_period=60;ht_locks=10000"},
        {"test_bug_seg10443", test_bug_10443, NULL, teardown, "min_data_age=2;inconsistent_slave_chk=true"},
        {NULL, NULL, NULL, NULL, NULL}
    };
    return tests;
}

MEMCACHED_PUBLIC_API
bool setup_suite(struct test_harness *th) {
    testHarness = *th;
    return true;
}

}
