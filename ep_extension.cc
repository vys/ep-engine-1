/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ep_extension.h"
#include "crc32.hh"
#include <memcached/extension.h>

#define ITEM_LOCK_TIMEOUT       15    /* 15 seconds */
#define MAX_KEY_LEN             250   /* maximum permissible key length */


static GetlExtension* getExtension(const void* cookie)
{
    return reinterpret_cast<GetlExtension*>(const_cast<void *>(cookie));
}

extern "C" {

    static const char *ext_get_name(const void *cmd_cookie) {
        (void) cmd_cookie;
        return "getl";
    }

    static ENGINE_ERROR_CODE ext_execute(const void *cmd_cookie, const void *cookie,
            int argc, token_t *argv,
            RESPONSE_HANDLER_T response_handler) {
        (void) cmd_cookie;

        if (strncmp(argv[0].value, "getl", argv[0].length) == 0) {
            return getExtension(cmd_cookie)->executeGetl(argc, argv,
                                                         (void *)cookie,
                                                         response_handler);
        } else {
            return getExtension(cmd_cookie)->executeUnl(argc, argv,
                                                        (void *)cookie,
                                                        response_handler);
        }
    }

    static bool ext_accept(const void *cmd_cookie, void *cookie,
            int argc, token_t *argv, size_t *ndata,
            char **ptr) {
        (void) cmd_cookie;
        (void) cookie;
        (void) argc;
        (void) ndata;
        (void) ptr;
        // accept both getl (get locked) and unl (unlock)commands
        return argc >= 1 && (strncmp(argv[0].value, "getl", argv[0].length) == 0 ||
                             strncmp(argv[0].value, "unl", argv[0].length) == 0);
    }

    static void ext_abort(const void *cmd_cookie, const void *cookie) {
        (void) cmd_cookie;
        (void) cookie;
    }

}  /* extern C */

GetlExtension::GetlExtension(EventuallyPersistentStore *kvstore,
                             GET_SERVER_API get_server_api):
    backend(kvstore)
{
    serverApi = get_server_api();
}

void GetlExtension::initialize()
{
    if (serverApi != NULL) {
        EXTENSION_ASCII_PROTOCOL_DESCRIPTOR *ptr = this;
        get_name = ext_get_name;
        accept = ext_accept;
        execute = ext_execute;
        abort = ext_abort;
        cookie = ptr;
        serverApi->extension->register_extension(EXTENSION_ASCII_PROTOCOL, ptr);

        getLogger()->log(EXTENSION_LOG_INFO, NULL, "Loaded extension: getl\n");
    }
}

ENGINE_ERROR_CODE GetlExtension::executeGetl(int argc, token_t *argv,
                                             void *response_cookie,
                                             RESPONSE_HANDLER_T response_handler)
{
    uint32_t lockTimeout = ITEM_LOCK_TIMEOUT;
    char *metadata_ptr = NULL;
    int metadata_len = 0;


    if (argc >= 3) {
        if (!parseUint32(argv[2].value, &lockTimeout) ||
                lockTimeout > (ITEM_LOCK_TIMEOUT * 2)) {
            lockTimeout = ITEM_LOCK_TIMEOUT;
        }
        if (argc > 3) {
            metadata_ptr = argv[3].value;
            metadata_len = argv[3].length;  
        }


    } else if (argc != 2) {
        return response_handler(response_cookie,
                                sizeof("CLIENT_ERROR\r\n") - 1,
                                "CLIENT_ERROR\r\n");
    }

    std::string k(argv[1].value, argv[1].length);
    std::string metadata(metadata_ptr, metadata_len);
    RememberingCallback<GetValue> getCb;

    ObjectRegistry::onSwitchThread(&(backend->getEPEngine()));

    // TODO:  Get vbucket ID here.
    bool gotLock = backend->getLocked(k, 0, getCb,
            serverApi->core->get_current_time(),
            lockTimeout, metadata, response_cookie);

    Item *item = NULL;
    ENGINE_ERROR_CODE ret;

    getCb.waitForValue();

    ENGINE_ERROR_CODE rv = getCb.val.getStatus();
    if (rv == ENGINE_SUCCESS) {
        item = getCb.val.getValue();
        std::stringstream strm;

        strm << "VALUE " << item->getKey() << " " << ntohl(item->getFlags())
             << " " << item->getNBytes() << " " << item->getCksum() << " " <<
            item->getCas() << "\r\n";

        std::string strVal = strm.str();
        size_t len = strVal.length();

        if ((response_handler(response_cookie, static_cast<int>(len),
                              strVal.c_str()) == ENGINE_SUCCESS) &&
            (response_handler(response_cookie, item->getNBytes(),
                              item->getData()) == ENGINE_SUCCESS) &&
            (response_handler(response_cookie, 7,
                              "\r\nEND\r\n") == ENGINE_SUCCESS)) {
            ret = ENGINE_SUCCESS;
        } else {
            ret = ENGINE_DISCONNECT;
        }
    } else if (rv == ENGINE_EWOULDBLOCK) {
        ret = rv;
    } else if (!gotLock) {
        if (metadata.length() > 0) {
           std::stringstream strm;
           strm << "LOCK_ERROR " << metadata << "\r\n";
           ret = response_handler(response_cookie, 
                              sizeof("LOCK_ERROR \r\n") - 1 + metadata.length(), strm.str().c_str()); 
        } else {
           ret = response_handler(response_cookie,
                              sizeof("LOCK_ERROR\r\n") - 1, "LOCK_ERROR\r\n");
        }
    } else {
        ret = response_handler(response_cookie,
                               sizeof("NOT_FOUND\r\n") - 1, "NOT_FOUND\r\n");
    }

    if (item != NULL) {
        delete item;
    }

    return ret;
}

ENGINE_ERROR_CODE GetlExtension::executeUnl(int argc, token_t *argv, void *response_cookie,
                                            RESPONSE_HANDLER_T response_handler)
{
    uint64_t cas = 0;

    // we need a valid cas value
    if (argc != 3 || !parseUint64(argv[2].value, &cas)) {
        return response_handler(response_cookie,
                                sizeof("CLIENT_ERROR\r\n") - 1,
                                "CLIENT_ERROR\r\n");
    }

    std::string k(argv[1].value, argv[1].length);
    RememberingCallback<GetValue> getCb;

    ENGINE_ERROR_CODE rv = backend->unlockKey(k, 0, cas, serverApi->core->get_current_time());

    if (rv == ENGINE_SUCCESS) {
        return response_handler(response_cookie,
                                sizeof("UNLOCKED\r\n") -1, "UNLOCKED\r\n");

    } else if (rv == ENGINE_TMPFAIL) {
        return response_handler(response_cookie,
                                sizeof("UNLOCK_ERROR\r\n") - 1, "UNLOCK_ERROR\r\n");
    } else {
        return response_handler(response_cookie,
                                sizeof("NOT_FOUND\r\n") - 1, "NOT_FOUND\r\n");
    }
}

/* Extension to support options command for engine */

static DiExtension* getDiExtension(const void* cookie)
{
    return reinterpret_cast<DiExtension*>(const_cast<void *>(cookie));
}

extern "C" {
    static const char *di_ext_get_name(const void *cmd_cookie) {
        (void) cmd_cookie;
        return "DI";
    }

    static ENGINE_ERROR_CODE di_ext_execute(const void *cmd_cookie, const void *cookie,
            int argc, token_t *argv,
            RESPONSE_HANDLER_T response_handler) {
        (void) cmd_cookie;

        return getDiExtension(cmd_cookie)->executeDi(argc, argv,
                (void *)cookie,
                response_handler);
    }

    static bool di_ext_accept(const void *cmd_cookie, void *cookie,
            int argc, token_t *argv, size_t *ndata,
            char **ptr) {
        (void) cmd_cookie;
        (void) cookie;
        (void) argc;
        (void) ndata;
        (void) ptr;
        // accept option command
        return (sizeof("options")-1 == argv[0].length && 
            strncasecmp(argv[0].value, "options", argv[0].length) == 0);
    }

    static void di_ext_abort(const void *cmd_cookie, const void *cookie) {
        (void) cmd_cookie;
        (void) cookie;
    }

}  /* extern C */

DiExtension::DiExtension(EventuallyPersistentStore *kvstore,
                             GET_SERVER_API get_server_api):
    backend(kvstore)
{
    serverApi = get_server_api();
}


void DiExtension::initialize()
{
    if (serverApi != NULL) {
        EXTENSION_ASCII_PROTOCOL_DESCRIPTOR *ptr = this;
        get_name = di_ext_get_name;
        accept = di_ext_accept;
        execute = di_ext_execute;
        abort = di_ext_abort;
        cookie = ptr;
        serverApi->extension->register_extension(EXTENSION_ASCII_PROTOCOL, ptr);
        getLogger()->log(EXTENSION_LOG_INFO, NULL, "Loaded extension: DI\n");
    }
}

ENGINE_ERROR_CODE DiExtension::executeDi(int argc, token_t *argv,
        void *response_cookie,
        RESPONSE_HANDLER_T response_handler)
{
    (void) argc;
    (void) argv;
    return response_handler(response_cookie,
            sizeof(OPTION_RESPONSE) - 1,
            OPTION_RESPONSE);
}
