/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

/* Consider this file as an extension to config.h, just that it contains
 * static text. The intention is to reduce the number of #ifdefs in the rest
 * of the source files without having to put all of them in AH_BOTTOM
 * in configure.ac.
 */
#ifndef CONFIG_STATIC_H
#define CONFIG_STATIC_H 1

#ifdef HAVE_INTTYPES_H
#include <inttypes.h>
#endif

#if ((defined (__SUNPRO_C) || defined(__SUNPRO_CC)) || defined __GNUC__)
#define EXPORT_FUNCTION __attribute__ ((visibility("default")))
#else
#define EXPORT_FUNCTION
#endif

#if HAVE_ARPA_INET_H
#include <arpa/inet.h>
#endif

#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif

#ifndef HAVE_GETHRTIME
typedef uint64_t hrtime_t;

#ifdef __cplusplus
extern "C" {
#endif
    extern hrtime_t gethrtime(void);
#ifdef __cplusplus
}
#endif

#endif

#ifndef SQLITE_HAS_CODEC
#define SQLITE_HAS_CODEC 0
#endif

#ifdef HAVE_POLL_H
#include <poll.h>
#endif

#ifdef linux
#undef ntohs
#undef ntohl
#undef htons
#undef htonl
#endif

#ifndef HAVE_HTONLL
#ifdef __cplusplus
extern "C" {
#endif
    extern uint64_t htonll(uint64_t);
    extern uint64_t ntohll(uint64_t);
#ifdef __cplusplus
}
#endif
#endif

#endif
