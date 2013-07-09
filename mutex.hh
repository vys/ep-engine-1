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
#ifndef MUTEX_HH
#define MUTEX_HH 1

#include <stdexcept>
#include <iostream>
#include <sstream>
#include <pthread.h>
#include <cerrno>
#include <cstring>
#include <cassert>

#include "common.hh"

/**
 * Abstraction built on top of pthread mutexes
 */
class Mutex {
public:
    Mutex() : held(false)
    {
        pthread_mutexattr_t *attr = NULL;
        int e=0;

#ifdef HAVE_PTHREAD_MUTEX_ERRORCHECK
        pthread_mutexattr_t the_attr;
        attr = &the_attr;

        if (pthread_mutexattr_init(attr) != 0 ||
            (e = pthread_mutexattr_settype(attr, PTHREAD_MUTEX_ERRORCHECK)) != 0) {
            std::string message = "MUTEX ERROR: Failed to initialize mutex: ";
            message.append(std::strerror(e));
            throw std::runtime_error(message);
        }
#endif

        if ((e = pthread_mutex_init(&mutex, attr)) != 0) {
            std::string message = "MUTEX ERROR: Failed to initialize mutex: ";
            message.append(std::strerror(e));
            throw std::runtime_error(message);
        }
    }

    virtual ~Mutex() {
        int e;
        if ((e = pthread_mutex_destroy(&mutex)) != 0) {
            std::string message = "MUTEX ERROR: Failed to destroy mutex: ";
            message.append(std::strerror(e));
            throw std::runtime_error(message);
        }
    }

    /**
     * True if I own this lock.
     *
     * Use this only for assertions.
     */
    bool ownsLock() {
        return held && pthread_equal(holder, pthread_self());
    }

protected:

    // The holders of locks twiddle these flags.
    friend class LockHolder;
    friend class MultiLockHolder;

    void acquire() {
        int e;
        if ((e = pthread_mutex_lock(&mutex)) != 0) {
            std::string message = "MUTEX ERROR: Failed to acquire lock: ";
            message.append(std::strerror(e));
            throw std::runtime_error(message);
        }
        setHolder(true);
    }

    bool tryAcquire() {
        int e;
        if ((e = pthread_mutex_trylock(&mutex)) != 0) {
            if (e == EBUSY) {
                return false;
            }
            std::string message = "MUTEX ERROR: Failed to acquire lock: ";
            message.append(std::strerror(e));
            throw std::runtime_error(message);
        }
        setHolder(true);
        return true;
    }

    void release() {
        assert(held && pthread_equal(holder, pthread_self()));
        setHolder(false);
        int e;
        if ((e = pthread_mutex_unlock(&mutex)) != 0) {
            std::string message = "MUTEX_ERROR: Failed to release lock: ";
            message.append(std::strerror(e));
            throw std::runtime_error(message);
        }
    }

    void setHolder(bool isHeld) {
        held = isHeld;
        holder = pthread_self();
    }

    pthread_mutex_t mutex;
    pthread_t holder;
    bool held;

    DISALLOW_COPY_AND_ASSIGN(Mutex);
};

#endif
