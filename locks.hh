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
#ifndef LOCKS_H
#define LOCKS_H 1

#include <stdexcept>
#include <iostream>
#include <sstream>
#include <functional>

#include "common.hh"
#include "mutex.hh"
#include "syncobject.hh"

/**
 * RAII lock holder to guarantee release of the lock.
 *
 * It is a very bad idea to unlock a lock held by a LockHolder without
 * using the LockHolder::unlock method.
 */
class LockHolder {
public:
    /**
     * Acquire the lock in the given mutex.
     * If try_lock is a non-NULL pointer, attempt trylock and set status in try_lock
     */
    LockHolder(Mutex &m, bool *try_lock = NULL) : mutex(m), locked(false) {
        if (try_lock) {
            if (trylock()) {
                *try_lock = true;
            } else {
                *try_lock = false;
            }
        } else {
            lock();
        }
    }

    /**
     * Copy constructor hands this lock to the new copy and then
     * consider it released locally (i.e. renders unlock() a noop).
     */
    LockHolder(const LockHolder& from) : mutex(from.mutex), locked(true) {
        const_cast<LockHolder*>(&from)->locked = false;
    }

    /**
     * Release the lock.
     */
    ~LockHolder() {
        unlock();
    }

    /**
     * Relock a lock that was manually unlocked.
     */
    void lock() {
        mutex.acquire();
        locked = true;
    }

    /**
     * Try relocking a lock that was manually unlocked.
     * Return true on success.
     */
    bool trylock() {
        if (mutex.tryAcquire()) {
            locked = true;
            return true;
        }
        return false;
    }

    /**
     * Manually unlock a lock.
     */
    void unlock() {
        if (locked) {
            locked = false;
            mutex.release();
        }
    }

private:
    Mutex &mutex;
    bool locked;

    void operator=(const LockHolder&);
};

/**
 * RAII lock holder over multiple locks.
 */
class MultiLockHolder {
public:

    /**
     * Acquire a series of locks.
     *
     * @param m beginning of an array of locks
     * @param n the number of locks to lock
     */
    MultiLockHolder(Mutex *m, size_t n) : mutexes(m),
                                          locked(new bool[n]),
                                          n_locks(n) {
        std::fill_n(locked, n_locks, false);
        lock();
    }

    /**
     * Copy constructor hands this lock to the new copy and then
     * consider it released locally (i.e. renders unlock() a noop).
     */
    MultiLockHolder(const MultiLockHolder& from) : mutexes(from.mutexes) {

        for (size_t i = 0; i < n_locks; i++) {
            locked[i] = true;
            const_cast<MultiLockHolder*>(&from)->locked[i] = false;
        }
    }

    ~MultiLockHolder() {
        unlock();
        delete[] locked;
    }

    /**
     * Relock the series after having manually unlocked it.
     */
    void lock() {
        for (size_t i = 0; i < n_locks; i++) {
            assert(!locked[i]);
            mutexes[i].acquire();
            locked[i] = true;
        }
    }

    /**
     * Manually unlock the series.
     */
    void unlock() {
        for (size_t i = 0; i < n_locks; i++) {
            if (locked[i]) {
                locked[i] = false;
                mutexes[i].release();
            }
        }
    }

private:
    Mutex  *mutexes;
    bool   *locked;
    size_t  n_locks;

};

#endif /* LOCKS_H */
