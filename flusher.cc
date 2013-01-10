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
#include <stdlib.h>

#include "flusher.hh"

bool FlusherStepper::callback(Dispatcher &d, TaskId t) {
    return flusher->step(d, t);
}

std::string FlusherStepper::description() {
    char buf[64];
    snprintf(buf, 64, "Running flusher loop %d", flusher->getFlusherId());
    return std::string(buf);
}

bool Flusher::stop(bool isForceShutdown) {
    forceShutdownReceived = isForceShutdown;
    enum flusher_state to = forceShutdownReceived ? stopped : stopping;
    return transition_state(to);
}

void Flusher::wait(void) {
    hrtime_t startt(gethrtime());
    while (_state != stopped) {
        usleep(1000);
    }
    hrtime_t endt(gethrtime());
    int udiff((endt - startt) / 1000);
    if (udiff > 10000) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Had to wait %dus for shutdown\n", udiff);
    }
}

bool Flusher::pause(void) {
    return transition_state(pausing);
}

bool Flusher::resume(void) {
    return transition_state(running);
}

static bool validTransition(enum flusher_state from,
                            enum flusher_state to)
{
    bool rv(true);
    if (from == initializing && to == running) {
    } else if (from == initializing && to == stopping) {
    } else if (from == running && to == pausing) {
    } else if (from == running && to == stopping) {
    } else if (from == pausing && to == paused) {
    } else if (from == stopping && to == stopped) {
    } else if (from == paused && to == running) {
    } else if (from == paused && to == stopping) {
    } else if (from == pausing && to == stopping) {
    } else {
        rv = false;
    }
    return rv;
}

const char * Flusher::stateName(enum flusher_state st) const {
    static const char * const stateNames[] = {
        "initializing", "running", "pausing", "paused", "stopping", "stopped"
    };
    assert(st >= initializing && st <= stopped);
    return stateNames[st];
}

bool Flusher::transition_state(enum flusher_state to) {

    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                     "Attempting transition from %s to %s\n",
                     stateName(_state), stateName(to));

    if (!forceShutdownReceived && !validTransition(_state, to)) {
        return false;
    }

    getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Transitioning from %s to %s\n",
                     stateName(_state), stateName(to));

    _state = to;
    //Reschedule the task
    LockHolder lh(taskMutex);
    assert(task.get());
    dispatcher->cancel(task);
    schedule_UNLOCKED();
    return true;
}

const char * Flusher::stateName() const {
    return stateName(_state);
}

enum flusher_state Flusher::state() const {
    return _state;
}

void Flusher::initialize(TaskId tid) {
    assert(task.get() == tid.get());
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                     "Initializing flusher; warming up\n");

    hrtime_t startTime = gethrtime();
    vbStateLoaded = false;
    store->warmup(vbStateLoaded, flusherId);
    store->stats.warmupTime.set((gethrtime() - startTime) / 1000);
    store->stats.warmupComplete.set(true);

    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                     "Warmup completed in %ds\n", store->stats.warmupTime.get());
    transition_state(running);
}

void Flusher::schedule_UNLOCKED() {
    dispatcher->schedule(shared_ptr<FlusherStepper>(new FlusherStepper(this)),
                         &task, Priority::FlusherPriority);
    assert(task.get());
}

void Flusher::start(void) {
    LockHolder lh(taskMutex);
    schedule_UNLOCKED();
    helper->start();
}

void Flusher::wake(void) {
    LockHolder lh(taskMutex);
    assert(task.get());
    dispatcher->wake(task, &task);
}

bool Flusher::step(Dispatcher &d, TaskId tid) {
    try {
        switch (_state) {
        case initializing:
            initialize(tid);
            return true;
        case paused:
            return false;
        case pausing:
            transition_state(paused);
            return false;
        case running:
            {
                // Poll the dispatcher every DEFAULT_MIN_SLEEP_TIME (0.1) seconds to
                // check if min_data_age changed, in which case perform the flush operation
                timeval tv;
                gettimeofday(&tv, NULL);
                bool valueChanged = false;
                if (last_min_data_age != store->stats.min_data_age || last_queue_age_cap != store->stats.queue_age_cap) {
                    valueChanged = true;
                    // if min_data_age or queue_age_cap was changed by the user, reset
                    // waketime to the current time so that it is recomputed correctly
                    gettimeofday(&waketime, NULL);
                }
                if (!valueChanged &&
                        (tv.tv_sec < waketime.tv_sec || (tv.tv_sec == waketime.tv_sec && tv.tv_usec < waketime.tv_usec))) {
                    d.snooze(tid, DEFAULT_MIN_SLEEP_TIME);
                    return true;
                }
                last_min_data_age = store->stats.min_data_age;
                last_queue_age_cap = store->stats.queue_age_cap;
                doFlush();
                if (_state == running) {
                    if (flushRv > 0) {
                        gettimeofday(&waketime, NULL);
                        advance_tv(waketime, flushRv);
                    } else if (!flushList || flushList->empty()) {
                        d.snooze(tid, DEFAULT_MIN_SLEEP_TIME);
                    }
                    return true;
                } else {
                    return false;
                }
            }
        case stopping:
            {
                std::stringstream ss;
                ss << "Shutting down flusher (Write of all dirty items)"
                   << std::endl;
                getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "%s",
                                 ss.str().c_str());
            }
            store->stats.min_data_age = 0; // FIXME why is this needed?
            flushAllPending();
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Flusher stopped\n");
            transition_state(stopped);
            return false;
        case stopped:
            return false;
        default:
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                "Unexpected state in flusher: %s", stateName());
            assert(false);
        }
    } catch(std::runtime_error &e) {
        std::stringstream ss;
        ss << "Exception in flusher loop: " << e.what() << std::endl;
        getLogger()->log(EXTENSION_LOG_WARNING, NULL, "%s",
                         ss.str().c_str());
        assert(false);
    }

    // We should _NEVER_ get here (unless you compile with -DNDEBUG causing
    // the assertions to be removed.. It's a bug, so we should abort and
    // create a coredump
    abort();
}

void Flusher::flushAllPending() {
    doFlush();
    do {
        doFlush();
    } while (helper->more() || store->hasItemsForPersistence(flusherId) > 0);
}

void Flusher::setupFlushQueues() {
    if (!flushList || !flushList->empty()) {
        flushList = helper->getFlushQueue();
        if (flushList && !flushList->empty()) {
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                             "Beginning a write queue flush.\n");
            rejectList = new std::list<FlushEntry>();
            flushStart = ep_current_time();
            prevFlushRv = store->stats.min_data_age;
        }
    }
}

int Flusher::doFlush() {

    // On a fresh entry, flushList is null and we need to build one.
    setupFlushQueues();
    flushRv = 0;

    if (shouldFlushAll) {
        store->flushOneDeleteAll(flusherId);
        setFlushAll(false);
    }

    // Now do the every pass thing.
    if (flushList && !flushList->empty()) {
        int n = store->flushSome(flushList, rejectList, flusherId);
        prevFlushRv = std::min(n, prevFlushRv);
        if (_state == pausing) {
            transition_state(paused);
        }

        if (flushList->empty()) {
            if (rejectList && !rejectList->empty()) {
                // Requeue the rejects.
                store->requeueRejectedItems(rejectList, flushList, flusherId);
                store->stats.flusherRequeuedRejected[flusherId]++;
                flushRv = prevFlushRv;
            } else {
                store->completeFlush(flushStart, flusherId);
                getLogger()->log(EXTENSION_LOG_INFO, NULL,
                                 "Completed flush by id = %d, age of oldest item was %ds\n", flusherId, flushRv);

                delete flushList;
                delete rejectList;
                rejectList = NULL;
                flushList = NULL;
            }
        }
    }

    return flushRv;
}

extern "C" {
    static void* flusher_helper(void* args);
}

static void* flusher_helper(void *args) {
    FlusherHelper *helper = (FlusherHelper *)args;
    helper->run();
    return NULL;
}

void FlusherHelper::run() {

    while (1) {
        const Flusher *flusher = store->getFlusher(kvid);
        if (flusher->state() == stopped) {
            break;
        }
        LockHolder lh(sync);
        if (!flushList) {
            flushList = new std::list<FlushEntry>();
            store->beginFlush(*flushList, kvid);
        }
        sync.wait();
    }
    return;
}

void FlusherHelper::start() {
    if (pthread_create(&myId, NULL, flusher_helper, this) != 0) {
        throw std::runtime_error("Error creating flusher helper thread ");
    }
}
