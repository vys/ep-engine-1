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
                if (last_min_data_age == store->stats.min_data_age && last_queue_age_cap == store->stats.queue_age_cap &&
                        (tv.tv_sec < waketime.tv_sec || (tv.tv_sec == waketime.tv_sec && tv.tv_usec < waketime.tv_usec))) {
                    d.snooze(tid, DEFAULT_MIN_SLEEP_TIME);
                    return true;
                }
                last_min_data_age = store->stats.min_data_age;
                last_queue_age_cap = store->stats.queue_age_cap;
                doFlush();
                if (_state == running) {
                    double tosleep = computeMinSleepTime();
                    gettimeofday(&waketime, NULL);
                    advance_tv(waketime, tosleep);
                    if (tosleep > 0) {
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
            store->stats.min_data_age = 0;
            completeFlush();
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

void Flusher::completeFlush() {
    doFlush();
    while (flushQueue) {
        doFlush();
    }
}

double Flusher::computeMinSleepTime() {
    // If we were preempted, keep going!
    if (flushQueue && !flushQueue->empty() && !rejectedItemsRequeued) {
        flushRv = 0;
        prevFlushRv = 0;
        return 0.0;
    }

    rejectedItemsRequeued = false;

    if (flushRv + prevFlushRv == 0) {
        minSleepTime = std::min(minSleepTime * 2, 1.0);
    } else {
        minSleepTime = DEFAULT_MIN_SLEEP_TIME;
    }
    prevFlushRv = flushRv;
    return std::max(static_cast<double>(flushRv), minSleepTime);
}

int Flusher::doFlush() {

    // On a fresh entry, flushQueue is null and we need to build one.
    if (!flushQueue) {
        flushRv = store->stats.min_data_age;
        flushQueue = helper->getFlushQueue();
        if (flushQueue && !flushQueue->empty()) {
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                             "Beginning a write queue flush.\n");
            rejectQueue = new std::queue<FlushEntry>();
            flushStart = ep_current_time();
        }
    }

    if (shouldFlushAll) {
        store->flushOneDeleteAll(flusherId);
        setFlushAll(false);
    }

    // Now do the every pass thing.
    if (flushQueue) {
        if (!flushQueue->empty()) {
            int n = store->flushSome(flushQueue, rejectQueue, flusherId);
            if (_state == pausing) {
                transition_state(paused);
            }
            flushRv = std::min(n, flushRv);
        }

        if (flushQueue->empty()) {
            if (rejectQueue && !rejectQueue->empty()) {
                // Requeue the rejects.
                store->requeueRejectedItems(rejectQueue, flushQueue, flusherId);
                rejectedItemsRequeued = true;
            } else {
                store->completeFlush(flushStart, flusherId);
                getLogger()->log(EXTENSION_LOG_INFO, NULL,
                                 "Completed a flush, age of oldest item was %ds\n",
                                 flushRv);

                delete flushQueue;
                delete rejectQueue;
                rejectQueue = NULL;
                flushQueue = NULL;
            }
        }
    } else {
        flushRv = 0;
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
        if (!queue) {
            queue = store->beginFlush(kvid);
        }
        if (flusher->state() == stopped) {
            break;
        }
        usleep(1000);
    }
    return;
}

void FlusherHelper::start() {
    if (pthread_create(&myId, NULL, flusher_helper, this) != 0) {
        throw std::runtime_error("Error creating flusher helper thread ");
    }
}
