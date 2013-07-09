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
#ifndef CHECKPOINT_REMOVER_HH
#define CHECKPOINT_REMOVER_HH 1

#include <assert.h>
#include <set>

#include "common.hh"
#include "stats.hh"
#include "dispatcher.hh"

class EventuallyPersistentStore;

/**
 * Dispatcher job responsible for removing closed unreferenced checkpoints from memory.
 */
class ClosedUnrefCheckpointRemover : public DispatcherCallback {
public:

    /**
     * Construct ClosedUnrefCheckpointRemover.
     * @param s the store
     * @param st the stats
     */
    ClosedUnrefCheckpointRemover(EventuallyPersistentStore *s, EPStats &st,
                                 size_t interval) :
        store(s), stats(st), sleepTime(interval), available(true),
        lastRun(ep_current_time()) {}

    bool callback(Dispatcher &d, TaskId t);

    std::string description() {
        return std::string("Removing closed unreferenced checkpoints from memory");
    }

private:
    EventuallyPersistentStore *store;
    EPStats                   &stats;
    size_t                     sleepTime;
    bool                       available;
    time_t                     lastRun;
};

#endif /* CHECKPOINT_REMOVER_HH */
