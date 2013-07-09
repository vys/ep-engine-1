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
#ifndef INVALID_VBTABLE_REMOVER_HH
#define INVALID_VBTABLE_REMOVER_HH 1

#include "common.hh"
#include "dispatcher.hh"
#include "stats.hh"

const int INVALID_VBTABLE_DEL_FREQ(600);

// Forward declaration.
class EventuallyPersistentEngine;

/**
 * Periodically remove invalid vbucket tables from the underlying database
 */
class InvalidVBTableRemover : public DispatcherCallback {
public:
    InvalidVBTableRemover(EventuallyPersistentEngine *e) : engine(e) { }

    bool callback(Dispatcher &d, TaskId t);

    /**
     * Description of task.
     */
    std::string description() {
        std::string rv("Removing an invalid vbucket table from DB");
        return rv;
    }

private:
    EventuallyPersistentEngine         *engine;
};

#endif /* INVALID_VBTABLE_REMOVER_HH */
