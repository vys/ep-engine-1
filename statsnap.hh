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
#ifndef STATSNAP_HH
#define STATSNAP_HH 1

#include "common.hh"

#include <string>
#include <map>

#include "dispatcher.hh"
#include "stats.hh"

const int STATSNAP_FREQ(60);

// Forward declaration.
class EventuallyPersistentEngine;

/**
 * Periodically take a snapshot of the stats and record it in the main
 * DB.
 */
class StatSnap : public DispatcherCallback {
public:
    StatSnap(EventuallyPersistentEngine *e, int _kvid) : engine(e), kvid(_kvid) { }

    bool callback(Dispatcher &d, TaskId t);

    /**
     * Grab stats from the engine.
     *
     * @param stats the optional stat parameter
     * @return true if the stat fetch was successful
     */
    bool getStats(const char *stats=NULL);

    /**
     * Get the current map of data.
     */
    std::map<std::string, std::string> &getMap() { return map; }

    /**
     * Description of task.
     */
    std::string description() {
        std::string rv("Updating stat snapshot on disk");
        return rv;
    }

private:
    EventuallyPersistentEngine         *engine;
    std::map<std::string, std::string>  map;
    int kvid;
};

#endif /* STATSNAP_HH */
