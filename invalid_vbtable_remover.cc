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

#include "config.h"
#include "invalid_vbtable_remover.hh"
#include "ep_engine.h"

const size_t PERSISTENCE_QUEUE_SIZE_THRESHOLD(1000000);

bool InvalidVBTableRemover::callback(Dispatcher &d, TaskId t) {
    size_t queueSize = engine->getEpStats().queue_size.get() +
                       engine->getEpStats().flusher_todo_get();
    if (queueSize < PERSISTENCE_QUEUE_SIZE_THRESHOLD) {
        // TODO: We need to determine the persistence queue size threshould dynamically
        // by considering various stats. More elegant solution would be to implement the
        // dynamic configuration manager in ns_server, which adjusts this threshold dynamically
        // based on various monitoring stats from each node.
        hrtime_t start_time(gethrtime());
        engine->getEpStore()->getRWUnderlying(0)->destroyInvalidVBuckets(true);
        hrtime_t wall_time = (gethrtime() - start_time) / 1000;
        engine->getEpStats().diskInvalidVBTableDelHisto.add(wall_time);
    }
    d.snooze(t, INVALID_VBTABLE_DEL_FREQ);
    return true;
}
