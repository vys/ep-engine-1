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
#ifndef HTRESIZER_HH
#define HTRESIZER_HH 1

#include "config.h"

#include "dispatcher.hh"

class EventuallyPersistentStore;

/**
 * Look around at hash tables and verify they're all sized
 * appropriately.
 */
class HashtableResizer : public DispatcherCallback {
public:

    HashtableResizer(EventuallyPersistentStore *s) : store(s) {}

    bool callback(Dispatcher &d, TaskId t);

    std::string description() {
        return std::string("Adjusting hash table sizes.");
    }

private:
    EventuallyPersistentStore *store;
};

#endif // HTRESIZER_HH
