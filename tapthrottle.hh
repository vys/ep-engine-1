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
#ifndef TAPTHROTTLE_HH
#define TAPTHROTTLE_HH 1

#include "common.hh"
#include "dispatcher.hh"
#include "stats.hh"

/**
 * Monitors various internal state to report whether we should
 * throttle incoming tap.
 */
class TapThrottle {
public:

    TapThrottle(EPStats &s) : stats(s) {}

    /**
     * If both are true, we should process incoming tap requests.
     */

    bool persistenceQueueSmallEnough() const;

private:
    EPStats &stats;
};

#endif // TAPTHROTTLE_HH
