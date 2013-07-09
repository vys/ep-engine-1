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
#ifndef ITEM_PAGER_HH
#define ITEM_PAGER_HH 1

#include <map>
#include <vector>
#include <list>

#include "common.hh"
#include "dispatcher.hh"
#include "stats.hh"


class EventuallyPersistentStore;

typedef std::pair<int64_t, int64_t> row_range;

/**
 * Dispatcher job responsible for purging expired items from
 * memory and disk.
 */
class ExpiredItemPager : public DispatcherCallback {
public:

    /**
     * Construct an ExpiredItemPager.
     *
     * @param s the store (where we'll visit)
     * @param st the stats
     * @param stime number of seconds to wait between runs
     */
    ExpiredItemPager(EventuallyPersistentStore *s, EPStats &st,
                     size_t stime, size_t ltime) :
        store(s), stats(st), sleepTime(stime), lruSleepTime(ltime),
        available(true), lastRun(ep_real_time()) {}

    bool callback(Dispatcher &d, TaskId t);

    std::string description() { return std::string("Paging expired items."); }

    bool pagerRunNeeded() {
        if (lastRun + sleepTime <= ep_real_time()) {
            return true;
        }
        return false;
    }

    void updateSleepTimes(time_t exp, time_t lru) {
        sleepTime = exp;
        lruSleepTime = lru;
    }

    uint32_t callbackFreq() {
        time_t min = 10;
        if (sleepTime < min) {
            min = sleepTime;
        }
        if (lruSleepTime < min) {
            min = lruSleepTime;
        }
        return static_cast<uint32_t>(min);
    }

private:
    EventuallyPersistentStore *store;
    EPStats                   &stats;
    time_t                     sleepTime;
    time_t                     lruSleepTime;
    bool                       available;
    time_t lastRun;
};

/**
 * Dispatcher job responsible for purging invalid items with the old vbucket version
 * from disk.
 */
class InvalidItemDbPager : public DispatcherCallback {
public:

    /**
     * Construct an InvalidItemDbPager.
     *
     * @param s the store
     * @param st the stats
     * @param deletion_size removal chunk size
     */
    InvalidItemDbPager(EventuallyPersistentStore *s, EPStats &st,
                       size_t deletion_size) :
        store(s), stats(st), chunk_size(deletion_size) { }

    ~InvalidItemDbPager() {
        std::map<uint16_t, std::vector<int64_t>* >::iterator it;
        for (it = vb_items.begin(); it != vb_items.end(); it++) {
            delete (*it).second;
        }
    }

    bool callback(Dispatcher &d, TaskId t);

    void addInvalidItem(Item *item, uint16_t vb_version);

    void createRangeList();

    std::string description() {
        return std::string("Removing items with the old vbucket version from disk.");
    }

private:
    EventuallyPersistentStore                    *store;
    EPStats                                      &stats;
    size_t                                        chunk_size;
    std::map<uint16_t, uint16_t>                  vb_versions;
    std::map<uint16_t, std::vector<int64_t>* >    vb_items;
    std::map<uint16_t, std::list<row_range> >     vb_row_ranges;
};

#endif /* ITEM_PAGER_HH */
