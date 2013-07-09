#!/usr/bin/env python

#   Copyright 2013 Zynga inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import sys
import time
import exceptions
import string
import random

sys.path.append('../management')
import mc_bin_client

LRUINTERVAL = 20
EXPIRYINF = 1000
TCHECK_COUNT = 10
TCHECK_INTERVAL = 1
VERBOSE = False
KEYLIST = []

def log(*s):
    if VERBOSE:
        print ' '.join(s)

def gen_string(size=15, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def key_in_mc(mc, key):
    found = True
    try:
        mc.get(key)
    except mc_bin_client.MemcachedError:
        found = False
    return found

def gen_new_key(mc):
    key = gen_string()
    while key in KEYLIST or key_in_mc(mc, key):
        key = gen_string()
    KEYLIST.append(key)
    return key

def store(mc, exp):
    key = gen_new_key(mc)
    val = gen_string()
    log(">> SET", key, str(exp), val)
    mc.set(key, exp, 0, val)

def get_stat(mc, key):
    stats = mc.stats()
    return int(stats[key])

def wait_for_lru(mc):
    log(">> Waiting for LRU to build a fresh cache")
    oldtime = get_stat(mc, 'ep_lru_build_end_time')
    while oldtime == -1:  # wait for current build to finish
        oldtime = get_stat(mc, 'ep_lru_build_end_time')
    newtime = get_stat(mc, 'ep_lru_build_end_time')
    while newtime in [-1, oldtime]:  # wait for a fresh build to finish
        newtime = get_stat(mc, 'ep_lru_build_end_time')

def get_lru_total(mc):
    log(">> Counting items in LRU")
    lstats = mc.stats("lru")
    lcount = 0
    for key in lstats:
        lcount += int(lstats[key])
    log(">> Found", str(lcount))
    return lcount

def validate_count(mc):
    log(">> Validating LRU count")
    wait_for_lru(mc)
    lcount = get_lru_total(mc)
    log(">> Counting items in cache")
    gcount = get_stat(mc, 'curr_items')
    log(">> Found", str(gcount))
    if lcount != gcount:
        raise exceptions.Exception("Expecting " + str(gcount) + " items in LRU but found " + str(lcount))

def key_in_lru(mc, key):
    key_stats = mc.stats("key " + key)
    return (int(key_stats['key_in_lru']) == 1)

def pick_random_key_from_list():
    return KEYLIST[random.randint(0, len(KEYLIST)-1)]

################################
###### ACTUAL TESTS BELOW ######
################################

def check_lru_stat_command(mc):
    log(">> Checking stats command with argument 'lru'")
    stats = mc.stats("lru")
    for key in stats:
        value = stats[key]  # check if it's iterable

def check_total(mc):
    log(">> Storing", str(TCHECK_COUNT), "elements at", str(TCHECK_INTERVAL), "second intervals")
    for x in range(TCHECK_COUNT):
        store(mc, EXPIRYINF)
        time.sleep(TCHECK_INTERVAL)
    validate_count(mc)

def check_delete(mc):
    TDELETE_COUNT = TCHECK_COUNT / 2
    log(">> Deleting", str(TDELETE_COUNT), "keys from the cache")
    for x in range(TDELETE_COUNT):
        key = pick_random_key_from_list()
        log(">> DELETE", key)
        KEYLIST.remove(key)
        mc.delete(key)
    validate_count(mc)

# This check does not wait for LRU when called, hence has to be called immediately after check_delete
def check_keys(mc):
    log(">> Checking if current keys are in LRU")
    for key in KEYLIST:
        if not key_in_lru(mc, key):
            raise exceptions.Exception("Expecting " + key + " in LRU but not found")
    TCHECK_LRUDELETE = len(KEYLIST) / 2
    log(">> Deleting", str(TCHECK_LRUDELETE), "keys from LRU")
    DELETED = []
    for x in range(TCHECK_LRUDELETE):
        key = pick_random_key_from_list()
        log(">> DELETE", key)
        KEYLIST.remove(key)
        DELETED.append(key)
        mc.delete(key)
    wait_for_lru(mc)
    log(">> Checking if deletes are reflected in LRU")
    for key in KEYLIST:
        if not key_in_lru(mc, key):
            raise exceptions.Exception("Expecting " + key + " in LRU but not found")
    # The following needs to change (we need to check if LRU didn't pick it up)
    for key in DELETED:
        try:
            if key_in_lru(mc, key):
                raise exceptions.Exception("Not expecting " + key + " in LRU but found")
        except mc_bin_client.MemcachedError:
            pass

if __name__ == '__main__':
    if '-v' in sys.argv:
        VERBOSE = True

    mc = mc_bin_client.MemcachedClient()
    mc.set_flush_param("exp_pager_stime", str(LRUINTERVAL))
    mc.set_flush_param("max_lru_entries", "500")

    check_lru_stat_command(mc)
    check_total(mc)
    check_delete(mc)
    check_keys(mc)

    # More cases with max_lru_entries limit
