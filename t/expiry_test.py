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

sys.path.append('../management')
import mc_bin_client

EXPIRY = 2
VERBOSE = False

def log(*s):
    if VERBOSE:
        print ' '.join(s)

def store(mc, kprefix):
    k = kprefix + '.set'
    log(">> ", k)
    mc.set(k, EXPIRY, 0, k)
    k = kprefix + '.add'
    log("++ ", k)
    mc.add(k, EXPIRY, 0, k)

def check(mc, kprefix):
    for suffix in ['.set', '.add']:
        try:
            k = kprefix + suffix
            log("<< ", k)
            mc.get(k)
            raise exceptions.Exception("Expected to fail to get " + k)
        except mc_bin_client.MemcachedError:
            pass

if __name__ == '__main__':
    mc = mc_bin_client.MemcachedClient()

    if '-v' in sys.argv:
        VERBOSE = True

    store(mc, 'a1')
    store(mc, 'a2')
    log("(sleep)")
    time.sleep(EXPIRY + 1)
    check(mc, 'a2')
    store(mc, 'a1')
    store(mc, 'a2')
