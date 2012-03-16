#!/usr/bin/env python

"""

Keys:
Every time block is assigned a prefix which is the name specified in its specification. In each block, threads are run with and ID (1 to 8).
Based on the throughput requirements in the block, every thread is assigned a number of operations to perform (with a SET/GET ratio). For
the SET operations, the keys are chosen in the following format:

<block_prefix>_<thread_id>_<key_num>

key_num is the number of the key pumped in, which is sequentially done starting from 1 to the total number expected.

The corresponding GET operations will be performed on the keys that have already been SET previously. Since everything is done
sequentially, these keys are generated on the fly instead of picking up from an array.


JSON format (distribution):
The configuration JSON specifies the percentages of SET and GET operations to perform in each block. These are maintained in CONF:

CONF = [BLOCK, BLOCK, BLOCK, ...]

BLOCK = {
           "name" : NAME,
           "start_time" : TIME
           "duration" : TIME
           "total_ops" : INT or "throughput" : INT
           "data_size" : INT or "total_size" : INT
           "set_perc" : PERCENTAGE,
           "set_distro" : DISTRIBUTION,
           "get_distro" : DISTRIBUTION
        }
Note: The start_time and duration values must be set to avoid overlap between any two blocks. For better performance, a positive INT second
gap between consecutive blocks is recommended. Exactly one of throughput (operations/sec) and total_ops must be specified (if both specified,
only total_ops is considered). Exactly one of data_size and total_size (size in bytes) must be specified (if both specified, only data_size
is considered).

DISTRIBUTION = {NAME : PERCENTAGE, NAME : PERCENTAGE, ...}
Note: Each NAME must belong to a block that has been executed previously (or the current). The total sum of percentages must be atmost 100.
In the case when the total percentage sum is less than 100, the remaining bandwidth is given to the current block. This constitutes the keys
that have already been inserted in the current block's execution.

NAME = unique STRING

PERCENTAGE = 0 <= INT <= 100

TIME = time as INT seconds.
Note: start_time must be atleast 0 and duration atleast 1.

"""

import sys
import time
import exceptions
import string
import argparse
import json
import random

sys.path.append('../management')
import mc_bin_client

STRDATA = ''
STRDATASIZE = 2000000 # 2MB
VERBOSE = False
CONF = []
THREADS = 1
# throughput/total_ops
# duration
# total_size/size_per_entry
BLOCKS = []

def log(*s):
    global VERBOSE
    if VERBOSE:
        print 'LOG: ' + ' '.join(s)

def err_and_exit(msg):
    print msg
    sys.exit()

def gen_string(size):
    global STRDATA, STRDATASIZE
    index = random.randint(0, STRDATASIZE - size - 1)
    return STRDATA[index : index+size]

def parse_command_line():
    global VERBOSE, CONF, THREADS
    parser = argparse.ArgumentParser(description="A time based incremental GET/SET dumper for testing memcache")
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbosity')
    parser.add_argument('-t', '--threads', action='store', type=int, default=1, required=False, help='Number of concurrent threads to run (1 to 8)')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('jsonstring', nargs='?', help='config in json format')
    group.add_argument('-c', '--configfile', action='store', required=False, help='Config file to read from if json string not specified')

    args = parser.parse_args()
    VERBOSE = args.verbose

    THREADS = args.threads
    if THREADS < 1 or THREADS > 8:
        err_and_exit("Thread count should be between 1 and 8.")

    configfile = args.configfile
    if not configfile == None:
        try:
            jsonstr = open(configfile, 'r').read()
        except IOError:
            err_and_exit("File " + configfile + " cannot be opened for reading.")

    jsonstr = args.jsonstring
    CONF = json.loads(jsonstr)

def init():
    global STRDATA, STRDATASIZE
    parse_command_line()
    # Generate a 2MB string to pick up random data from
    STRDATA = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(STRDATASIZE))

def pump_data(key_prefix, distribution, duration, total_ops, set_perc, avg_size, const_size=True):
    pass

def validate_distro(distro, names, cname):
    total_perc = 0
    for name in distro:
        if not name in names:
            err_and_exit("Invalid distribution: " + cname)
        perc = distro[name]
        if perc < 0:
            err_and_exit("Invalid percentage in distribution: " + cname)
        total_perc += distro[name]
    if total_perc > 100:
        err_and_exit("Total percentage in distribution must be atmost 100: " + cname)

def process_conf():
    global CONF
    names = []
    intervals = []

    for block in CONF:
        name = block['name']
        if name in names:
            err_and_exit("Duplicate block names: " + name)

        start_time = block['start_time']
        duration = block['duration']
        end_time = start_time + duration
        if duration < 1:
            err_and_exit("Duration must be positive: " + name)
        for interval in intervals:
            if start_time < interval[1] and end_time > interval[0]:
                err_and_exit("Time interval conflict: " + name)
        intervals.append([start_time, end_time])

        total_ops = 0
        if 'total_ops' in block:
            total_ops = block['total_ops']
        elif 'throughput' in block:
            total_ops = block['throughput'] * duration
        else:
            err_and_exit("Atleast one of total_ops and throughput must be specified: " + name)
        if total_ops < 0:
            err_and_exit("Atleast 0 operations are required: " + name)

        const_size = True
        data_size = 0
        if 'data_size' in block:
            data_size = block['data_size']
        elif 'total_size' in block:
            data_size = total_size / total_ops
            const_size = False
        else:
            err_and_exit("Atleast one of data_size and total_size must be specified: " + name)
        if data_size < 1:
            err_and_exit("Atleast 0 data size is required: " + name)

        set_perc = block['set_perc']
        if set_perc < 0 or set_perc > 100:
            err_and_exit("Percentages must be within (0, 100): " + name)

        set_distro = block['set_distro']
        get_distro = block['get_distro']
        validate_distro(set_distro, names, cname)
        validate_distro(get_distro, names, cname)

        names.append(name)
        # Put in BLOCKS

if __name__ == '__main__':
    init()

