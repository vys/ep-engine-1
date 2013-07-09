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
#include "atomic.hh"
#include "locks.hh"
#include <pthread.h>
#include <unistd.h>
#include "assert.h"
#define NUM_THREADS 90
#define NUM_ITEMS 100000

struct thread_args {
    SyncObject mutex;
    SyncObject gate;
    AtomicList<int> list;
    int counter;
};

extern "C" {
static void *launch_consumer_thread(void *arg) {
    struct thread_args *args = static_cast<struct thread_args *>(arg);
    int count(0);
    std::list<int> outList;
    LockHolder lh(args->mutex);
    LockHolder lhg(args->gate);
    args->counter++;
    lhg.unlock();
    args->gate.notify();
    args->mutex.wait();
    lh.unlock();

    while(count < NUM_THREADS * NUM_ITEMS) {
        args->list.size();
        args->list.getAll(outList);
        while (!outList.empty()) {
            count++;
            outList.pop_front();
        }
    }
    sleep(1);
    assert(args->list.empty());
    assert(outList.empty());
    return NULL;
}

static void *launch_test_thread(void *arg) {
    struct thread_args *args = static_cast<struct thread_args *>(arg);
    int i(0);
    LockHolder lh(args->mutex);
    LockHolder lhg(args->gate);
    args->counter++;
    lhg.unlock();
    args->gate.notify();
    args->mutex.wait();
    lh.unlock();
    for (i = 0; i < NUM_ITEMS; ++i) {
        args->list.push(i);
    }

    return NULL;
}
}

int main() {
    pthread_t threads[NUM_THREADS];
    pthread_t consumer;
    int i(0), rc(0);
    struct thread_args args;

    alarm(60);

    args.counter = 0;

    rc = pthread_create(&consumer, NULL, launch_consumer_thread, &args);
    assert(rc == 0);

    for (i = 0; i < NUM_THREADS; ++i) {
        rc = pthread_create(&threads[i], NULL, launch_test_thread, &args);
        assert(rc == 0);
    }

    // Wait for all threads to reach the starting gate
    int counter;
    while (true) {
        LockHolder lh(args.gate);
        counter = args.counter;
        if (counter == NUM_THREADS + 1) {
            break;
        }
        args.gate.wait();
    }
    args.mutex.notify();

    for (i = 0; i < NUM_THREADS; ++i) {
        rc = pthread_join(threads[i], NULL);
        assert(rc == 0);
    }

    rc = pthread_join(consumer, NULL);
    assert(rc == 0);
    assert(args.list.empty());
}
