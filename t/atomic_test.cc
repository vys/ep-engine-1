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
#include <cassert>
#include <vector>
#include <algorithm>
#include <unistd.h>

#include "atomic.hh"
#include "threadtests.hh"

const size_t numThreads    = 100;
const size_t numIterations = 1000;

class AtomicIntTest : public Generator<int> {
public:

    AtomicIntTest(int start=0) : i(start) {}

    int operator()() {
        for (size_t j = 0; j < numIterations-1; j++) {
           ++i;
        }
        return ++i;
    }

    int latest(void) { return i.get(); }

private:
    Atomic<int> i;
};

static void testAtomicInt() {
    AtomicIntTest intgen;
    // Run this test with 100 concurrent threads.
    std::vector<int> r(getCompletedThreads<int>(numThreads, &intgen));

    // We should have 100 distinct numbers.
    std::sort(r.begin(), r.end());
    std::unique(r.begin(), r.end());
    assert(r.size() == numThreads);

    // And the last number should be (numThreads * numIterations)
    assert(intgen.latest() == (numThreads * numIterations));
}

static void testSetIfLess() {
    Atomic<int> x;

    x.set(842);
    x.setIfLess(924);
    assert(x.get() == 842);
    x.setIfLess(813);
    assert(x.get() == 813);
}

static void testSetIfBigger() {
    Atomic<int> x;

    x.set(842);
    x.setIfBigger(13);
    assert(x.get() == 842);
    x.setIfBigger(924);
    assert(x.get() == 924);
}

int main() {
    alarm(60);
    testAtomicInt();
    testSetIfLess();
    testSetIfBigger();
}
