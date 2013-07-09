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
#ifndef RINGBUFFER_HH
#define RINGBUFFER_HH

#include <cassert>
#include <vector>
#include <algorithm>

#include "common.hh"

/**
 * A RingBuffer holds a fixed number of elements of type T.
 */
template <typename T>
class RingBuffer {
public:

    /**
     * Construct a RingBuffer to hold the given number of elements.
     */
    explicit RingBuffer(size_t s) : pos(0), max(s), wrapped(false) {
        storage = new T[max];
    }

    ~RingBuffer() {
        delete[] storage;
    }

    /**
     * How many elements are currently stored in this ring buffer?
     */
    size_t size() {
        return wrapped ? max : pos;
    }

    /**
     * Add an object to the RingBuffer.
     */
    void add(T ob) {
        if (pos == max) {
            wrapped = true;
            pos = 0;
        }
        storage[pos++] = ob;
    }

    /**
     * Remove all items.
     */
    void reset() {
        pos = 0;
        wrapped = 0;
    }

    /**
     * Copy out the contents of this RingBuffer into the a vector.
     */
    std::vector<T> contents() {
        std::vector<T> rv;
        rv.resize(size());
        size_t copied(0);
        if (wrapped && pos != max) {
            std::copy(storage + pos, storage + max, rv.begin());
            copied = max - pos;
        }
        std::copy(storage, storage + pos, rv.begin() + copied);
        return rv;
    }

private:
    T *storage;
    size_t pos;
    size_t max;
    bool wrapped;

    DISALLOW_COPY_AND_ASSIGN(RingBuffer);
};

#endif /* RINGBUFFER_HH */
