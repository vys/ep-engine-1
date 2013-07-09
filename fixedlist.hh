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
#ifndef FIXED_LIST_HH
#define FIXED_LIST_HH

#include "atomic.hh"
#include <list>

#define hleft(i)   (2*i + 1)   // Maintain:
#define hright(i)  (2*i + 2)   // 1. hleft < hright for all i
#define hparent(i) ((i-1) / 2) // 2. hleft and hright are increasing functions

template <typename T>
class less {
public:
    int operator () (T &a, T &b) {
        return (a < b ? -1 : (b < a ? 1 : 0));
    }
};

template <typename T, typename Compare = less<T> >
class FixedList {
public:
    class FixedListIterator {
    public:
        FixedListIterator() : node(NULL) {}

        // performs the pop operation
        T* operator ++(int) {
            assert(node);
            T **curr;
            do {
                curr = node;
                if (*curr == NULL) {
                    return NULL;
                }
            } while (!ep_sync_bool_compare_and_swap(&node, curr, curr+1));
            return *curr;
        }

        T* operator *() {
            return *node;
        }

        bool operator ==(const FixedListIterator& b) {
            return (node == b.node);
        }

        bool operator !=(const FixedListIterator& b) {
            return (node != b.node);
        }

        FixedListIterator swap(const FixedListIterator& b) {
            assert(b.node);
            T **curr;
            do {
                curr = node;
            } while (!ep_sync_bool_compare_and_swap(&node, curr, b.node));
            return FixedListIterator(curr);
        }

    private:
        FixedListIterator(T **n) : node(n) {}

        friend class FixedList;

        T   **node;
    };

    typedef FixedListIterator iterator;

    FixedList(size_t sz, const Compare& comp = Compare()) : data(NULL) { // Set data to NULL to ensure safe call to clear()
        init(sz, comp);
    }

    ~FixedList() {
        clear();
    }

    // Initialize (equivalent to destructing and constructing again)
    void init(size_t sz, const Compare& comp = Compare()) {
        clear();
        comparator = comp;
        maxSize = sz;
        currentSize = 0;
        data = new T*[maxSize + 1]; // the last one is potentially a tail (when array is full)
        memset(data, 0, sizeof(T*) * (maxSize + 1));
        built = false;
        fresh = false;
    }

    // Reset values without reallocating array
    void reset() {
        data[currentSize] = data[0];
        data[0] = NULL;
        currentSize = 0;
        built = false;
        fresh = false;
    }

    /**
     * Inserts items from l and leaves behind all items that were either rejected
     * or removed from the heap till it reaches endIter
     * Note: It is impossible to have more items in the final reject list than in the
     * original list. This leaves gaps in the list that are set to NULL.
     */
    void insert(std::list<T*> &l, typename std::list<T*>::iterator &endIter) {
        typename std::list<T*>::iterator it;
        for (it = l.begin(); it != endIter; it++) {
            *it = insert(*it);
        }
    }

    void build() {
        sortHeapToArray();
        built = true;
        fresh = true;
    }

    size_t size() {
        return currentSize;
    }

    size_t getMaxSize() {
        return maxSize;
    }

    size_t memSize() {
        return sizeof(FixedList) + (data ? (maxSize + 1) * sizeof(T*) : 0);
    }

    void checkSanity() {
        assert(currentSize <= maxSize);
        if (built) {
            // check sorted list validity
            for (size_t i = 0; i < currentSize-1; i++) {
                assert(comparator(*data[i], *data[i+1]) <= 0);
            }
        } else {
            // check heap validity
            for (size_t i = 0; i < currentSize; i++) {
                size_t c = hleft(i);
                if (c < currentSize) {
                    assert(comparator(*data[i], *data[c]) >= 0);
                } else {
                    break;
                }
                c = hright(i);
                if (c < currentSize) {
                    assert(comparator(*data[i], *data[c]) >= 0);
                }
            }
        }
    }

    int numLessThan(T *compareTo) {
        assert(built);
        int l = 0, r = currentSize, m;
        while (l < r) {
            m = (l+r) / 2;
            if (comparator(*data[m], *compareTo) < 0) {
                l = m + 1;
            } else {
                r = m;
            }
        }
        return l;
    }

    int numGreaterThan(T *compareTo) {
        return currentSize - (numLessThan(compareTo));
    }

    /**
     * Returns the rejected item or NULL if none
     */
    T* insert(T *item) {
        assert(!built);
        if (maxSize == 0) {
            return item;
        }
        T* ret = NULL;
        if (currentSize < maxSize) {
            data[currentSize++] = item;
            siftUp(currentSize - 1);
            ret = data[currentSize];
            data[currentSize] = NULL;
        } else if (comparator(*data[0], *item) > 0) {
            ret = data[0];
            data[0] = item;
            siftDown(0, currentSize);
        } else {
            ret = item;
        }
        return ret;
    }

    bool isBuilt() {
        return built;
    }

    bool isFresh() {
        return fresh;
    }

    void setFresh(bool to = true) {
        fresh = to;
    }

    T* first() {
        if (currentSize == 0 || !built) {
            return NULL;
        }
        return data[0];
    }

    T* last() {
        if (currentSize == 0) {
            return NULL;
        }
        return data[built ? currentSize-1 : 0];
    }

    iterator end() {
        assert(built);
        return iterator(&data[currentSize]);
    }

    iterator begin() {
        assert(built);
        return iterator(data);
    }

    T** getArray() {
        return data;
    }

private:
    void clear() {
        delete [] data;
        data = NULL;
    }

    void swap(int i, int j) {
        T *tmp = data[i];
        data[i] = data[j];
        data[j] = tmp;
    }

    void siftDown(int p, int s) {
        int l;
        while ((l = hleft(p)) < s) {
            int c = p;
            if (comparator(*data[c], *data[l]) < 0) {
                c = l;
            }
            l = hright(p);
            if (l < s && comparator(*data[c], *data[l]) < 0) {
                c = l;
            }
            if (c == p) {
                return;
            } else {
                swap(c, p);
                p = c;
            }
        }
    }

    void siftUp(int p) {
        while (p != 0) {
            int parent = hparent(p);
            if (comparator(*data[p], *data[parent]) > 0) {
                swap(p, parent);
            } else {
                return;
            }
            p = parent;
        }
    }

    void sortHeapToArray() {
        int s = currentSize;
        while (s > 1) {
            swap(s - 1, 0);
            s--;
            siftDown(0, s);
        }
    }

    size_t      currentSize;
    size_t      maxSize;
    Compare     comparator;
    T           **data;
    bool        built;
    bool        fresh;
};

#endif // FIXED_LIST_HH
