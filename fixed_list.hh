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
private:
    void clear() {
        delete [] _data;
        _data = NULL;
    }

    void swap(int i, int j) {
        T *tmp = _data[i];
        _data[i] = _data[j];
        _data[j] = tmp;
    }

    void siftDown(int p, int __size) {
        int l;
        while ((l = hleft(p)) < __size) {
            int c = p;
            if (_comp(*_data[c], *_data[l]) < 0) {
                c = l;
            }
            l = hright(p);
            if (l < __size && _comp(*_data[c], *_data[l]) < 0) {
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
            if (_comp(*_data[p], *_data[parent]) > 0) {
                swap(p, parent);
            } else {
                return;
            }
            p = parent;
        }
    }

    void sortHeapToArray() {
        int __size = _size;
        while (__size > 1) {
            swap(__size - 1, 0);
            __size--;
            siftDown(0, __size);
        }
    }

    size_t      _size;
    size_t      _maxSize;
    Compare     _comp;
    T           **_data;
    bool        _built;

public:
    class FixedListIterator {
    public:
        FixedListIterator() : _node(NULL) {}

        // performs the pop operation
        T* operator ++(int) {
            assert(_node);
            T **curr;
            do {
                curr = _node;
                if (*curr == NULL) {
                    return NULL;
                }
            } while (!ep_sync_bool_compare_and_swap(&_node, curr, curr+1));
            return *curr;
        }

        bool operator ==(const FixedListIterator& b) {
            return (_node == b._node); // _usable equality is implied
        }

        bool operator !=(const FixedListIterator& b) {
            return (_node != b._node); // _usable equality is implied
        }

        FixedListIterator swap(const FixedListIterator& b) {
            assert(b._node);
            T **curr;
            do {
                curr = _node;
            } while (!ep_sync_bool_compare_and_swap(&_node, curr, b._node));
            return FixedListIterator(curr);
        }

    private:
        FixedListIterator(T **node) : _node(node) {}

        friend class FixedList;

        T   **_node;
    };

    typedef FixedListIterator iterator;

    FixedList(size_t maxSize, const Compare& comp = Compare()) : _data(NULL) { // Set _data to NULL to ensure safe call to clear()
        init(maxSize, comp);
    }

    ~FixedList() {
        clear();
    }

    // Initialize (equivalent to destructing and constructing again)
    void init(size_t maxSize, const Compare& comp = Compare()) {
        clear();
        _comp = comp;
        _maxSize = maxSize;
        _size = 0;
        _data = new T*[maxSize + 1]; // the last one is potentially a tail (when array is full)
        _built = false;
    }

    std::list<T*> *insert(std::list<T*> &l) {
        std::list<T*> *ret = new std::list<T*>;
        T *dummy;
        typename std::list<T*>::iterator it;
        for (it = l.begin(); it != l.end(); it++) {
            if ((dummy = insert(*it)) != NULL) {
                ret->push_back(dummy);
            }
        }
        return ret;
    }

    void build() {
        sortHeapToArray();
        _data[_size] = NULL;
        _built = true;
    }

    size_t size() {
        return _size;
    }

    size_t memSize() {
        return sizeof(FixedList) + (_data ? (_maxSize + 1) * sizeof(T*) : 0);
    }

    void checkSanity() {
        assert(_size <= _maxSize);
        if (_built) {
            // check sorted list validity
            for (size_t i = 0; i < _size-1; i++) {
                assert(_comp(*_data[i], *_data[i+1]) <= 0);
            }
        } else {
            // check heap validity
            for (size_t i = 0; i < _size; i++) {
                size_t c = hleft(i);
                if (c < _size) {
                    assert(_comp(*_data[i], *_data[c]) >= 0);
                } else {
                    break;
                }
                c = hright(i);
                if (c < _size) {
                    assert(_comp(*_data[i], *_data[c]) >= 0);
                }
            }
        }
    }

    int numLessThan(T *compareTo) {
        assert(_built);
        int l = 0, r = _size, m;
        while (l < r) {
            m = (l+r) / 2;
            if (_comp(*_data[m], *compareTo) < 0) {
                l = m + 1;
            } else {
                r = m;
            }
        }
        return l;
    }

    int numGreaterThan(T *compareTo) {
        return _size - (numLessThan(compareTo));
    }

    T* insert(T *data) {
        assert(!_built);
        if (_maxSize == 0) {
            return NULL;
        }
        T* ret = NULL;
        if (_size < _maxSize) {
            _data[_size++] = data;
            siftUp(_size - 1);
        } else if (_comp(*_data[0], *data) > 0) {
            ret = _data[0];
            _data[0] = data;
            siftDown(0, _size);
        }
        return ret;
    }

    T* last() {
        assert(_size);
        return _data[_built ? _size-1 : 0];
    }

    T* first() {
        assert(_size);
        return _data[0];
    }

    iterator end() {
        assert(_built);
        return iterator(&_data[_size]);
    }

    iterator begin() {
        assert(_built);
        return iterator(_data);
    }
};

#endif // FIXED_LIST_HH
