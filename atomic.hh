/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef ATOMIC_HH
#define ATOMIC_HH

#include <pthread.h>
#include <list>
#include <sched.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

#include "callbacks.hh"
#include "locks.hh"

#define MAX_THREADS 100

#if defined(HAVE_GCC_ATOMICS)
#include "atomic/gcc_atomics.h"
#elif defined(HAVE_ATOMIC_H)
#include "atomic/libatomic.h"
#else
#error "Don't know how to use atomics on your target system!"
#endif

extern "C" {
   typedef void (*ThreadLocalDestructor)(void *);
}

/**
 * Container of thread-local data.
 */
template<typename T>
class ThreadLocal {
public:
    ThreadLocal(ThreadLocalDestructor destructor = NULL) {
        int rc = pthread_key_create(&key, destructor);
        if (rc != 0) {
            fprintf(stderr, "Failed to create a thread-specific key: %s\n", strerror(rc));
            abort();
        }
    }

    ~ThreadLocal() {
        pthread_key_delete(key);
    }

    void set(const T &newValue) {
        int rc = pthread_setspecific(key, newValue);
        if (rc != 0) {
            std::stringstream ss;
            ss << "Failed to store thread specific value: " << strerror(rc);
            throw std::runtime_error(ss.str().c_str());
        }
    }

    T get() const {
        return reinterpret_cast<T>(pthread_getspecific(key));
    }

    void operator =(const T &newValue) {
        set(newValue);
    }

    operator T() const {
        return get();
    }

private:
    pthread_key_t key;
};

/**
 * Container for a thread-local pointer.
 */
template <typename T>
class ThreadLocalPtr : public ThreadLocal<T*> {
public:
    ThreadLocalPtr(ThreadLocalDestructor destructor = NULL) : ThreadLocal<T*>(destructor) {}

    ~ThreadLocalPtr() {}

    T *operator ->() {
        return ThreadLocal<T*>::get();
    }

    T operator *() {
        return *ThreadLocal<T*>::get();
    }

    void operator =(T *newValue) {
        set(newValue);
    }
};

/**
 * Holder of atomic values.
 */
template <typename T>
class Atomic {
public:

    Atomic(const T &initial = 0) {
        set(initial);
    }

    ~Atomic() {}

    T get() const {
        return value;
    }

    void set(const T &newValue) {
        value = newValue;
        ep_sync_synchronize();
    }

    operator T() const {
        return get();
    }

    void operator =(const T &newValue) {
        set(newValue);
    }

    bool cas(const T &oldValue, const T &newValue) {
        return ep_sync_bool_compare_and_swap(&value, oldValue, newValue);
    }

    T operator ++() { // prefix
        return ep_sync_add_and_fetch(&value, 1);
    }

    T operator ++(int) { // postfix
        return ep_sync_fetch_and_add(&value, 1);
    }

    T operator --() { // prefix
        return ep_sync_add_and_fetch(&value, -1);
    }

    T operator --(int) { // postfix
        return ep_sync_fetch_and_add(&value, -1);
    }

    T operator +=(T increment) {
        // Returns the new value
        return ep_sync_add_and_fetch(&value, increment);
    }

    T operator -=(T decrement) {
        return ep_sync_add_and_fetch(&value, -decrement);
    }

    T incr(const T &increment) {
        // Returns the old value
        return ep_sync_fetch_and_add(&value, increment);
    }

    T decr(const T &decrement) {
        return ep_sync_add_and_fetch(&value, -decrement);
    }

    T swap(const T &newValue) {
        T rv;
        while (true) {
            rv = get();
            if (cas(rv, newValue)) {
                break;
            }
        }
        return rv;
    }

    T swapIfNot(const T &badValue, const T &newValue) {
        T oldValue;
        while (true) {
            oldValue = get();
            if (oldValue != badValue) {
                if (cas(oldValue, newValue)) {
                    break;
                }
            } else {
                break;
            }
        }
        return oldValue;
    }

   void setIfLess(const T &newValue) {
      T oldValue = get();

      while (newValue < oldValue) {
         if (cas(oldValue, newValue)) {
            break;
         }
         oldValue = get();
      }
   }

   void setIfBigger(const T &newValue) {
      T oldValue = get();

      while (newValue > oldValue) {
         if (cas(oldValue, newValue)) {
            break;
         }
         oldValue = get();
      }
   }

private:
    volatile T value;
};

/**
 * Atomic pointer.
 *
 * This does *not* make the item that's pointed to atomic.
 */
template <typename T>
class AtomicPtr : public Atomic<T*> {
public:
    AtomicPtr(T *initial = NULL) : Atomic<T*>(initial) {}

    ~AtomicPtr() {}

    T *operator ->() {
        return Atomic<T*>::get();
    }

    T operator *() {
        return *Atomic<T*>::get();
    }

    operator bool() const {
        return Atomic<T*>::get() != NULL;
    }

    bool operator !() const {
        return  Atomic<T*>::get() == NULL;
    }
};

/**
 * A lighter-weight, smaller lock than a mutex.
 *
 * This is primarily useful when contention is rare.
 */
class SpinLock {
public:
    SpinLock() : lock(0) {}

    bool tryAcquire() {
       return ep_sync_lock_test_and_set(&lock, 1) == 0;
    }

    void acquire() {
        while (!tryAcquire()) {
            sched_yield();
        }
    }

    void release() {
        ep_sync_lock_release(&lock);
    }

private:
    volatile int lock;
    DISALLOW_COPY_AND_ASSIGN(SpinLock);
};

/**
 * Safe LockHolder for SpinLock instances.
 */
class SpinLockHolder {
public:
    SpinLockHolder(SpinLock *theLock) : sl(theLock) {
        lock();
    }

    ~SpinLockHolder() {
        unlock();
    }

    void lock() {
        sl->acquire();
        locked = true;
    }

    void unlock() {
        if (locked) {
            sl->release();
            locked = false;
        }
    }
private:
    SpinLock *sl;
    bool locked;
};

template <class T> class RCPtr;

/**
 * A reference counted value (used by RCPtr).
 */
class RCValue {
public:
    RCValue() : _rc_refcount(0) {}
    RCValue(const RCValue &) : _rc_refcount(0) {}
    ~RCValue() {}
private:
    template <class TT> friend class RCPtr;
    int _rc_incref() const {
        return ++_rc_refcount;
    }

    int _rc_decref() const {
        return --_rc_refcount;
    }

    mutable Atomic<int> _rc_refcount;
};

/**
 * Concurrent reference counted pointer.
 */
template <class C>
class RCPtr {
public:
    RCPtr(C *init = NULL) : value(init) {
        if (init != NULL) {
            static_cast<RCValue*>(value)->_rc_incref();
        }
    }

    RCPtr(const RCPtr<C> &other) : value(other.gimme()) {}

    ~RCPtr() {
        if (value && static_cast<RCValue *>(value)->_rc_decref() == 0) {
            delete value;
        }
    }

    void reset(C *newValue = NULL) {
        if (newValue != NULL) {
            static_cast<RCValue *>(newValue)->_rc_incref();
        }
        swap(newValue);
    }

    void reset(const RCPtr<C> &other) {
        swap(other.gimme());
    }

    bool cas(RCPtr<C> &oldValue, RCPtr<C> &newValue) {
        SpinLockHolder lh(&lock);
        if (value == oldValue.get()) {
            C *tmp = value;
            value = newValue.gimme();
            if (tmp != NULL &&
                static_cast<RCValue *>(tmp)->_rc_decref() == 0) {
                lh.unlock();
                delete tmp;
            }
            return true;
        }
        return false;
    }

    // safe for the lifetime of this instance
    C *get() const {
        return value;
    }

    RCPtr<C> & operator =(const RCPtr<C> &other) {
        reset(other);
        return *this;
    }

    C &operator *() const {
        return *value;
    }

    C *operator ->() const {
        return value;
    }

    bool operator! () const {
        return !value;
    }

    operator bool () const {
        return (bool)value;
    }

private:
    C *gimme() const {
        SpinLockHolder lh(&lock);
        if (value) {
            static_cast<RCValue *>(value)->_rc_incref();
        }
        return value;
    }

    void swap(C *newValue) {
        SpinLockHolder lh(&lock);
        C *tmp(value.swap(newValue));
        lh.unlock();
        if (tmp != NULL && static_cast<RCValue *>(tmp)->_rc_decref() == 0) {
            delete tmp;
        }
    }

    AtomicPtr<C> value;
    mutable SpinLock lock; // exists solely for the purpose of implementing reset() safely
};

/**
 * Efficient approximate-FIFO list optimize for concurrent writers.
 *
 * Multiple-producer threads, single consumer-thread.
 * 
 * Maintains a per-producer-thread thread-local list, push() in each thread adds to local thread list.
 * There is no pop(). Consumer thread can reap the whole list atomically.
 * 
 */
template <typename T>
class AtomicList {
public:
    AtomicList() : counter(0), numItems(0) {}

    ~AtomicList() {
        size_t i;
        for (i = 0; i < counter; ++i) {
            delete lists[i];
        }
    }

    /**
     * Place an item in the list.
     */
    void push(T value) {
        std::list<T> *l = swapList(); // steal our list
        l->push_back(value);
        ++numItems;
        l = swapList(l);
    }

    void pushList(std::list<T> &inList) {
        std::list<T> *l = swapList(); // steal our list
        numItems.incr(inList.size());
        while (!inList.empty()) {
            l->push_back(inList.front());
            inList.pop_front();
        }
        l = swapList(l);
    }

    /**
     * Grab all items from this list an place them into the provided
     * output list.
     *
     * @param outList a destination list to fill
     */
    void getAll(std::list<T> &outList) {
        std::list<T> *l(swapList()); // Grab my own list
        std::list<T> *newList(NULL);
        int count(0);

        // Will start empty unless this thread is adding stuff
        if (!l->empty()) {
            count += l->size();
            outList.splice(outList.end(), *l);
        }

        size_t c(counter);
        for (size_t i = 0; i < c; ++i) {
            // Swap with another thread
            newList = lists[i].swapIfNot(NULL, l);
            // Empty the list
            if (newList != NULL) {
                l = newList;
                if (!l->empty()) {
                    count += l->size();
                    outList.splice(outList.end(), *l);
                }
            }
        }

        l = swapList(l);
        numItems -= count;
    }

    /**
     * Pop all the items from this list and put them into the output vector instance.
     *
     * @param outVector a destination vector to fill
     */
    void toArray(std::vector<T> &outVector) {
        std::list<T> q;
        getAll(q);
        while (!q.empty()) {
            outVector.push_back(q.front());
            q.pop_front();
        }
    }

    /**
     * Get the number of lists internally maintained.
     */
    size_t getNumLists() const {
        return counter;
    }

    /**
     * True if this list is empty.
     */
    bool empty() const {
        return size() == 0;
    }

    /**
     * Return the number of listd items.
     */
    size_t size() const {
        return numItems;
    }
private:
    AtomicPtr<std::list<T> > *initialize() {
        std::list<T> *l = new std::list<T>;
        size_t i(counter++);
        lists[i] = l;
        threadList = &lists[i];
        return &lists[i];
    }

    std::list<T> *swapList(std::list<T> *newList = NULL) {
        AtomicPtr<std::list<T> > *lPtr(threadList);
        if (lPtr == NULL) {
            lPtr = initialize();
        }
        return lPtr->swap(newList);
    }

    ThreadLocalPtr<AtomicPtr<std::list<T> > > threadList;   // This thread's local list
    AtomicPtr<std::list<T> > lists[MAX_THREADS];            // Collection of lists across all the threads.
    Atomic<size_t> counter;                                 // number of lists
    Atomic<size_t> numItems;                                // Total items held by this AtomicList
    DISALLOW_COPY_AND_ASSIGN(AtomicList);
};


/**
 * Efficient approximate-FIFO list optimize for concurrent writers.
 *
 * Multiple-producer threads, single consumer-thread.
 * 
 * Maintains a per-producer-thread thread-local list, push() in each thread adds to local thread list.
 * There is no pop(). Consumer thread can reap the whole list atomically.
 * 
 */
template <class IntrusiveList, class IntrusiveNode>
class AtomicIntrusiveList {
public:
    AtomicIntrusiveList() : counter(0), numItems(0) {}

    ~AtomicIntrusiveList() {
        size_t i;
        for (i = 0; i < counter; ++i) {
            delete lists[i];
        }
    }

    /**
     * Place an item in the list.
     */
    void push(IntrusiveNode &value) {
        IntrusiveList *l = swapList(); // steal our list
        l->push_back(value);
        ++numItems;
        l = swapList(l);
    }

    void pushList(IntrusiveList &inList) {
        IntrusiveList *l = swapList(); // steal our list
        numItems.incr(inList.size());
        while (!inList.empty()) {
            l->push_back(inList.front());
            inList.pop_front();
        }
        l = swapList(l);
    }

    /**
     * Grab all items from this list an place them into the provided
     * output list.
     *
     * @param outList a destination list to fill
     */
    void getAll(IntrusiveList &outList) {
        IntrusiveList *l(swapList()); // Grab my own list
        IntrusiveList *newList(NULL);
        int count(0);

        // Will start empty unless this thread is adding stuff
        if (!l->empty()) {
            count += l->size();
            outList.splice(outList.end(), *l);
        }

        size_t c(counter);
        for (size_t i = 0; i < c; ++i) {
            // Swap with another thread
            newList = lists[i].swapIfNot(NULL, l);
            // Empty the list
            if (newList != NULL) {
                l = newList;
                if (!l->empty()) {
                    count += l->size();
                    outList.splice(outList.end(), *l);
                }
            }
        }

        l = swapList(l);
        numItems -= count;
    }

    /**
     * Get the number of lists internally maintained.
     */
    size_t getNumLists() const {
        return counter;
    }

    /**
     * True if this list is empty.
     */
    bool empty() const {
        return size() == 0;
    }

    /**
     * Return the number of listd items.
     */
    size_t size() const {
        return numItems;
    }
private:
    AtomicPtr<IntrusiveList > *initialize() {
        IntrusiveList *l = new IntrusiveList;
        size_t i(counter++);
        lists[i] = l;
        threadList = &lists[i];
        return &lists[i];
    }

    IntrusiveList *swapList(IntrusiveList *newList = NULL) {
        AtomicPtr<IntrusiveList> *lPtr(threadList);
        if (lPtr == NULL) {
            lPtr = initialize();
        }
        return lPtr->swap(newList);
    }

    ThreadLocalPtr<AtomicPtr<IntrusiveList> > threadList;   // This thread's local list
    AtomicPtr<IntrusiveList> lists[MAX_THREADS];            // Collection of lists across all the threads.
    Atomic<size_t> counter;                                 // number of lists
    Atomic<size_t> numItems;                                // Total items held by this AtomicIntrusiveList
    DISALLOW_COPY_AND_ASSIGN(AtomicIntrusiveList);
};

#endif // ATOMIC_HH
