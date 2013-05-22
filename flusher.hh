/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef FLUSHER_H
#define FLUSHER_H 1

#include "common.hh"
#include "ep.hh"
#include "dispatcher.hh"
#include "kvstore-mapper.hh"
#include "flushlist.hh"

enum flusher_state {
    initializing,
    running,
    pausing,
    paused,
    stopping,
    stopped
};

class Flusher;

const double DEFAULT_MIN_SLEEP_TIME = 0.1;

/**
 * A DispatcherCallback adaptor over Flusher.
 */
class FlusherStepper : public DispatcherCallback {
public:
    FlusherStepper(Flusher* f) : flusher(f) { }
    bool callback(Dispatcher &d, TaskId t);

    std::string description(); 

    hrtime_t maxExpectedDuration() {
        // Flusher can take a while, but let's report if it runs for
        // more than ten minutes.
        return 10 * 60 * 1000 * 1000;
    }

private:
    Flusher *flusher;
};

/*
 * FlusherHelper
 *
 * A helper thread to execute beginFlush in parallel to regular flushSome.
 *
 * This thread holds a flushList that it prepared.
 * When it's corresponding flusher takes that list for further processing,
 * this thread goes and prepares next list in parallel and keeps it ready for next run.  
 */
class FlusherHelper {
public:
    FlusherHelper(int kv, EventuallyPersistentStore *st) :
        kvid(kv), store(st), flushList(NULL), sync() {
    }

    ~FlusherHelper() {
        if (flushList != NULL) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "FlusherHelper dying with %d items in the flushList\n",
                             flushList->size());
            delete flushList;
        }
        wakeup();
        pthread_join(myId, NULL);
    }

    FlushList* getFlushQueue() {
        LockHolder lh(sync);
        if (!flushList) {
            sync.notify();
            return NULL;
        }
        FlushList *ret = flushList;
        flushList = NULL;
        sync.notify();
        return ret;
    }

    bool more() {
        LockHolder lh(sync);
        return flushList != NULL;
    }

    void wakeup() {
        LockHolder lh(sync);
        sync.notify();
        return;
    }
        
    void start();
    void run();
    void retrievePendingItems(std::list<Item*> &out);

private:
    int kvid;
    pthread_t myId;
    EventuallyPersistentStore *store;
    FlushList *flushList;
    SyncObject sync;
};

/**
 * Manage persistence of data for an EventuallyPersistentStore.
 */
class Flusher {
public:

    Flusher(EventuallyPersistentStore *st, Dispatcher *d, int i) :
        store(st), _state(initializing), dispatcher(d), flusherId(i),
        flushRv(0), prevFlushRv(0), minSleepTime(0.1),
        flushList(NULL), rejectList(NULL), vbStateLoaded(false),
        forceShutdownReceived(false), pauseCounter(0), last_min_data_age(-1),
        last_queue_age_cap(-1), shouldFlushAll(false) {
            gettimeofday(&waketime, NULL);
            helper = new FlusherHelper(flusherId, store);
        }

    ~Flusher() {
        if (_state != stopped) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Flusher being destroyed in state %s\n",
                             stateName(_state));

        }
        if (rejectList != NULL) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Flusher being destroyed with %d tasks in the reject queue\n",
                             rejectList->size());
            delete rejectList;
        }

        delete helper;
    }

    bool stop(bool isForceShutdown = false);
    void wait(enum flusher_state to = stopped);
    bool pause();
    bool resume();

    void initialize(TaskId);

    void start(void);
    void wake(void);
    bool step(Dispatcher&, TaskId);

    bool isVBStateLoaded() const {
        return vbStateLoaded.get();
    }

    enum flusher_state state() const;
    const char * stateName() const;

    void setFlushAll(bool flag) {
        shouldFlushAll = flag;
    }

    int getFlusherId() { return flusherId; }

    void retrievePendingItems(std::list<Item*> &out) const;

private:
    bool transition_state(enum flusher_state to);
    int doFlush();
    void setupFlushQueues();
    void flushAllPending();
    void schedule_UNLOCKED();
    double computeMinSleepTime();

    EventuallyPersistentStore *store;
    volatile enum flusher_state _state;
    Mutex taskMutex;
    TaskId task;
    Dispatcher *dispatcher;
    const char * stateName(enum flusher_state st) const;
    int flusherId;

    // Current flush cycle state.
    int                      flushRv;
    int                      prevFlushRv;
    double                   minSleepTime;
    FlushList   *flushList;
    FlushList   *rejectList;
    rel_time_t               flushStart;
    Atomic<bool>             vbStateLoaded;
    Atomic<bool>             forceShutdownReceived;
    Atomic<int>              pauseCounter;
    Mutex                    pauseLock;
    timeval                  waketime;
    int                      last_min_data_age;
    int                      last_queue_age_cap;
    FlusherHelper            *helper;
    bool                     shouldFlushAll;

    DISALLOW_COPY_AND_ASSIGN(Flusher);
};
#endif /* FLUSHER_H */
