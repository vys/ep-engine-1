#ifndef EVICTION_HH
#define EVICTION_HH 1

#include <set>

#include<ep.hh>
#include<fixed_list.hh>
#include "rss.hh"

#define MAX_EVICTION_ENTRIES 500000

#define MIN_EVICTION_QUANTUM_SIZE 524288 // 512KB
#define MAX_EVICTION_QUANTUM_SIZE 33554432 // 32MB

#define MIN_EVICTION_QUANTUM_MAX_COUNT 2
#define MAX_EVICTION_QUANTUM_MAX_COUNT 32

//Generic class for identifying evictable items.
class EvictItem {
public:
    EvictItem(StoredValue *v, uint16_t vb) : key(v->getKey()), vbid(vb) {}

    EvictItem() : key(""), vbid(0) {}

    ~EvictItem() {}

    void increaseCurrentSize(EPStats &st) {
        size_t by = sizeof(EvictItem) + key.size();
        st.evictionStats.memSize.incr(by);
    }

    void reduceCurrentSize(EPStats &st) {
        size_t by = sizeof(EvictItem) + key.size();
        st.evictionStats.memSize.decr(by);
    }

    std::string const getKey() {
        return key;
    }

    uint16_t vbucketId() const {
        return vbid;
    }

protected:
    std::string key;
    uint16_t vbid;
};

class EvictionPolicy {
public:
    EvictionPolicy(EventuallyPersistentStore *s, EPStats &st, bool job,
                   bool inlineEviction) :
                   backgroundJob(job), supportsInlineEviction(inlineEviction),
                   store(s), stats(st), age(0) {}

    virtual ~EvictionPolicy() {}

    // Inline eviction method. Called during front-end operations.
    virtual EvictItem* getOneEvictItem() = 0;

    virtual std::string description () const = 0;
    virtual void getStats(const void *cookie, ADD_STAT add_stat) = 0;

    time_t evictAge() { return age; }

    void evictAge(time_t val) {age = val; }

    bool evictItemByAge(time_t age, StoredValue *v, RCPtr<VBucket> vb);

    // Following set of functions are needed only by policies that need a
    // background job to build their data structures.
    virtual void setSize(size_t val) = 0;
    virtual void initRebuild() = 0;
    virtual bool addEvictItem(StoredValue *v, RCPtr<VBucket>) = 0;
    virtual bool storeEvictItem() = 0;
    virtual void completeRebuild() = 0;

    virtual bool evictionJobNeeded(time_t lruSleepTime) = 0;
    virtual bool eligibleForEviction(StoredValue *v, EvictItem *e) {
        (void)v;
        (void)e;
        return true;
    }
    bool backgroundJob;

    bool supportsInlineEviction;

protected:
    EventuallyPersistentStore *store;
    EPStats &stats;

private:
    time_t age;
};

// Timing stats for the policies that use background job.
class BGTimeStats {
public:
    BGTimeStats() : startTime(0), endTime(0) {}

    void getStats(const void *cookie, ADD_STAT add_stat);

    Histogram<hrtime_t> completeHisto;
    time_t startTime;
    time_t endTime;
};

class LRUItem : public EvictItem {
public:
    LRUItem(StoredValue *v, uint16_t vb, time_t t) : EvictItem(v, vb), timestamp(t) {}

    LRUItem(time_t t) : EvictItem(), timestamp(t) {}

    ~LRUItem() {}

    void increaseCurrentSize(EPStats &st) {
        size_t by = sizeof(LRUItem) + key.size();
        st.evictionStats.memSize.incr(by);
    }

    void reduceCurrentSize(EPStats &st) {
        size_t by = sizeof(LRUItem) + key.size();
        st.evictionStats.memSize.decr(by);
    }

    int getAttr() const {
        return timestamp;
    }

private:
    time_t timestamp;
};

class LRUItemCompare {
public:
    int operator () (LRUItem &a, LRUItem &b) {
        if (a.getAttr() < b.getAttr()) {
            return -1;
        } else if (b.getAttr() < a.getAttr()) {
            return 1;
        }
        return 0;
    }
};

//Implementation of LRU based eviction policy
class LRUPolicy : public EvictionPolicy {
public:
    LRUPolicy(EventuallyPersistentStore *s, EPStats &st, bool job, size_t sz) :
              EvictionPolicy(s, st, job, true), maxSize(sz),
              activeList(new LRUFixedList(maxSize)),
              inactiveList(new LRUFixedList(maxSize)),
              tempList(new LRUFixedList(maxSize)),
              stopBuild(false),
              numEvictableItems(0),
              freshStart(false),
              oldest(0),
              newest(0),
              lastRun(ep_real_time()) {
        activeList->build();
        it = activeList->begin();
        stats.evictionStats.memSize.incr(activeList->memSize() + inactiveList->memSize() + tempList->memSize());
        // this assumes that three pointers are used per node of activeList
        stats.evictionStats.memSize.incr(3 * sizeof(int*));
    }

    ~LRUPolicy() {
        clearStage(true);
        // this assumes that three pointers are used per node of activeList
        stats.evictionStats.memSize.decr(3 * sizeof(int*));

        clearLRUFixedList(activeList);
        clearLRUFixedList(inactiveList);
        clearLRUFixedList(tempList);
        stats.evictionStats.memSize.decr(activeList->memSize() + inactiveList->memSize() + tempList->memSize());
        delete activeList;
        delete inactiveList;
        delete tempList;
    }

    LRUItemCompare lruItemCompare;

    size_t getNumEvictableItems() {
        return numEvictableItems.get();
    }

    size_t getActiveListSize() {
        // activeList will never be NULL after the first iteration and doesn't require a lock
        if (activeList) {
            return activeList->size();
        } else {
            return 0;
        }
    }

    size_t getInactiveListSize() {
        LockHolder lh(swapLock); // inactiveList access requires a lock
        if (inactiveList) {
            return inactiveList->size();
        } else {
            return 0;
        }
    }

    void setSize(size_t val) {
        if (maxSize != val) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL, "Eviction: Setting max entries to %zu", maxSize);
        }
        maxSize = val;
    }

    void initRebuild();

    bool addEvictItem(StoredValue *v, RCPtr<VBucket> currentBucket);

    bool storeEvictItem();

    void completeRebuild();

    bool switchActiveListIter(bool forceLock = false) {
        bool lock = false;
        LockHolder lh(swapLock, forceLock ? NULL : &lock);
        if (forceLock || lock) {
            if (inactiveList->isFresh()) {
                it.swap(inactiveList->begin());

                LRUFixedList *tmp = activeList;
                activeList = inactiveList;
                inactiveList = tmp;

                inactiveList->setFresh(false);

                numEvictableItems = activeList->size();
                stats.evictionStats.numActiveListItems = activeList->size();
                stats.evictionStats.numInactiveListItems = 0;

                return true;
            }
        }
        return false;
    }

    EvictItem* getOneEvictItem() {
        LRUItem *evictItem = it++;
        if (evictItem == NULL) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL, "Eviction: could not get item from activeList");
            if (switchActiveListIter()) {
                stats.evictionStats.frontendSwaps++;
                evictItem = it++;
                if (evictItem == NULL) {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL, "Eviction: just switched activeList, but got no item from new activeList as well!!");
                    return NULL;
                }
            } else {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL, "Eviction: switchActiveList failed!");
                return NULL;
            }
        }

        numEvictableItems--;
        stats.evictionStats.numActiveListItems--;
        evictItem->reduceCurrentSize(stats);
        return static_cast<EvictItem *>(evictItem);
    }

    bool evictionJobNeeded(time_t lruSleepTime) {
        if (lruSleepTime == 0) {
            return false;
        }
        size_t mem_used = GetSelfRSS();
        size_t max_size = StoredValue::getMaxDataSize(stats);
        if (mem_used < (size_t)(memThresholdPercent * max_size)) {
            return false;
        }
        time_t currTime = ep_real_time();
        if (lastRun + lruSleepTime <= currTime) {
            freshStart = true;
            lastRun = currTime;
            return true;
        }

        if (evictAge() != 0) {
            freshStart = true;
            return true;
        }

        size_t target = (size_t)(rebuildPercent * activeList->size());
        if (!inactiveList->isFresh() || numEvictableItems <= target) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL, "evictionJobNeeded: target=%zu, inactiveList->isFresh is %d", target, inactiveList->isFresh() ? 1 : 0);
            return true;
        }
        return false;
    }

    bool eligibleForEviction(StoredValue *v, EvictItem *e) {
        LRUItem *l = static_cast<LRUItem*>(e);
        return (int(v->getDataAge()) <= l->getAttr());
    }

    std::string description() const { return std::string("lru"); }

    void getStats(const void *cookie, ADD_STAT add_stat);

    Histogram<int> &getLruHisto() {
        return lruHisto;
    }

    static void setRebuildPercent(double v) {
        rebuildPercent = v;
    }

    static void setMemThresholdPercent(double v) {
        memThresholdPercent = v;
    }

    typedef FixedList<LRUItem, LRUItemCompare> LRUFixedList;
    typedef FixedList<LRUItem, LRUItemCompare>::iterator LRUFixedListIter;

private:

    // Generate histogram representing distribution of all the keys in the
    // LRU queue with respect to their age.
    void genLruHisto() {
        lruHisto.reset();
        time_t cur = ep_current_time();
        int total = 0;
        // Handle keys that are not accessed since startup. Those keys have
        // the timestamp as 0 and cannot be used with a relative timestamp.
        if (inactiveList->size() && (*inactiveList->begin())->getAttr() == 0) {
            LRUItem l(1);
            total = inactiveList->numLessThan(&l);
            lruHisto.add(cur, total);
        }
        int remaining = inactiveList->size() - total;
        int i = 1;
        total = 0;
        while (total < remaining) {
            time_t t = cur - 2 * i;
            LRUItem l(t);
            int curtotal = inactiveList->numGreaterThan(&l);
            lruHisto.add(2 * i, curtotal - total);
            total = curtotal;
            i++;
        }
    }

    void clearLRUFixedList(LRUFixedList *l) {
        if (l) {
            int listSize = l->size();
            LRUItem** array = l->getArray();
            for (int i = 0; i < listSize; i++) {
                LRUItem *item = array[i];
                item->reduceCurrentSize(stats);
                delete item;
            }
            l->reset();
        }
    }

    void clearStage(bool deleteItems = false) {
        // this assumes that three pointers are used per node of activeList
        stats.evictionStats.memSize.decr(stage.size() * 3 * sizeof(int*));
        if (deleteItems) {
            for (std::list<LRUItem*>::iterator iter = stage.begin(); iter != stage.end(); iter++) {
                LRUItem *item = *iter;
                item->reduceCurrentSize(stats);
                delete item;
            }
        }
        stage.clear();
    }

    size_t maxSize;
    std::list<LRUItem*> stage;
    LRUFixedList *activeList;
    LRUFixedList *inactiveList;
    LRUFixedListIter it;
    LRUFixedList *tempList;
    BGTimeStats timestats;
    Histogram<int> lruHisto;
    time_t startTime, endTime;
    bool stopBuild;
    Atomic<size_t> numEvictableItems;
    Mutex swapLock;
    bool freshStart;
    time_t oldest;
    time_t newest;
    static double rebuildPercent;
    static double memThresholdPercent;
    time_t lastRun;
};

/*
 * Implementation of a simple policy that does FIFO based eviction.
 * It walks the hash table and uses the first 'n' elements for eviction.
 */
class RandomPolicy : public EvictionPolicy {
    class RandomList {
    private:
        class RandomNode {
        public:
            RandomNode(EvictItem* _data = NULL) : data(_data), next(NULL) {}
            ~RandomNode() {}

            EvictItem *data;
            RandomNode *next;
        };

    public:
        RandomList() : head(new RandomNode) {}

        ~RandomList() {
            while (head != NULL) {
                RandomNode *next = head->next;
                delete head;
                head = next;
            }
        }

        class RandomListIterator {
        public:
            RandomListIterator(RandomNode *node = NULL) : _node(node) {}

            EvictItem* operator *() {
                assert(_node && _node->data);
                return _node->data;
            }

            EvictItem* operator ++() {
                RandomNode *old;
                do {
                    old = _node;
                    if (old->next == NULL) {
                        return NULL;
                    }
                } while (!ep_sync_bool_compare_and_swap(&_node, old, old->next));
                return old->data;
            }

            RandomNode *swap(RandomListIterator &it) {
                RandomNode *old;
                do {
                    old = _node;
                } while (!ep_sync_bool_compare_and_swap(&_node, old, it._node));
                return old;
            }
        private:
            RandomNode *_node;
        };

        typedef RandomListIterator iterator;

        iterator begin() {
            return iterator(head);
        }

        void add(EvictItem *item) {
            RandomNode *n = new RandomNode(item);
            n->next = head;
            head = n;
        }

        static size_t nodeSize() {
            return sizeof(RandomNode);
        }

    private:
        RandomNode *head;
    };

public:
    RandomPolicy(EventuallyPersistentStore *s, EPStats &st, bool job, size_t sz)
        :   EvictionPolicy(s, st, job, true),
            maxSize(sz),
            activeList(new RandomList()),
            it(activeList->begin()),
            stopBuild(false) {
        stats.evictionStats.memSize.incr(sizeof(RandomList) + RandomList::nodeSize());
    }

    ~RandomPolicy() {
        clearTemplist();
        if (activeList) {
            EvictItem *node;
            while ((node = ++it) != NULL) {
                node->reduceCurrentSize(stats);
                delete node;
            }
            stats.evictionStats.memSize.decr(queueSize.get() * RandomList::nodeSize());
            delete activeList;
        }
    }

    void setSize(size_t val) {
        maxSize = val;
    }

    void initRebuild();

    bool addEvictItem(StoredValue *v,RCPtr<VBucket> currentBucket);

    bool storeEvictItem();

    void completeRebuild();

    EvictItem *getOneEvictItem() {
        EvictItem *evictItem = ++it;
        if (evictItem == NULL) {
            return NULL;
        }
        evictItem->reduceCurrentSize(stats);
        stats.evictionStats.memSize.decr(RandomList::nodeSize());
        queueSize--;
        return evictItem;
    }

    bool evictionJobNeeded(time_t lruSleepTime) {
        (void)lruSleepTime;
        return true;
    }

    void getStats(const void *cookie, ADD_STAT add_stat);

    std::string description() const { return std::string("random"); }

private:

    void clearTemplist() {
        if (tempList) {
            EvictItem *node;
            RandomList::iterator tempit = tempList->begin();
            int c = 0;
            while ((node = ++tempit) != NULL) {
                c++;
                node->reduceCurrentSize(stats);
                delete node;
                stats.evictionStats.memSize.decr(RandomList::nodeSize());
            }
            stats.evictionStats.memSize.decr((c+1) * RandomList::nodeSize());
            delete tempList;
            tempList = NULL;
        }
    }

    size_t maxSize;
    RandomList *activeList;
    RandomList *tempList;
    RandomList::iterator it;
    size_t size;
    Atomic<size_t> queueSize;
    BGTimeStats timestats;
    time_t startTime, endTime;
    bool stopBuild;
};

// Background eviction policy to mimic the item-pager behaviour based on
// memory watermarks.
class BGEvictionPolicy : public EvictionPolicy {
public:
    BGEvictionPolicy(EventuallyPersistentStore *s, EPStats &st, bool job) :
                     EvictionPolicy(s, st, job, false), shouldRun(true),
                     ejected(0), startTime(0), endTime(0) {}

    ~BGEvictionPolicy() {}

    void setSize(size_t val) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "No use of size %d", val);
    }

    void initRebuild() {
        startTime = ep_real_time();
    }

    bool addEvictItem(StoredValue *v, RCPtr<VBucket> currentBucket) {
        double r = static_cast<double>(std::rand()) / static_cast<double>(RAND_MAX);
        if (toKill >= r) {
            if (!v->eligibleForEviction()) {
                ++stats.numFailedEjects;
                return false;
            }
            // Check if the key with its CAS value exists in the open or closed referenced
            //checkpoints.
            bool foundInCheckpoints =
                currentBucket->checkpointManager.isKeyResidentInCheckpoints(v->getKey(),
                                                                            v->getCas());
            if (!foundInCheckpoints && v->ejectValue(stats, currentBucket->ht)) {
                if (currentBucket->getState() == vbucket_state_replica) {
                    ++stats.numReplicaEjects;
                }
                ++ejected;
            }
        }
        return true;
    }

    bool storeEvictItem() {
        // Background eviction does not stop till it has completed the hash walk
        return true;
    }

    void completeRebuild() {
        endTime = ep_real_time();
        toKill = 0;
        timestats.startTime = startTime;
        timestats.endTime = endTime;
    }

    EvictItem* getOneEvictItem() {
        // No evictions in the front-end op
        return NULL;
    }

    bool evictionJobNeeded(time_t lruSleepTime) {
        (void)lruSleepTime;
        if (evictAge() != 0) {
            return true;
        }
        double current = static_cast<double>(StoredValue::getCurrentSize(stats));
        double upper = static_cast<double>(stats.mem_high_wat);
        double lower = static_cast<double>(stats.mem_low_wat);

        if (current > upper) {
            toKill = (current - static_cast<double>(lower)) / current;
            return true;
        } else {
            return false;
        }
    }

    void getStats(const void *cookie, ADD_STAT add_stat);

    std::string description() const { return std::string("bgeviction"); }

private:
    double toKill;
    bool shouldRun;
    size_t ejected;
    BGTimeStats timestats;
    time_t startTime, endTime;
};

class EvictionPolicyFactory {
public:
    static EvictionPolicy *getInstance(std::string desc, EventuallyPersistentStore *s,
                                       EPStats &st, size_t sz) {
        EvictionPolicy *p = NULL;
        if (desc == "lru") {
            p = new LRUPolicy(s, st, true, sz);
        } else if (desc == "random") {
            p = new RandomPolicy(s, st, true, sz);
        } else if (desc == "bgeviction") {
            p = new BGEvictionPolicy(s, st, true);
        } else {
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Invalid policy name");
        }
        return p;
    }
};

class EvictionManager {
public:

    static EvictionManager *getInstance() {
        return managerInstance;
    }

    static void createInstance(EventuallyPersistentStore *s, EPStats &st, std::string p, size_t headroom, bool disableInlineEviction) {
        if (managerInstance == NULL) {
            managerInstance = new EvictionManager(s, st, p, headroom, disableInlineEviction);
        }
    }

    static void destroy() {
        delete managerInstance;
        managerInstance = NULL;
    }

    ~EvictionManager() {
        delete evpolicy;
    }

    bool setPolicy(const char *name) {
        if (policies.find(name) != policies.end()) {
            policyName = name;
            return true;
        } else {
            return false;
        }
    }

    std::set<std::string>& getPolicyNames() {
        return policies;
    }

    EvictionPolicy *getCurrentPolicy() {
        return evpolicy;
    }

    EvictionPolicy *evictionBGJob();

    bool evictHeadroom();

    int getMaxSize(void) {
        return maxSize;
    }

    bool enableJob(void) {
        return !pauseJob;
    }

    void enableJob(bool doit) {
        pauseJob = !doit;
    }

    void setMaxSize(int val) {
        maxSize = val;
    }

    void setPruneAge(time_t val) {
        pruneAge = val;
    }

    size_t getEvictionQuantumSize() {
        return evictionQuantumSize;
    }

    void setEvictionQuantumSize(size_t to) {
        if (to < MIN_EVICTION_QUANTUM_SIZE || to > MAX_EVICTION_QUANTUM_SIZE) {
            std::stringstream ss;
            ss << "New eviction_quantum_size param value " << to
                << " is not ranged between the min allowed value " << MIN_EVICTION_QUANTUM_SIZE
                << " and max value " << MAX_EVICTION_QUANTUM_SIZE;
            getLogger()->log(EXTENSION_LOG_WARNING, NULL, ss.str().c_str());
            return;
        }
        evictionQuantumSize = to;
    }

    size_t getEvictionQuantumMaxCount() {
        return evictionQuantumMaxCount;
    }

    void setEvictionQuantumMaxCount(size_t to) {
        if (to < MIN_EVICTION_QUANTUM_MAX_COUNT || to > MAX_EVICTION_QUANTUM_MAX_COUNT) {
            std::stringstream ss;
            ss << "New eviction_quantum_max_count param value " << to
                << " is not ranged between the min allowed value " << MIN_EVICTION_QUANTUM_MAX_COUNT
                << " and max value " << MAX_EVICTION_QUANTUM_MAX_COUNT;
            getLogger()->log(EXTENSION_LOG_WARNING, NULL, ss.str().c_str());
            return;
        }
        evictionQuantumMaxCount = to;
    }

    bool allowOps(size_t mem) {
        return mem < StoredValue::getMaxDataSize(stats);
    }

    size_t getEvictionHeadroom() {
        return headroom;
    }

    void setEvictionHeadroom(size_t to) {
        headroom = to;
    }

    uint32_t getEvictionQuietWindow() {
        return evictionQuietWindow;
    }

    bool getEvictionDisable() {
        return disableInlineEviction;
    }

    void setEvictionDisable(bool doit) {
        disableInlineEviction = doit;
    }

    static void setMinBlobSize(size_t ss) {
        minBlobSize = ss;
    }

    static size_t getMinBlobSize() {
        return minBlobSize;
    }

private:
    static EvictionManager *managerInstance;
    static Atomic<size_t> minBlobSize;

    EventuallyPersistentStore *store;
    EPStats &stats;

    std::string policyName;
    uint32_t maxSize; // Total number of entries for eviction
    EvictionPolicy* evpolicy;

    bool pauseJob;
    time_t pruneAge;
    Atomic<size_t> evictionQuantumSize;
    Atomic<size_t> evictionQuantumMaxCount;
    uint32_t evictionQuietWindow;
    size_t headroom;
    bool disableInlineEviction;

    Atomic<bool> pauseEvict;

    Mutex evictionLock; // below variables are mutated only under this mutex.
    uint32_t lastEvictTime;
    size_t lastRSSTarget;

    std::set<std::string> policies;

    EvictionManager(EventuallyPersistentStore *s, EPStats &st, std::string &policy, size_t headRoom, bool die) :
        store(s), stats(st), policyName(policy), maxSize(MAX_EVICTION_ENTRIES),
        evpolicy(EvictionPolicyFactory::getInstance(policyName, s, st, maxSize)),
        pauseJob(false),
        pruneAge(0), evictionQuantumSize(10485760), evictionQuantumMaxCount(10), evictionQuietWindow(120),
        headroom(headRoom), disableInlineEviction(die), pauseEvict(false),
        lastEvictTime(0), lastRSSTarget(0) {
        policies.insert("lru");
        policies.insert("random");
        policies.insert("bgeviction");

        // This is here in case the default argument was not set correctly
        if (!evpolicy) {
            evpolicy = EvictionPolicyFactory::getInstance("random", s, st, maxSize);
        }
    }

    DISALLOW_COPY_AND_ASSIGN(EvictionManager);
};
#endif /* EVICTION_HH */
