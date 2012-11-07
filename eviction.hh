#ifndef EVICTION_HH
#define EVICTION_HH 1

#include <set>

#include<ep.hh>
#include<fixed_list.hh>

#define MAX_EVICTION_ENTRIES 500000

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
    virtual EvictItem* evict() = 0;

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
              list(new FixedList<LRUItem, LRUItemCompare>(maxSize)),
              templist(NULL),
              stopBuild(false),
              count(0),
              lastRun(ep_real_time()) {
        list->build();
        it = list->begin();
        stats.evictionStats.memSize.incr(list->memSize());
        // this assumes that three pointers are used per node of list
        stats.evictionStats.memSize.incr(3 * sizeof(int*));
    }

    ~LRUPolicy() {
        clearStage(true);
        // this assumes that three pointers are used per node of list
        stats.evictionStats.memSize.decr(3 * sizeof(int*));
        clearTemplist();
        if (list) {
            while (it != list->end()) {
                LRUItem *item = it++;
                item->reduceCurrentSize(stats);
                delete item;
            }
            stats.evictionStats.memSize.decr(list->memSize());
            delete list;
        }
    }

    LRUItemCompare lruItemCompare;

    size_t getNumEvictableItems() {
        return count.get();
    }

    size_t getPrimaryQueueSize() {
        if (list) {
            return list->size();
        } else {
            return 0;
        }
    }

    size_t getSecondaryQueueSize() {
        if (templist) {
            return templist->size();
        } else {
            return 0;
        }
    }

    void setSize(size_t val) { 
        if (maxSize != val) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Eviction: Setting max entries to %llu", maxSize);
        }
        maxSize = val; 
    }
    
    void initRebuild();

    bool addEvictItem(StoredValue *v, RCPtr<VBucket> currentBucket);

    bool storeEvictItem();

    void completeRebuild();

    EvictItem *evict(void) {
        LRUItem *ent = it++;
        if (ent == NULL) {
            return NULL;
        }
        count--;
        ent->reduceCurrentSize(stats);
        return static_cast<EvictItem *>(ent);
    }

    bool evictionJobNeeded(time_t lruSleepTime) {
        if (lruSleepTime == 0) {
            return false;
        }
        size_t mem_used = stats.currentSize + stats.memOverhead;
        size_t max_size = StoredValue::getMaxDataSize(stats);
        if (mem_used < (size_t)(memThresholdPercent * max_size)) {
            return false;
        }
        time_t currTime = ep_real_time();
        if (lastRun + lruSleepTime <= currTime) {
            lastRun = currTime;
            return true;
        }
        size_t target = (size_t)(rebuildPercent * curSize);
        if (evictAge() != 0) {
            return true;
        }
        if (curSize == 0 || count <= target) {
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

private:

    // Generate histogram representing distribution of all the keys in the
    // LRU queue with respect to their age.
    void genLruHisto() {
        lruHisto.reset();
        time_t cur = ep_current_time();
        int total = 0;
        // Handle keys that are not accessed since startup. Those keys have
        // the timestamp as 0 and cannot be used with a relative timestamp.
        if (templist->size() && (*templist->begin())->getAttr() == 0) {
            LRUItem l(1);
            total = templist->numLessThan(&l);
            lruHisto.add(cur, total);
        }
        int remaining = templist->size() - total;
        int i = 1;
        total = 0;
        while (total < remaining) {
            time_t t = cur - 2 * i;
            LRUItem l(t);
            int curtotal = templist->numGreaterThan(&l);
            lruHisto.add(2 * i, curtotal - total);
            total = curtotal;
            i++;
        }
    }

    void clearTemplist() {
        if (templist) {
            int k = templist->size();
            LRUItem** array = templist->getArray();
            for (int i = 0; i < k; i++) {
                LRUItem *item = array[i];
                item->reduceCurrentSize(stats);
                delete item;
            }
            stats.evictionStats.memSize.decr(templist->memSize());
            delete templist;
            templist = NULL;
        }
    }

    void clearStage(bool deleteItems = false) {
        // this assumes that three pointers are used per node of list
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
    size_t curSize;
    std::list<LRUItem*> stage;
    FixedList<LRUItem, LRUItemCompare> *list;
    FixedList<LRUItem, LRUItemCompare>::iterator it;
    FixedList<LRUItem, LRUItemCompare> *templist;
    BGTimeStats timestats;
    Histogram<int> lruHisto;
    time_t startTime, endTime;
    bool stopBuild;
    Atomic<size_t> count;
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
            list(new RandomList()),
            it(list->begin()),
            stopBuild(false) {
        stats.evictionStats.memSize.incr(sizeof(RandomList) + RandomList::nodeSize());
    }

    ~RandomPolicy() {
        clearTemplist();
        if (list) {
            EvictItem *node;
            while ((node = ++it) != NULL) {
                node->reduceCurrentSize(stats);
                delete node;
            }
            stats.evictionStats.memSize.decr(queueSize.get() * RandomList::nodeSize());
            delete list;
        }
    }

    void setSize(size_t val) {
        maxSize = val;
    }

    void initRebuild();

    bool addEvictItem(StoredValue *v,RCPtr<VBucket> currentBucket);

    bool storeEvictItem();

    void completeRebuild();

    EvictItem *evict() {
        EvictItem *ent = ++it;
        if (ent == NULL) {
            return NULL;
        }
        ent->reduceCurrentSize(stats);
        stats.evictionStats.memSize.decr(RandomList::nodeSize());
        queueSize--;
        return ent;
    }

    bool evictionJobNeeded(time_t lruSleepTime) {
        (void)lruSleepTime;
        return true;
    }

    void getStats(const void *cookie, ADD_STAT add_stat);

    std::string description() const { return std::string("random"); }

private:
    
    void clearTemplist() {
        if (templist) {
            EvictItem *node;
            RandomList::iterator tempit = templist->begin();
            int c = 0;
            while ((node = ++tempit) != NULL) {
                c++;
                node->reduceCurrentSize(stats);
                delete node;
                stats.evictionStats.memSize.decr(RandomList::nodeSize());
            }
            stats.evictionStats.memSize.decr((c+1) * RandomList::nodeSize());
            delete templist;
            templist = NULL;
        }
    }

    size_t maxSize;
    RandomList *list;
    RandomList *templist;
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

    EvictItem *evict() {
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

    static void createInstance(EventuallyPersistentStore *s, EPStats &st, std::string p) {
        if (managerInstance == NULL) {
            managerInstance = new EvictionManager(s, st, p);
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

    bool evictSize(size_t size);

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

    static void setMinBlobSize(size_t ss) {
        minBlobSize = ss;
    }

    static size_t getMinBlobSize() {
        return minBlobSize;
    }

private:
    uint32_t maxSize; // Total number of entries for eviction
    bool pauseJob;
    EventuallyPersistentStore *store;
    EPStats &stats;
    std::string policyName;
    EvictionPolicy* evpolicy;
    std::set<std::string> policies;
    time_t pruneAge;
    static Atomic<size_t> minBlobSize;
    static EvictionManager *managerInstance;

    EvictionManager(EventuallyPersistentStore *s, EPStats &st, std::string &p) :
        maxSize(MAX_EVICTION_ENTRIES),
        pauseJob(false), store(s), stats(st), policyName(p),
        evpolicy(EvictionPolicyFactory::getInstance(policyName, s, st, maxSize)),
        pruneAge(0) {
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
