#ifndef EV_HH
#define EV_HH 1

#include <set>

#include<ep.hh>
#include<fixed_list.hh>

#define MAX_EVICTION_ENTRIES 500000

//Generic class for identifying evictable items.
class EvictItem {
public:
    EvictItem(StoredValue *v, uint16_t vb) : key(v->getKey()), vbid(vb) {}
    
    virtual ~EvictItem() {}
    
    void increaseCurrentSize(EPStats &st) {
        size_t by = sizeof(EvictItem) + key.size();
        st.currentEvictionMemSize.incr(by);
    }

    void reduceCurrentSize(EPStats &st) {
        size_t by = sizeof(EvictItem) + key.size(), val;
        do {
            val = st.currentEvictionMemSize.get();
            assert(val >= by);
        } while (!st.currentEvictionMemSize.cas(val, val - by));
    }

    const std::string &getKey() { 
        return key; 
    }
    
    uint16_t get_vbucket_id() const { 
        return vbid; 
    }

private:
    std::string key;
    uint16_t vbid;
};

class EvictionPolicy {
public:
    EvictionPolicy(EventuallyPersistentStore *s, EPStats &st, bool job) : 
                   backgroundJob(job), store(s), stats(st) {
        stats.currentEvictionMemSize.incr(sizeof(EvictionPolicy));
    }

    virtual ~EvictionPolicy() {
        stats.currentEvictionMemSize.decr(sizeof(EvictionPolicy));
    }

    virtual EvictItem* evict(void) = 0;
    virtual std::string description () const = 0;
    virtual void getStats(const void *cookie, ADD_STAT add_stat) = 0;

    /* Following set of functions are needed only by policies that need a 
       background job to build their data structures.
     */
    virtual void setSize(size_t val) = 0;
    virtual void initRebuild() = 0;
    virtual bool addEvictItem(StoredValue *v, RCPtr<VBucket>) = 0;
    virtual bool storeEvictItem(void) = 0;
    virtual void completeRebuild(void) = 0;
    bool backgroundJob;

protected:
    EventuallyPersistentStore *store;
    EPStats &stats;
};

class LRUItem : public EvictItem {
public:
    LRUItem(StoredValue *v, uint16_t vb, time_t t) : EvictItem(v, vb), timestamp(t) {}
    ~LRUItem() {}

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

class BGTimeStats {
public:
    BGTimeStats() : startTime(0), endTime(0), visitTime(0), storeTime(0), completeTime(0) {}
    void reset(void) {
        startTime = 0;
        endTime = 0;
        visitTime = 0;
        storeTime = 0;
        completeTime = 0;
    }

    void getStats(const void *cookie, ADD_STAT add_stat);

    time_t startTime;
    time_t endTime;
    time_t visitTime;
    time_t storeTime;
    time_t completeTime;
};

//Implementation of LRU based eviction policy
class LRUPolicy : public EvictionPolicy {
public:
    LRUPolicy(EventuallyPersistentStore *s, EPStats &st, bool job) 
        : EvictionPolicy(s, st, job), list(NULL), templist(NULL) {
        // this assumes that three pointers are used per node of list
        stats.currentEvictionMemSize.incr(sizeof(LRUPolicy) + 3 * sizeof(int*));
    }

    ~LRUPolicy() {
        // this assumes that three pointers are used per node of list
        stats.currentEvictionMemSize.decr(sizeof(LRUPolicy) + 3 * sizeof(int*));
        if (list) {
            while (it != list->end()) {
                LRUItem *item = it++;
                item->reduceCurrentSize(stats);
                delete item;
            }
            stats.currentEvictionMemSize.decr(list->memSize());
            delete list;
        }
        if (templist) {
            it = templist->begin();
            while (it != templist->end()) {
                LRUItem *item = it++;
                item->reduceCurrentSize(stats);
                delete item;
            }
            stats.currentEvictionMemSize.decr(templist->memSize());
            delete templist;
        }
    }

    LRUItemCompare lruItemCompare;

    void setSize(size_t val) { maxSize = val; }
    
    void initRebuild() {
        templist = new FixedList<LRUItem, LRUItemCompare>(maxSize);
        stats.currentEvictionMemSize.incr(templist->memSize());
        timestats.reset();
        timestats.startTime = ep_real_time(); 
    }

    bool addEvictItem(StoredValue *v, RCPtr<VBucket> currentBucket) {
        assert(templist);
        time_t start = ep_real_time();
        LRUItem *item = new LRUItem(v, currentBucket->getId(), v->getDataAge());
        item->increaseCurrentSize(stats);
        if ((templist->size() == maxSize) && (lruItemCompare(*templist->last(), *item) < 0)) {
            return false;
        }
        stage.push_front(item);
        // this assumes that three pointers are used per node of list
        stats.currentEvictionMemSize.incr(3 * sizeof(int*));
        timestats.visitTime += ep_real_time() - start;
        return true;
    }

    bool storeEvictItem() {
        assert(templist);
        time_t start = ep_real_time();
        templist->insert(stage, true);
        timestats.storeTime += ep_real_time() - start;
        // this assumes that three pointers are used per node of list
        stats.currentEvictionMemSize.decr(stage.size() * 3 * sizeof(int*));
        stage.clear();
        return true;
    }

    void completeRebuild() {
        assert(templist);
        time_t start = ep_real_time();
        templist->build();
        FixedList<LRUItem, LRUItemCompare>::iterator tempit = templist->begin();
        tempit = it.swap(tempit);
        while (tempit != list->end()) {
            LRUItem *item = tempit++;
            item->reduceCurrentSize(stats);
            delete item;
        }
        stats.currentEvictionMemSize.decr(templist->memSize());
        delete list;
        list = templist;
        templist = NULL;
        // this assumes that three pointers are used per node of list
        stats.currentEvictionMemSize.decr(stage.size() * 3 * sizeof(int*));
        stage.clear();
        timestats.endTime = ep_real_time();
        timestats.completeTime = timestats.endTime - start;
    }

    EvictItem *evict(void) {
        LRUItem *ent = it++;
        return static_cast<EvictItem *>(ent);
    }

    std::string description() const { return std::string("lru"); }

    void getStats(const void *cookie, ADD_STAT add_stat);

private:
    std::list<LRUItem*> stage;
    FixedList<LRUItem, LRUItemCompare> *list;
    FixedList<LRUItem, LRUItemCompare> *templist;
    FixedList<LRUItem, LRUItemCompare>::iterator it;
    size_t maxSize;
    Atomic<uint32_t> count;
    BGTimeStats timestats;
};

class RandomPolicy : public EvictionPolicy {
    class RandomNode {
    public:
        RandomNode(EvictItem* _data = NULL) : data(_data), next(NULL) {}
        ~RandomNode() {}

        EvictItem *data;
        RandomNode *next;
    };

    class RandomList {
    public:
        RandomList() : head(new RandomNode) {} 
        
        ~RandomList() {
            while (head != NULL) {
                RandomNode *next = head->next;
                delete head;
                head = next;
            }
        }
    
        class RandomListIteraror {
        public:
            RandomListIteraror(RandomNode *node = NULL) : _node(node) {}

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

            RandomNode *swap(RandomListIteraror &it) {
                RandomNode *old;
                do {
                    old = _node;
                } while (!ep_sync_bool_compare_and_swap(&_node, old, it._node));
                return old;
            }
        private:
            RandomNode *_node;
        };

        typedef RandomListIteraror iterator;

        iterator begin() {
            return iterator(head);
        }
        void add(EvictItem *item) {
            RandomNode *n = new RandomNode(item);
            n->next = head;
            head = n;
        }

        private:
            RandomNode *head;
    };

public:
    RandomPolicy(EventuallyPersistentStore *s, EPStats &st, bool job)
        : EvictionPolicy(s, st, job), list(new RandomList()), it(list->begin()) {}

    ~RandomPolicy() {}

    void setSize(size_t val) {
        maxSize = val;
    }
    void initRebuild() {
        templist = new RandomList();
        timestats.reset();
        timestats.startTime = ep_real_time();
    }

    bool addEvictItem(StoredValue *v, RCPtr<VBucket> currentBucket) {
        time_t start = ep_real_time();
        if (size == maxSize) {
            return false;
        }
        EvictItem *item = new EvictItem(v, currentBucket->getId());
        templist->add(item);
        size++;
        timestats.visitTime += ep_real_time() - start;
        return true;
    }

    bool storeEvictItem() {
        if (size >= maxSize) {
            return false;
        }
        return true;
    }

    void completeRebuild() {
        time_t start = ep_real_time();
        RandomList::iterator tempit = templist->begin();
        queueSize = size;
        tempit = it.swap(tempit);
        EvictItem *node;
        while ((node = ++tempit) != NULL) {
            delete node;
        }
        delete list;
        list = templist;
        size = 0;
        timestats.endTime = ep_real_time();
        timestats.completeTime = timestats.endTime - start;
        userstats = timestats;
    }

    EvictItem *evict() {
        EvictItem *ent = ++it;
        queueSize--;
        return ent;
    }

    void getStats(const void *cookie, ADD_STAT add_stat);

    std::string description() const { return std::string("random"); }
private:
    RandomList *list;
    RandomList *templist;
    RandomList::iterator it;
    size_t maxSize;
    size_t size;
    Atomic<size_t> queueSize;
    BGTimeStats timestats;
    BGTimeStats userstats;
};

// Background eviction policy to mimic the item-pager behaviour based on
// memory watermarks.
class BGEvictionPolicy : public EvictionPolicy {
public:
    BGEvictionPolicy(EventuallyPersistentStore *s, EPStats &st, bool job) :
                     EvictionPolicy(s, st, job), shouldRun(true), ejected(0) {}

    ~BGEvictionPolicy() {}

    void setSize(size_t val) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "No use of size %d", val);
    }

    void initRebuild() {
        timestats.reset();
        timestats.startTime = ep_real_time();
        double current = static_cast<double>(StoredValue::getCurrentSize(stats));
        double upper = static_cast<double>(stats.mem_high_wat);
        double lower = static_cast<double>(stats.mem_low_wat);

        if (current > upper) {
            toKill = (current - static_cast<double>(lower)) / current;
            shouldRun = true;
        } else {
            shouldRun = false;
            return;
        }
    }

    bool addEvictItem(StoredValue *v, RCPtr<VBucket> currentBucket) {
        time_t start = ep_real_time();
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
        timestats.visitTime += ep_real_time() - start;
        return true;
    }

    bool storeEvictItem() {
        if (!shouldRun) {
            return false;
        }
        return true; 
    }

    void completeRebuild() {
        timestats.endTime = ep_real_time();
        timestats.completeTime = 0; // No work here
        userstats = timestats;
    }

    EvictItem *evict() {
        // No evictions in the front-end op
        return NULL;
    }

    void getStats(const void *cookie, ADD_STAT add_stat);

    std::string description() const { return std::string("bgeviction"); }

private:
    double toKill;
    bool shouldRun;
    size_t ejected;
    BGTimeStats timestats;
    BGTimeStats userstats;
};

class EvictionPolicyFactory {
public:
    static EvictionPolicy *getInstance(std::string desc, EventuallyPersistentStore *s, EPStats &st) {
        EvictionPolicy *p = NULL;
        if (desc == "lru") {
            p = new LRUPolicy(s, st, true);
        } else if (desc == "random") {
            p = new RandomPolicy(s, st, true);
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
    EvictionManager(EventuallyPersistentStore *s, EPStats &st, const char *p) :
        maxSize(MAX_EVICTION_ENTRIES), count(0), pauseEviction(false),
        pauseJob(false), store(s), stats(st), policyName(p),
        evpolicy(EvictionPolicyFactory::getInstance(policyName, s, st)) {
        policies.insert("lru");
        policies.insert("random");
        policies.insert("bgeviction");

        // This is here in case the default argument was not set correctly
        if (!evpolicy) {
            evpolicy = EvictionPolicyFactory::getInstance("random", s, st);
        }
    }

    ~EvictionManager() {}
   
    bool setPolicy(const char *name) {
        if (policies.find(name) != policies.end()) {
            policyName = name;
            return true;
        } else {
            return false;
        }
    }

    EvictionPolicy *getCurrentPolicy() {
        return evpolicy;
    }

    EvictionPolicy *evictionBGJob();

    bool evictSize(size_t size);

    void prune(uint64_t age) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Pruning keys with age %ull", age); 
    }

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

private:
    uint32_t maxSize; // Total number of entries for eviction
    Atomic<int> count;
    bool pauseEviction;
    bool pauseJob;
    EventuallyPersistentStore *store;
    EPStats &stats;
    std::string policyName;
    EvictionPolicy* evpolicy;
    std::set<std::string> policies;
};
#endif /* EV_HH */
