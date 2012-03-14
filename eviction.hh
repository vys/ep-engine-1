#ifndef EV_HH
#define EV_HH 1

#include<ep.hh>
#include<fixed_list.hh>

#define MAX_EVICTION_ENTRIES 500000

//Generic class for identifying evictable items.
class EvictItem {
public:
    EvictItem(StoredValue *v, uint16_t vb) : key(v->getKey()), vbid(vb) {}
    
    virtual ~EvictItem() {}
    
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
                    backgroundJob(job), store(s), stats(st) {}
    virtual ~EvictionPolicy() {}
    virtual EvictItem* evict(void) = 0;
    virtual std::string description () const = 0;

    /* Following set of functions are needed only by policies that need a 
       background job to build their data structures.
     */
    virtual void setSize(int val) = 0;
    virtual void initRebuild() = 0;
    virtual bool addEvictItem(StoredValue *v, uint16_t vb) = 0;
    virtual bool storeEvictItem(void) = 0;
    virtual void completeRebuild(void) = 0;
    bool backgroundJob;

protected:
    EventuallyPersistentStore *store;
    EPStats &stats;
};

class LRUItem : public EvictItem {
public:
    LRUItem(EvictItem e, time_t t) : EvictItem(e), timestamp(t) {}
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


//Implementation of LRU based eviction policy
class LRUPolicy : public EvictionPolicy {
public:
    LRUPolicy(EventuallyPersistentStore *s, EPStats &st, bool job) 
        : EvictionPolicy(s, st, job), list(NULL) {}

    ~LRUPolicy() {}
    LRUItemCompare lruItemCompare;

    void setSize(int val) { maxSize = val; }
    
    void initRebuild() {
        templist = new FixedList<LRUItem, LRUItemCompare>(maxSize);
        startTime = ep_real_time(); 
    }

    bool addEvictItem(StoredValue *v, uint16_t vb) {
        LRUItem item(EvictItem(v, vb), v->getDataAge());
        if ((templist->size() == maxSize) && (lruItemCompare(**(templist->last()), item) < 0)) {
            return false;
        }
        stage.push_front(item);
        return true;
    }

    bool storeEvictItem() {
        templist->insert(stage);
        return true;
    }

    void completeRebuild() {
        endTime = ep_real_time();
        FixedList<LRUItem, LRUItemCompare>::iterator tempit = templist->begin();
        it.swap(tempit);
//        lstats.queueBuildTime = endTime - startTime;
        stage.clear();
        delete list;
        list = templist;
    }

    EvictItem *evict(void);

    std::string description() const { return std::string("lru"); }

private:
    std::list<LRUItem> stage;
    FixedList<LRUItem, LRUItemCompare> *list;
    FixedList<LRUItem, LRUItemCompare> *templist;
    FixedList<LRUItem, LRUItemCompare>::iterator it;
    int maxSize;
    Atomic<uint32_t> count;
    time_t startTime;
    time_t endTime;
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

    EvictItem *evict();

    void setSize(int val) {
        maxSize = val;
    }
    void initRebuild() {
        templist = new RandomList();
        size = 0;
        startTime = ep_real_time(); 
    }

    bool addEvictItem(StoredValue *v, uint16_t vb) {
        if (size == maxSize) {
            return false;
        }
        EvictItem *item = new EvictItem(v, vb);
        templist->add(item);
        size++;
        return true;
    }

    bool storeEvictItem() {return false;} // Nothing to do

    void completeRebuild() {
        endTime = ep_real_time();
        RandomList::iterator tempit = templist->begin();
        tempit = it.swap(tempit);
        EvictItem *node;
        while ((node = ++tempit) != NULL) {
            delete node;
        }
        delete list;
        list = templist;
    }

    std::string description() const { return std::string("random"); }
private:
    RandomList *list;
    RandomList *templist;
    RandomList::iterator it;
    size_t maxSize;
    size_t size;
    time_t startTime;
    time_t endTime;
};

class EvictionPolicyFactory {
public:
    static EvictionPolicy *getInstance(std::string desc, EventuallyPersistentStore *s, EPStats &st) {
        EvictionPolicy *p = NULL;
        if (desc == "lru") {
            p = new LRUPolicy(s, st, true);
        } else if (desc == "random") {
            p = new RandomPolicy(s, st, true);
        } else {
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Invalid policy name"); 
        }
        return p;
    }
};

class EvictionManager {
public:
    EvictionManager(EventuallyPersistentStore *s, EPStats &st) : 
        policyName("random"), maxSize(MAX_EVICTION_ENTRIES), store(s), stats(st) {
        evpolicy = EvictionPolicyFactory::getInstance(policyName, s, st);
    }

    ~EvictionManager() {}
   
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
    std::string policyName;
    uint32_t getCount(void) { return count; }

private:
    EvictionPolicy* evpolicy;
    uint32_t maxSize; // Total number of entries for eviction
    Atomic<uint32_t> count;
    time_t startTime;
    time_t endTime;
    bool pauseEviction;
    bool pauseJob;
    EventuallyPersistentStore *store;
    EPStats &stats;
//    class lruFailedEvictions    failedstats;         // Failures in this run
};
#endif /* EV_HH */
