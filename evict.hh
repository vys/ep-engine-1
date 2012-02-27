#ifndef EVICT_HH
#define EVICT_HH 1

#include "stored-value.hh"
#include "ep.hh"
#include "stats.hh"

#define DEBUG_MODE

#ifndef DEBUG_MODE
#define assert 
#endif

#define MAX_INTERVALS 31

extern int time_intervals[];

class failedEvictions {
public:
    failedEvictions() : numKeyNotPresent(0), numDirties(0), numAlreadyEvicted(0), numDeleted(0), numKeyTooRecent(0) {}

    Atomic<uint32_t>    numKeyNotPresent;
    Atomic<uint32_t>    numDirties;
    Atomic<uint32_t>    numAlreadyEvicted;
    Atomic<uint32_t>    numDeleted;
    Atomic<uint32_t>    numKeyTooRecent;
};

class lruStats {
public:
    lruStats () : numTotalEvicts(0), numEvicts(0), numTotalKeysEvicted(0),
                  numEmptyLRU(0), numKeysEvicted(0) {}

    Atomic<uint32_t>        numTotalEvicts;
    Atomic<uint32_t>        numEvicts;
    Atomic<uint32_t>        numTotalKeysEvicted; // Total evictions so far
    Atomic<uint32_t>        numEmptyLRU;
    Atomic<uint32_t>        numKeysEvicted; // Evictions in this run
    static Atomic<size_t>     lruMemSize;
    class failedEvictions    failedTotal; // All failures so far
    class failedEvictions    failed;         // Failures in this run
//    Add histogram structure here
};

class lruPruneStats {
public:
    lruPruneStats() : numPruneRuns(0), numKeyPrunes(0) {}
    Atomic<uint32_t>        numPruneRuns;
    Atomic<uint64_t>        numKeyPrunes;
};

class lruEntry {
public:

    static lruEntry *newlruEntry(StoredValue *v, uint16_t vb, time_t start_time)
    {
        lruEntry *ent = new lruEntry;
        if (ent == NULL) {
            return NULL;
        }
        ent->key.assign(v->getKey());
        ent->age = start_time - v->getDataAge();
        ent->prev = ent->next = NULL;
        ent->vbid = vb;
        lruStats::lruMemSize += sizeof(lruEntry) + ent->key.size();
        return ent;
    }
                        
    ~lruEntry()
    {
        lruStats::lruMemSize -= sizeof(lruEntry) + key.size();
    }

    std::string getKey() { return key; }
    uint16_t get_vbucket_id() { return vbid; }
    int getAge(void) { return age; }
    int lruAge(lruList *lru);

    lruEntry        *prev;
    lruEntry        *next;
private:
    std::string     key;
    /* Age can be negative !!*/
    int             age;
    uint16_t        vbid;
};

struct lruCursor {
    lruCursor () : ptr(NULL), count(0) {}
    lruEntry            *ptr;
    Atomic<int>         count;
};

class lruList {
public:
    lruList(EventuallyPersistentStore *s, EPStats &st);

    ~lruList()
    {
        lruStats::lruMemSize -= sizeof(lruList);
    }
    
    static lruList *New (EventuallyPersistentStore *s, EPStats &st) ;
    void setMaxEntries(int s) { maxEntries = s; }
    int getMaxEntries(void) { return maxEntries; }
    bool isEmpty() 
    {
        bool rv = (count == 0);
        if (rv) {
            checkLRUisEmpty();
        }
        return rv;
    }

    void clearLRU()
    {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "LRU: Clearing old LRU at %d.", ep_current_time());
        lruEntry *ent;
        while ((ent = pop()) != NULL) {
            delete ent;
        }
        for (int i = 1; i < MAX_INTERVALS; ++i) {
            cursor[i].ptr = NULL;
        }
        checkLRUisEmpty();
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "LRU: Clearing LRU done.");
    }

    void getLRUStats(Histogram<int> &histo)
    {
        int adj = getBuildEndTime() - getBuildStartTime();
        for (int i=1; i < MAX_INTERVALS; ++i) {
            int t = time_intervals[i] + adj;
            histo.add(t, cursor[i-1].count);
        }
    }

    int getLRUCount(void) {return count; }
    int getOldest(void) { return head->lruAge(this); }
    int getNewest(void) { return tail->prev->lruAge(this); }
    bool update(lruEntry *);
    bool update_locked(lruEntry *ent);
    void eject(size_t);
    int prune(uint64_t);
    lruEntry *pop(void);
    int lruAge(int my_age);
        
    lruStats    lstats;
    lruPruneStats lpstats;
#if 0

    bool update(T *v);
    T* pop();    
#endif
    
    void setBuildStartTime(time_t t)
    {
        build_end_time = -1;
        build_start_time = t;
    }

    void setBuildEndTime(time_t t)
    {
        build_end_time = t;
        // Sneak in a sanity check
        checkLRUSanity();
    }

    time_t getBuildStartTime()
    {
        return build_start_time;
    }

    time_t getBuildEndTime()
    {
        return build_end_time;
    }

    int keyInLru(const char *, int);
private:
    bool peek(std::string *key, uint16_t *vb);
    void addKey(lruEntry *ent);
    void remove()
    {
        remove_head();
    }
    bool shouldInsertLRU(lruEntry *ent)
    {    
        int val = ent->getAge(); 

        if (val < newest && count == maxEntries) {
            return false;
        }
        return true;
    }

    void checkLRUisEmpty()
    {
        checkLRUSanity();

        for (int i = 0; i < MAX_INTERVALS ;++i) {
            assert(cursor[i].count == 0);
        }
        assert((head == tail) && !count);
    }
    void checkLRUSanity(void)
    {
        assert(count >= 0);
        lruEntry *ent = head;
        int i = 0;
        while (ent != tail) {
            lruEntry *n = ent->next;
            assert ((n == tail) || n->getAge() <= ent->getAge());        
            ent = ent->next;
            i++;
        }
        assert(head->prev == NULL && tail->next == NULL);
        assert(i == count);

        int mycount = 0;
        for (i = 0; i < MAX_INTERVALS ;++i) {
                        mycount += cursor[i].count;
        }
        assert(mycount == count);
    }
#if 0
    void insert_at_head(lruEntry *list, lruEntry *elem)
    {    
        assert(list);
        elem->next = list;
        list->prev = elem;
        set_head(elem);
    }

    // Not used. Fix if plan to use
    void insert_at_tail(lruEntry *list, lruEntry *elem)
    {
        assert(list);
        list->next = elem;
        elem->prev = list;
        set_tail(elem);
    }
    void set_tail(lruEntry *elem)
    {
        tail = elem;
        newest = elem->getAge();
    }
#endif
    void set_head(lruEntry *elem)
    {
        head = elem;
        oldest = elem->getAge();
    }

    void insert_after(lruEntry *list, lruEntry *elem)
    {
        assert(list->next);
        list->next->prev = elem;
        if (list->next == tail) {
            newest = elem->getAge();
        }
        elem->next = list->next;
        list->next = elem;
        elem->prev = list;
     }

    void insert_before(lruEntry *list, lruEntry *elem)
    {
        if (list->prev) {
            list->prev->next = elem;
        }
        if (list == head) {
            set_head(elem);
        }
        elem->prev = list->prev;
        list->prev = elem;
        elem->next = list;
    }

    void insert(lruEntry *list, lruEntry *elem, int *set_cursor)
    {
        lruEntry *tmp;

        assert(list);
        if (list->getAge() <= elem->getAge()) {
            insert_before(list, elem);
            if (set_cursor) {
                *set_cursor = 1;
            }
            return;
        }
        tmp = list;
        while ((list != tail) && list->getAge() > elem->getAge()) {
            tmp = list;
            list = list->next;
        }
        insert_after(tmp, elem);
    }

    lruEntry *find_closest_left_elem(int index)
    {
        for (int i = index; i >= 0; --i) {
            if (cursor[i].ptr != NULL) {
                return cursor[i].ptr;
            }
        }
            return NULL;
    }

    lruEntry *find_closest_right_elem(int index)
    {
        for (int i = index; i < MAX_INTERVALS; ++i) {
            if (cursor[i].ptr != NULL) {
                return cursor[i].ptr;
            }
        }
        return NULL;
    } 
    int find_cursor_index(int val)
    {
        int i;

        assert(val < time_intervals[0]);

        for (i = 1; i < MAX_INTERVALS; ++i) {
            if (val > time_intervals[i]) {
                return i - 1;
            }
        }
        return MAX_INTERVALS - 1;
    }

    void remove_cursor(lruEntry *elem)
    {
        int index = find_cursor_index(elem->getAge());
        if (elem == cursor[index].ptr) {
            lruEntry *tmp = elem->next;
            if (tmp && find_cursor_index(tmp->getAge()) == index) {
                 cursor[index].ptr = tmp;
            } else {
                 cursor[index].ptr = NULL;
            }
        }
        cursor[index].count --;
        assert(cursor[index].count >= 0);
    }

    void remove_head()
    {
        if (head == NULL) {
            return;
        }
        remove_cursor(head);

        lruEntry *newhead = head->next;
        count --;
        assert(count >= 0);
        if (count <= 1) {
            tail = newhead;
        }

        head = newhead;
        if (newhead) {
            newhead->prev = NULL;
            oldest = newhead->getAge();
        } 
    }

    friend class PagingVisitor;
    friend class EventuallyPersistentStore;
    EventuallyPersistentStore *store;
    EPStats                   &stats;
    lruEntry                  *head;
    lruEntry                  *tail;
    Atomic<int>               count;
    int                       maxEntries;
    int                       oldest;
    int                       newest;
    time_t                    build_start_time;
    time_t                    build_end_time;
    lruCursor                 cursor[MAX_INTERVALS];
};

class lruStage {
public:
    lruStage(size_t s) : size(s)
    {
        size_t tsize = size * sizeof(lruEntry *);
        entries = (lruEntry **)malloc(tsize);        
        assert(entries);
        lruStats::lruMemSize += sizeof(lruStage) + tsize;
        index = 0;
    }

    ~lruStage()
    {
        clear();
        lruStats::lruMemSize -= sizeof(lruStage);
    }

    bool add(StoredValue *s, uint16_t vb, time_t time) 
    {
        if (entries == NULL || index >= size) {
            /* No room in the stage. Update stats for it */
            getLogger()->log(EXTENSION_LOG_INFO, NULL, "LRU: Stage cleared, cannot add.");
            return false;
        }
        lruEntry *ent = lruEntry::newlruEntry(s, vb, time);
        entries[index++] = ent;
        return true;
    }

    void commit(lruList *lru) 
    {
        if (entries == NULL)
            return;
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "LRU: Stage commiting.");
        while (index) {
            lru->update_locked(entries[index - 1]);
            index --;
        }
    }

    void clear(void)
    {
        if (entries) {
            lruStats::lruMemSize -= size * sizeof(lruEntry *);
            free(entries);
            entries = NULL;
        }
        index = 0;
    }
private:
        lruEntry    **entries;
        size_t        size;
        size_t        index;
};

#endif /* EVICT_HH */
