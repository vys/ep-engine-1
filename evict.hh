#ifndef EVICT_HH
#define EVICT_HH 1

#include "stored-value.hh"
#include "ep.hh"
#include "stats.hh"

#define DEBUG_MODE

#ifndef DEBUG_MODE
#define assert 
#endif

#define MAX_INTERVALS 28

extern int time_intervals[];

class lruEntry {
public:

    lruEntry *newlruEntry(StoredValue *v, uint16_t vb, time_t start_time)
    {
        lruEntry *ent = new lruEntry;
        if (ent == NULL) {
            return NULL;
        }
        ent->key.assign(v->getKey());
        ent->age = start_time - v->getDataAge();
        ent->prev = ent->next = NULL;
        ent->vbid = vb;
        return ent;
    }
    
    std::string getKey() { return key; }
    uint16_t get_vbucket_id() { return vbid; }
    int getAge(void) { return age; }
    int lruAge(lruList *lru);

    lruEntry     *prev;
    lruEntry    *next;
private:
    std::string     key;
    /* Age can be negative !!*/
    int            age;
    int        frequency;
    uint16_t    vbid;
};

struct lruCursor {
    lruCursor () : ptr(NULL), count(0) {}
    lruEntry    *ptr;
    int         count;
};

struct failedEvictions {
    uint32_t    numKeyNotPresent;
    uint32_t    numDirties;
    uint32_t    numAlreadyEvicted;
    uint32_t    numDeleted;
    uint32_t    numKeyTooRecent;
};

class lruStats {
public: 
    uint32_t        numTotalEvicts;
    uint32_t        numEvicts;
    uint32_t         numTotalKeysEvicted; // Total evictions so far
    uint32_t        numEmptyLRU;
    uint32_t         numKeysEvicted; // Evictions in this run
    struct failedEvictions    failedTotal; // All failures so far
    struct failedEvictions    failed;         // Failures in this run
//    Add histogram structure here
};

class lruPruneStats {
public:
    uint32_t        numPruneRuns;
    uint64_t        numKeyPrunes;
};

class lruList : public lruEntry {
public:
    lruList(EventuallyPersistentStore *s, EPStats &st);
    
    //    void initLRU(lruList *);
    static lruList *New (EventuallyPersistentStore *s, EPStats &st) ;
    bool isEmpty() 
    {
        bool rv = (count == 0);
        if (rv) {
            checkLRUisEmpty(this);
        }
        return rv;
    }

    void clearLRU(lruList *l)
    {
        SpinLockHolder slh(&lru_lock);
        checkLRUSanity(l);

        while (l->count) {
            remove();
        }

        checkLRUisEmpty(l);
    }

    void getLRUStats(Histogram<int> &histo, lruList *l)
    {
        for (int i=0; i < MAX_INTERVALS; ++i) {
            int t = time_intervals[i] + 
                    l->getBuildEndTime() - l->getBuildStartTime();
            histo.add(t, l->cursor[i].count);
        }
    }

    int getLRUCount(void) {return count; }
    int getOldest(void) { return head->lruAge(this); }
    int getNewest(void) { return tail->lruAge(this); }
    bool update(lruEntry *);
    bool update_locked(lruEntry *ent);
    void eject(size_t);
    int prune(uint64_t);
        
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
    SpinLock    lru_lock;
private:
    bool peek(std::string *key, uint16_t *vb);
    //    void addKey(StoredValue *v, uint16_t vb);
    void addKey(lruEntry *ent);
    void remove()
    {
        remove_head();
    }
    bool shouldInsertLRU(lruEntry *ent)
    {    
        int val = ent->getAge(); 

        if (val < newest && count == maxLruEntries) {
            return false;
        }
        return true;
    }

    void checkLRUisEmpty(lruList *l)
    {
        checkLRUSanity(l);

        for (int i = 0; i < MAX_INTERVALS ;++i) {
            assert(l->cursor[i].ptr == NULL);
            assert(l->cursor[i].count == 0);
        }
         assert(!head && !tail && !count);
    }
    void checkLRUSanity(lruList *l)
    {
        assert(l->count >= 0);
        lruEntry *ent = l->head;
        int i = 0;
        while (ent) {
            lruEntry *n = ent->next;
            assert (!n || n->getAge() <= ent->getAge());        
            ent = ent->next;
            i++;
        }
        assert(!head || head->prev == NULL);
        assert(!tail || tail->next == NULL);
        assert(i == count);

        int mycount = 0;
        for (i = 0; i < MAX_INTERVALS ;++i) {
                        mycount += cursor[i].count;
        }
        assert(mycount == count);
    }

    void insert_at_head(lruEntry *list, lruEntry *elem)
    {    
        assert(list);
        elem->next = list;
        list->prev = elem;
        set_head(elem);
    }

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
    void set_head(lruEntry *elem)
    {
        head = elem;
        oldest = elem->getAge();
    }

    void insert_after(lruEntry *list, lruEntry *elem)
    {
        if (list->next) {
            list->next->prev = elem;
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
        elem->prev = list->prev;
        list->prev = elem;
        elem->next = list;
    }

    void insert(lruEntry *list, lruEntry *elem, int *set_cursor)
    {
        lruEntry *tmp;

        assert(list);
        tmp = list;
        while (list && list->getAge() > elem->getAge()) {
            tmp = list;
            list = list->next;
        }
        if (tmp == list) {
            insert_before(tmp, elem);
            if (tmp == head) {
                set_head(elem);
                }
            if (set_cursor) {
                *set_cursor = 1;
                }
        } else {
            insert_after(tmp, elem);
            if (tmp == tail) {
                set_tail(elem);
            }
        }
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

        head->getKey().clear();
        delete head;
        head = newhead;
        if (newhead) {
            newhead->prev = NULL;
            oldest = newhead->getAge();
        } 
    }

    friend class lruEntry;
    friend class PagingVisitor;
    EventuallyPersistentStore *store;
    EPStats        &stats;
    lruEntry    *head;
    lruEntry    *tail;
    int        count;
    int maxLruEntries;
    int        oldest;
    int        newest;
    time_t build_start_time;
    time_t build_end_time;
    lruCursor    cursor[MAX_INTERVALS];
};

class lruStage : lruEntry {
public:
    lruStage(size_t s) : size(s)
    {
        entries = (lruEntry **)calloc(size, sizeof(lruEntry *));        
        assert(entries);
        index = 0;
    }

    bool add(StoredValue *s, uint16_t vb, time_t time) 
    {
        if (index >= size) {
            /* No room in the stage. Update stats for it */
            return false;
        }
        lruEntry *ent = newlruEntry(s, vb, time);
        entries[index++] = ent;
        return true;
    }

    void commit(lruList *lru) 
    {
        SpinLockHolder slh(&lru->lru_lock);
        while (index) {
            lru->update_locked(entries[index - 1]);
            index --;
        }
    }

    void clear(void)
    {
        if (entries) {
            free(entries);
           }
        index = 0;
    }
private:
        lruEntry    **entries;
        size_t        size;
        size_t        index;
};

#endif /* EVICT_HH */
