#include "evict.hh"

/* Keep track of keys with interesting age */
int time_intervals[] = {
    5184000,    // 2 months
    2592000,    // 1 month
    1209600,    // 2 weeks
    604800,     // 1 week
    172800,     // 2 days
    86400,      // 1 day
    36000,      // 10 hours
    18000,
    7200,
    3600,
    1800,
    600,
    300,
    120,
    60,
    30,
    10,
    0,
    -10,
    -30,
    -60,
    -120,
    -300,
    -600,
    -1800,
    -3600,
    -7200,
    -18000,
    -36000,
    -86400,
    -172800
};

Atomic<size_t> lruStats::memSize;

lruList::lruList(EventuallyPersistentStore *s, EPStats &st)
    : store(s), stats(st), head(NULL), tail(NULL), count(0), oldest(0), newest(0)
{
    build_end_time = build_start_time = -1;
    maxEntries = s->getMaxLruEntries();
}

lruList *lruList::New (EventuallyPersistentStore *s, EPStats &st) 
{
    getLogger()->log(EXTENSION_LOG_DETAIL, NULL, "XXX: LRU: New list.");
    lruList *l = new lruList(s, st);
    lruEntry *stub = new lruEntry;
    stub->prev = stub->next = NULL;
    stub->getKey().clear();
    l->head = l->tail = stub;
    lruStats::memSize += sizeof(lruList);
    return l;
}

bool lruList::update_locked(lruEntry *ent)
{
    if (!shouldInsertLRU(ent)) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "XXX: LRU: Item too new to insert.");
        return false;
    }
    if (count == maxEntries) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "XXX: LRU: List full. Replacing newest element.");
        remove_head();
    }
    addKey(ent);
    return true;
}

bool lruList::update(lruEntry *ent)
{
    return update_locked(ent);
}


lruEntry *lruList::pop(void)
{
    lruEntry *ent, *headnext;
    do {
        ent = head;
        if (head == tail) {
            return NULL;
        }
        headnext = head->next;
    } while (!ep_sync_bool_compare_and_swap(&head, ent, headnext));
    --count;
    int index = find_cursor_index(ent->getAge());
    --cursor[index].count;
    // Should always be safe to do
    head->prev = NULL;
    oldest = head->getAge();
    return ent;
}  

void lruList::eject(size_t size)
{
    size_t cur = 0;
    std::string k;
    uint16_t b;

    getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "XXX: LRU: Commencing ejection to evict %udB of keys.", size);
    while(cur < size) {
        lruEntry *ent = pop();

        if (ent == NULL) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL, "XXX: LRU: Empty list, ejection failed. Evicted only %udB out of a total %udB required.", cur, size);
            lstats.numEmptyLRU++;
            return;
        }
        k.assign(ent->getKey());
        b = ent->get_vbucket_id();
        
        ent->freeLruEntry();

        RCPtr<VBucket> vb = store->getVBucket(b);
        int bucket_num(0);
        LockHolder lh = vb->ht.getLockedBucket(k, &bucket_num);
        StoredValue *v = vb->ht.unlocked_find(k, bucket_num, false);

        if (!v) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL, "XXX: LRU: Key not present.");
            lstats.failedTotal.numKeyNotPresent++;
            lstats.failed.numKeyNotPresent++;
        } else if (!v->ejectValue(stats, vb->ht)) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL, "XXX: LRU: Key not eligible for eviction.");
            if (v->isResident() == false) {
                lstats.failedTotal.numAlreadyEvicted++;
                lstats.failed.numAlreadyEvicted++;
            } else if (v->isClean() == false) {
                lstats.failedTotal.numDirties++;
                lstats.failed.numDirties++;
            } else if (v->isDeleted() == false) {
                lstats.failedTotal.numDeleted++;
                lstats.failed.numDeleted++;
            }
        } else {
            cur += v->valLength(); 
            /* update stats for eviction that just happened */
            lstats.numTotalKeysEvicted++;
            lstats.numKeysEvicted++;
        }
        lh.unlock();
    }
}

/*
Algo:
1. Find the real place for the key based on the age. Find the real cursor.
If the cursor is present, just follow insert into the doubly linked list logic.
else, set this key as the cursor.
Find the closest cursor to this left of this key and set it as prev of this key.
    Set head if needed.
Find the closest cursor to the right of this key and set it as next of this key.
    Set tail if needed.
*/
void lruList::addKey(lruEntry *ent)
{
    assert(ent != NULL);
    uint32_t val = ent->getAge();
    int index;

    index = find_cursor_index(val);

    /* special case the first element */
    if (head == tail) {
        getLogger()->log(EXTENSION_LOG_DETAIL, NULL, "XXX: LRU: First element in.");
        assert(count == 0);
        ent->next = tail;
        tail->prev = ent;
        ent->prev = NULL;
        head = ent;
        oldest = newest = val;
        count++;
        cursor[index].ptr = ent;
        cursor[index].count++;
        return;
    }

    if (cursor[index].ptr != NULL) {
        int set_cursor = 0;
        /* Just insert at the right place */
        insert(cursor[index].ptr, ent, &set_cursor);
        if (set_cursor) {
            cursor[index].ptr = ent;
        }
    } else {
        lruEntry *left = find_closest_left_elem(index);
        lruEntry *right = find_closest_right_elem(index);

        assert(left || right);

        if (!left) {
            insert_before(right, ent);
        } else if (!right) {
            insert_before(tail, ent);
        } else {
            insert(left, ent, NULL);
        }
        /* we are the first in this window */
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "XXX: LRU: Created new cursor.");
        cursor[index].ptr = ent;
    }
    cursor[index].count++;
    count++;
}

int lruList::keyInLru(const char *keybytes, int keylen)
{
    if (build_end_time == -1)
    {
        getLogger()->log(EXTENSION_LOG_INFO, NULL, "XXX: LRU: Querying key existence in unbuilt LRU.");
        return -1;
    }
    lruEntry *p = head;
    while (p)
    {
        if (p->getKey().compare(0, keylen, keybytes) == 0)
            return 1;
        p = p->next;
    }
    return 0;
}

int lruEntry::lruAge(lruList *lru) 
{
    return (getAge() + lru->getBuildEndTime() - lru->getBuildStartTime());
}

int lruList::lruAge(int my_age)
{
    return (my_age + getBuildEndTime() - getBuildStartTime());
}

int lruList::prune(uint64_t prune_age)
{
    std::string k;
    uint16_t b;

    lpstats.numPruneRuns++;
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "XXX: LRU: Commencing prune upto age %ulld.", prune_age);
    while ((uint64_t)lruAge(oldest) > prune_age) {
        lruEntry *ent = pop();
        if (!ent) {
            break;
        }
        int key_age = ent->lruAge(this);
        assert(key_age >= 0);

        // Popped entry might be different from the one in the first check
        if ((uint64_t)key_age < prune_age) {
            /* we are done */
            break;
        }
        k.assign(ent->getKey());
        b = ent->get_vbucket_id();
        ent->freeLruEntry();

        RCPtr<VBucket> vb = store->getVBucket(b);
        int bucket_num(0);

        LockHolder lh = vb->ht.getLockedBucket(k, &bucket_num);
        StoredValue *v = vb->ht.unlocked_find(k, bucket_num, false);
        if (v->ejectValue(stats, vb->ht) == false) {
            // Update some stats 
            getLogger()->log(EXTENSION_LOG_INFO, NULL, "XXX: LRU: Key ejection failed during LRU prune.");
        } else {
             lpstats.numKeyPrunes++;
        }
    }
    return 0;
}
