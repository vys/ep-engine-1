#include "evict.hh"

#define MAX_LRU_ENTRIES 500000
/* Keep track of keys with interesting age */
int time_intervals[] = {
    2147483647, // MAX_INT. Equals to seconds worth of 136 years
    172800, // 2 days
    86400,
    36000,
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
    -864000,
    -172800
};

lruList *lruList::New (EventuallyPersistentStore *s, EPStats &st) 
{
    lruList *l = (lruList *):: operator new (sizeof(lruList));
    memset(l, 0, sizeof(lruList));
    new (l) lruList(s, st);
    l->maxLruEntries = MAX_LRU_ENTRIES;
    return l;
}

bool lruList::update_locked(lruEntry *ent)
{
    if (!shouldInsertLRU(ent)) {
        return false;
    }
    if (count == maxLruEntries) {
        remove_head();
    }
    addKey(ent);
    return true;
}

bool lruList::update(lruEntry *ent)
{
    SpinLockHolder slh(&lru_lock);
    return update_locked(ent);

}

/* Get a key from lruList to be evicted 
Start from the LRU end.
Lookup the key in the hashtable. It is up for eviction if
notDirty, isResident and !isDeleted.
If selected for eviction, 
return the corresponding StoredValue. This will be used by the caller
to call ejectValue.
*/

bool lruList::peek(std::string *k, uint16_t *vb)
{
    lruEntry *ent = head;
    if (head == NULL) {
        return false;
    }
    k->assign(ent->getKey());
    *vb = ent->get_vbucket_id();
    return true;
}

void lruList::eject(size_t size)
{
    size_t cur = 0;
    std::string k;
    uint16_t b;

    while(cur < size) {
    SpinLockHolder slh(&lru_lock);

    if (!peek(&k, &b)) {
        lstats.numEmptyLRU++;
        return;
    }
    remove();
    slh.unlock();

    RCPtr<VBucket> vb = store->getVBucket(b);
    int bucket_num(0);
    LockHolder lh = vb->ht.getLockedBucket(k, &bucket_num);
    StoredValue *v = vb->ht.unlocked_find(k, bucket_num, false);

    if (!v) {
        lstats.failedTotal.numKeyNotPresent++;
        lstats.failed.numKeyNotPresent++;
    } else if (!v->ejectValue(stats, vb->ht)) {
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
    if (head == NULL) {
    assert(tail == NULL);
    head = tail = ent;
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
        insert_at_head(right, ent);
        } else if (!right) {
        insert(left, ent, NULL);
        } else {
            insert(left, ent, NULL);
        }
    /* we are the first in this window */
    cursor[index].ptr = ent;

    }
    cursor[index].count++;
    count++;
}

int lruList::keyInLru(const char *keybytes, int keylen)
{
    if (build_end_time == -1)
        return -1;
    lruEntry *p = head;
    while (p)
    {
        if (p->getKey().compare(0, keylen, keybytes) == 0)
            return 1;
        p = p->next;
    }
    return 0;
}
