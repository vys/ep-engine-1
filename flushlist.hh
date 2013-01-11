/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef __FLUSHLIST_HH__
#define __FLUSHLIST_HH__

#include <stored-value.hh>
#include <queueditem.hh>
#include <boost/intrusive/list.hpp>

class EventuallyPersistentStore;


/* 
   Structure used by flusher and the persistent layer to pass mutations.
   As we are keeping pointer to StoredValue structure, it cannot disappear from
   underneath flusher.
*/
class FlushEntry : public boost::intrusive::list_base_hook<> {
public:
    FlushEntry(StoredValue *s, uint16_t vb, 
              enum queue_operation o, uint16_t vv, time_t qtime = -1) : 
              sv(s), vbucketId(vb), vbucket_version(vv),
              queued(qtime == -1 ? ep_current_time() : qtime), op(o) {}

    StoredValue * getStoredValue () const { return sv; }
    uint16_t getVBucketId() const {return vbucketId; }
    const std::string getKey() const { return sv->getKey(); }
    uint32_t getQueuedTime(void) const { return queued; }
    uint16_t getVBucketVersion() const { return vbucket_version;}
    uint64_t getId() const {return sv->getId(); }
    enum queue_operation getOperation() const { return op; }
    void setOperation (enum queue_operation o) { op = o; }

private:
    StoredValue *sv;
    uint16_t vbucketId;
    uint16_t vbucket_version;
    uint32_t queued;
    enum queue_operation op;
};

//typedef std::list<FlushEntry> FlushList;

typedef boost::intrusive::list<FlushEntry> FlushList;


/**
 * FlushLists maintains efficient list of items that need to be flushed,
 *  categorized by kvstore id and shard id.
 *
 *  Currently it keeps an array of AtomicList<FlushEntry>, which itself is a separate list per thread.
 *  So, this is a very interesting datastructure.
 *
 *  FIXME maxShards based list allocation is a bit lazy on my part. The cost of a few pointers can be ignored for now.
 *
 */
class FlushLists {
    public:

        typedef AtomicIntrusiveList<FlushList, FlushEntry> AtomicFlushList;

        FlushLists(EventuallyPersistentStore *eps, int nKVS, int maxShrds) : epStore(eps), numKVStores(nKVS), maxShards(maxShrds) {
            assert(numKVStores > 0 && maxShards > 0);
            flushLists = new AtomicFlushList[numKVStores*maxShards];
        }

        ~FlushLists() {
            delete[] flushLists;
        }

        void push(int kvId, int shardId, FlushEntry &flushEntry) {
            assert(flushLists != NULL);
            flushLists[kvId*maxShards+shardId].push(flushEntry);
        }

        void get(FlushList& out, int kvId);

        size_t size() {
            size_t s = 0;
            for (int i = 0; i < numKVStores; i++) {
                s += size(i);
            }
            return s;
        }

        size_t size(int kvId) {
            size_t s = 0;
            for (int i = 0; i < maxShards; i++) {
                s += size(kvId, i);
            }
            return s;
        }

        size_t size(int kvId, int shardId) {
            assert(flushLists != NULL);
            return flushLists[kvId*maxShards+shardId].size();
        }

        bool empty(int kvId) {
            return 0 == size(kvId);
        }
        

    private:
        EventuallyPersistentStore *epStore;
        int numKVStores;
        int maxShards;
        AtomicFlushList* flushLists;
};

#endif /* __FLUSHLIST_HH__ */
