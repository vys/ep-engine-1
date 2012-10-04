/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef FLUSHENTRY_HH
#define FLUSHENTRY_HH 1

#include <stored-value.hh>
#include <queueditem.hh>

/* 
   Structure used by flusher and the persistent layer to pass mutations.
   This assumes that the StoredValue structure cannot disappear while
   it is referred to by FlushEntry. 
*/   

class FlushEntry {
public:
    FlushEntry(StoredValue *s, uint16_t vb, 
              enum queue_operation o, uint16_t vv) : 
              sv(s), vbucketId(vb), op(o), vbucket_version(vv),
              queued(ep_current_time()) {}

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
    uint16_t    vbucketId;
    enum queue_operation op;
    uint16_t vbucket_version;
    uint32_t queued;
};

#endif /* FLUSHENTRY_HH */
