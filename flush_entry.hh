/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef FLUSHENTRY_HH
#define FLUSHENTRY_HH 1

#include <stored-value.hh>
#include <queueditem.hh>

/* 
   Structure used by flusher and the persistent layer to pass mutations.
   Pointer to StoredValue structure is used as it cannot disappear from
   underneath flusher.
*/
class FlushEntry {
public:
    FlushEntry(StoredValue *s, uint16_t vb, 
              enum queue_operation o, uint16_t vv) : 
              sv(s), vbucketId(vb), vbucket_version(vv),
              queued(ep_current_time()), op(o) {}

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
    uint16_t vbucket_version;
    uint32_t queued;
    enum queue_operation op;
};

#endif /* FLUSHENTRY_HH */
