/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"
#include "tapthrottle.hh"

const size_t MAXIMUM_QUEUE(1000000);

bool TapThrottle::persistenceQueueSmallEnough() const {
    size_t queueSize = stats.queue_size.get() + stats.flusher_todo_get();
    return queueSize < MAXIMUM_QUEUE;
}

int64_t TapThrottle::hasSomeMemory(size_t needed) const {
    double currentSize = static_cast<double>(stats.currentSize.get() + stats.memOverhead.get());
    double maxSize = static_cast<double>(stats.maxDataSize.get()) * stats.tapThrottleThreshold;
    return (int64_t) ((currentSize + needed) - maxSize);
}
