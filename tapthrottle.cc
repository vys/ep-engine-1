/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"
#include "tapthrottle.hh"

bool TapThrottle::persistenceQueueSmallEnough() const {
    size_t queueSize = stats.queue_size.get() + stats.flusher_todo_get();
    return queueSize < stats.tapThrottlePersistThreshold;
}
