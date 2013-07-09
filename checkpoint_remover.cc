/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/*
 *   Copyright 2013 Zynga inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include "config.h"
#include "vbucket.hh"
#include "ep_engine.h"
#include "ep.hh"
#include "checkpoint_remover.hh"

/**
 * Remove all the closed unreferenced checkpoints for each vbucket.
 */
class CheckpointVisitor : public VBucketVisitor {
public:

    /**
     * Construct a CheckpointVisitor.
     */
    CheckpointVisitor(EventuallyPersistentStore *s, EPStats &st, bool *sfin)
        : store(s), stats(st), removed(0),
          stateFinalizer(sfin) {}

    bool visitBucket(RCPtr<VBucket> vb) {
        currentBucket = vb;
        bool newCheckpointCreated = false;
        removed = vb->checkpointManager.removeClosedUnrefCheckpoints(vb, newCheckpointCreated);
        // If the new checkpoint is created, notify this event to the tap notify IO thread
        // so that it can then signal all paused TAP connections.
        if (newCheckpointCreated) {
            store->getEPEngine().notifyTapNotificationThread();
        }
        update();
        return false;
    }

    void update() {
        stats.itemsRemovedFromCheckpoints.incr(removed);
        if (removed > 0) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "Removed %d closed unreferenced checkpoints from VBucket %d.\n",
                             removed, currentBucket->getId());
        }
        removed = 0;
    }

    void complete() {
        if (stateFinalizer) {
            *stateFinalizer = true;
        }
    }

private:
    EventuallyPersistentStore *store;
    EPStats                   &stats;
    size_t                     removed;
    bool                      *stateFinalizer;
};

bool ClosedUnrefCheckpointRemover::callback(Dispatcher &d, TaskId t) {
    if (ep_current_time() - lastRun > 100) {
        getLogger()->log(EXTENSION_LOG_INFO, NULL, 
                         "Checkpoint remover languished for %d seconds \n",
                         ep_current_time() - lastRun);
    }

    if (available) {
        lastRun = ep_current_time();
        ++stats.checkpointRemoverRuns;

        available = false;
        shared_ptr<CheckpointVisitor> pv(new CheckpointVisitor(store, stats, &available));
        store->visit(pv, "Checkpoint Remover", &d, Priority::CheckpointRemoverPriority);
    }
    d.snooze(t, sleepTime);
    return true;
}
