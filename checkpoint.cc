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
#include "checkpoint.hh"

void Checkpoint::setState(checkpoint_state state) {
    checkpointState = state;
}

void Checkpoint::popBackCheckpointEndItem() {
    if (toWrite.size() > 0 && toWrite.back()->getOperation() == queue_op_checkpoint_end) {
        toWrite.pop_back();
    }
}

uint64_t Checkpoint::getCasForKey(const std::string &key) {
    uint64_t cas = -1;
    checkpoint_index::iterator it = keyIndex.find(key);
    if (it != keyIndex.end()) {
        std::list<queued_item>::iterator currPos = it->second.position;
        cas = (*(currPos))->getCas();
    }
    return cas;
}

queue_dirty_t Checkpoint::queueDirty(const queued_item &item, CheckpointManager *checkpointManager) {
    assert (checkpointState == opened);

    uint64_t newMutationId = checkpointManager->nextMutationId();
    queue_dirty_t rv;

    checkpoint_index::iterator it = keyIndex.find(item->getKey());
    // Check if this checkpoint already had an item for the same key.
    if (it != keyIndex.end()) {
        std::list<queued_item>::iterator currPos = it->second.position;
        uint64_t currMutationId = it->second.mutation_id;

        std::map<const std::string, CheckpointCursor>::iterator map_it;
        for (map_it = checkpointManager->persistenceCursors.begin();
             map_it != checkpointManager->persistenceCursors.end(); map_it++) {
            if (*(map_it->second.currentCheckpoint) == this) {
                // If the existing item is in the left-hand side of the item pointed by the
                // persistence cursor, decrease the persistence cursor's offset by 1.
                std::string key = (*(map_it->second.currentPos))->getKey();
                checkpoint_index::iterator ita = keyIndex.find(key);
                if (ita != keyIndex.end()) {
                    uint64_t mutationId = ita->second.mutation_id;
                    if (currMutationId <= mutationId) {
                        checkpointManager->decrPersistenceCursorOffset(map_it->second, 1);
                    }
                }
                // If the persistence cursor points to the existing item for the same key,
                // shift the cursor left by 1.
                if (map_it->second.currentPos == currPos) {
                    checkpointManager->decrPersistenceCursorPos_UNLOCKED(map_it->second);
                }
            }
        }

        for (map_it = checkpointManager->tapCursors.begin();
             map_it != checkpointManager->tapCursors.end(); map_it++) {

            if (*(map_it->second.currentCheckpoint) == this) {
                std::string key = (*(map_it->second.currentPos))->getKey();
                checkpoint_index::iterator ita = keyIndex.find(key);
                if (ita != keyIndex.end()) {
                    uint64_t mutationId = ita->second.mutation_id;
                    if (currMutationId <= mutationId) {
                        --(map_it->second.offset);
                    }
                }
                // If an TAP cursor points to the existing item for the same key, shift it left by 1
                if (map_it->second.currentPos == currPos) {
                    --(map_it->second.currentPos);
                }
            }
        }
        // Copy the queued time of the existing item to the new one.
        item->setQueuedTime((*currPos)->getQueuedTime());
        // Remove the existing item for the same key from the list.
        toWrite.erase(currPos);
        rv = EXISTING_ITEM;
    } else {
        if (item->getKey().size() > 0) {
            ++numItems;
        }
        rv = NEW_ITEM;
    }
    // Push the new item into the list
    toWrite.push_back(item);

    if (item->getKey().size() > 0) {
        std::list<queued_item>::iterator last = toWrite.end();
        // --last is okay as the list is not empty now.
        index_entry entry = {--last, newMutationId};
        // Set the index of the key to the new item that is pushed back into the list.
        keyIndex[item->getKey()] = entry;
        if (rv == NEW_ITEM) {
            size_t newEntrySize = item->getKey().size() + sizeof(index_entry);
            indexMemOverhead += newEntrySize;
            stats.memOverhead.incr(newEntrySize);
            assert(stats.memOverhead.get() < GIGANTOR);
        }
    }
    return rv;
}

size_t Checkpoint::mergePrevCheckpoint(Checkpoint *pPrevCheckpoint) {
    size_t numNewItems = 0;
    size_t newEntryMemOverhead = 0;
    std::list<queued_item>::reverse_iterator rit = pPrevCheckpoint->rbegin();
    for (; rit != pPrevCheckpoint->rend(); ++rit) {
        const std::string &key = (*rit)->getKey();
        if (key.size() == 0) {
            continue;
        }
        checkpoint_index::iterator it = keyIndex.find(key);
        if (it == keyIndex.end()) {
            // Skip the first two meta items
            std::list<queued_item>::iterator pos = toWrite.begin();
            for (; pos != toWrite.end(); ++pos) {
                if ((*pos)->getKey().compare("") != 0) {
                    break;
                }
            }
            toWrite.insert(pos, *rit);
            index_entry entry = {--pos, pPrevCheckpoint->getMutationIdForKey(key)};
            keyIndex[key] = entry;
            newEntryMemOverhead += key.size() + sizeof(index_entry);
            ++numItems;
            ++numNewItems;
        }
    }
    indexMemOverhead += newEntryMemOverhead;
    stats.memOverhead.incr(newEntryMemOverhead);
    assert(stats.memOverhead.get() < GIGANTOR);
    return numNewItems;
}

uint64_t Checkpoint::getMutationIdForKey(const std::string &key) {
    uint64_t mid = 0;
    checkpoint_index::iterator it = keyIndex.find(key);
    if (it != keyIndex.end()) {
        mid = it->second.mutation_id;
    }
    return mid;
}


Atomic<rel_time_t> CheckpointManager::checkpointPeriod = DEFAULT_CHECKPOINT_PERIOD;
Atomic<size_t> CheckpointManager::checkpointMaxItems = DEFAULT_CHECKPOINT_ITEMS;
Atomic<size_t> CheckpointManager::maxCheckpoints = DEFAULT_MAX_CHECKPOINTS;
bool CheckpointManager::inconsistentSlaveCheckpoint = false;
bool CheckpointManager::keepClosedCheckpoints = false;

CheckpointManager::~CheckpointManager() {
    LockHolder lh(queueLock);
    std::list<Checkpoint*>::iterator it = checkpointList.begin();
    while(it != checkpointList.end()) {
        delete *it;
        ++it;
    }
}

uint64_t CheckpointManager::getOpenCheckpointId_UNLOCKED() {
    if (checkpointList.size() == 0) {
        return 0;
    }

    uint64_t id = checkpointList.back()->getId();
    return checkpointList.back()->getState() == opened ? id : id + 1;
}

uint64_t CheckpointManager::getOpenCheckpointId() {
    LockHolder lh(queueLock);
    return getOpenCheckpointId_UNLOCKED();
}

size_t CheckpointManager::getNumOpenCheckpointItems() {
    LockHolder lh(queueLock);
    if (checkpointList.size() == 0) {
        return 0;
    }

    size_t s = checkpointList.back()->getNumItems();
    return checkpointList.back()->getState() == opened ? s : 0;
}

uint64_t CheckpointManager::getLastClosedCheckpointId_UNLOCKED() {
    if (!isCollapsedCheckpoint) {
        uint64_t id = getOpenCheckpointId_UNLOCKED();
        lastClosedCheckpointId = id > 0 ? (id - 1) : 0;
    }
    return lastClosedCheckpointId;
}

uint64_t CheckpointManager::getLastClosedCheckpointId() {
    LockHolder lh(queueLock);
    return getLastClosedCheckpointId_UNLOCKED();
}

void CheckpointManager::setOpenCheckpointId_UNLOCKED(uint64_t id) {
    if (checkpointList.size() > 0) {
        checkpointList.back()->setId(id);
        // Update the checkpoint_start item with the new Id.
        queued_item qi = createCheckpointItem(id, vbucketId, queue_op_checkpoint_start);
        std::list<queued_item>::iterator it = ++(checkpointList.back()->begin());
        *it = qi;
    }
}

bool CheckpointManager::addNewCheckpoint_UNLOCKED(uint64_t id) {
    // This is just for making sure that the current checkpoint should be closed.
    if (checkpointList.size() > 0 && checkpointList.back()->getState() == opened) {
        closeOpenCheckpoint_UNLOCKED(checkpointList.back()->getId());
    }

    Checkpoint *checkpoint = new Checkpoint(stats, id, opened);
    // Add a dummy item into the new checkpoint, so that any cursor referring to the actual first
    // item in this new checkpoint can be safely shifted left by 1 if the first item is removed
    // and pushed into the tail.
    queued_item dummyItem(new QueuedItem("", 0xffff, queue_op_empty));
    checkpoint->queueDirty(dummyItem, this);

    // This item represents the start of the new checkpoint and is also sent to the slave node.
    queued_item qi = createCheckpointItem(id, vbucketId, queue_op_checkpoint_start);
    checkpoint->queueDirty(qi, this);
    ++numItems;
    checkpointList.push_back(checkpoint);
    return true;
}

bool CheckpointManager::addNewCheckpoint(uint64_t id) {
    LockHolder lh(queueLock);
    return addNewCheckpoint_UNLOCKED(id);
}

bool CheckpointManager::closeOpenCheckpoint_UNLOCKED(uint64_t id) {
    if (checkpointList.size() == 0) {
        return false;
    }
    if (id != checkpointList.back()->getId() || checkpointList.back()->getState() == closed) {
        return true;
    }

    Checkpoint *c = checkpointList.back();
    c->setState(closed);
    getLogger()->log(EXTENSION_LOG_WARNING, NULL, "Vbucket-id: %d "
                     "Closing checkpoint-id %d with %d items, age %d seconds",
                     vbucketId, c->getId(), c->getNumItems(),
                     ep_real_time() - c->getCreationTime());
    return true;
}

bool CheckpointManager::closeOpenCheckpoint(uint64_t id) {
    LockHolder lh(queueLock);
    return closeOpenCheckpoint_UNLOCKED(id);
}

void CheckpointManager::registerPersistenceCursor(int i) {
    LockHolder lh(queueLock);
    char n[256];
    snprintf (n, 256, "PersistenceCursor-%d", i);
    std::string name(n);
    assert(checkpointList.size() > 0);
    CheckpointCursor cursor(name, checkpointList.begin(),
                            checkpointList.front()->begin());
    checkpointList.front()->registerCursorName(name);
    persistenceCursors[name] = cursor;
    persistenceVector.push_back(&persistenceCursors[name]);
}

protocol_binary_response_status CheckpointManager::startOnlineUpdate() {
    LockHolder lh(queueLock);
    assert(checkpointList.size() > 0);

    if (doOnlineUpdate) {
        return PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
    }

    // close the current checkpoint and create a new checkpoint
    checkOpenCheckpoint_UNLOCKED(true, true);

    // This item represents the start of online update and is also sent to the slave node..
    queued_item dummyItem(new QueuedItem("", vbucketId, queue_op_online_update_start));
    checkpointList.back()->queueDirty(dummyItem, this);
    ++numItems;

    onlineUpdateCursor.currentCheckpoint = checkpointList.end();
    --(onlineUpdateCursor.currentCheckpoint);

    onlineUpdateCursor.currentPos = checkpointList.back()->begin();
    (*(onlineUpdateCursor.currentCheckpoint))->registerCursorName(onlineUpdateCursor.name);

    doOnlineUpdate = true;
    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

protocol_binary_response_status CheckpointManager::stopOnlineUpdate() {
    LockHolder lh(queueLock);

    if ( !doOnlineUpdate ) {
        return PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
    }

    // This item represents the end of online update and is also sent to the slave node..
    queued_item dummyItem(new QueuedItem("", vbucketId, queue_op_online_update_end));
    checkpointList.back()->queueDirty(dummyItem, this);
    ++numItems;

    //close the current checkpoint and create a new checkpoint
    checkOpenCheckpoint_UNLOCKED(true, true);

    (*(onlineUpdateCursor.currentCheckpoint))->removeCursorName(onlineUpdateCursor.name);

    // Adjust for onlineupdate start and end items
    numItems -= 2;
    std::map<const std::string, CheckpointCursor>::iterator map_it;
    for (map_it = persistenceCursors.begin(); 
         map_it != persistenceCursors.end(); ++map_it) {
        decrPersistenceCursorOffset(map_it->second, 2);
    }
    for (map_it = tapCursors.begin(); map_it != tapCursors.end(); ++map_it) {
        map_it->second.offset -= 2;
    }

    doOnlineUpdate = false;
    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

protocol_binary_response_status CheckpointManager::beginHotReload() {
    LockHolder lh(queueLock);

    if (!doOnlineUpdate) {
         getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                          "Not in online update phase, just return.");
         return PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
    }
    if (doHotReload) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "In online update revert phase already, just return.");
        return PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
    }

    doHotReload = true;

    //FIXME
//    assert(persistenceCursor.currentCheckpoint == onlineUpdateCursor.currentCheckpoint);

    // This item represents the end of online update and is also sent to the slave node..
    queued_item dummyItem(new QueuedItem("", vbucketId, queue_op_online_update_revert));
    checkpointList.back()->queueDirty(dummyItem, this);
    ++numItems;

    //close the current checkpoint and create a new checkpoint
    checkOpenCheckpoint_UNLOCKED(true, true);

    //Update persistence cursor due to hotReload
    std::map<const std::string, CheckpointCursor>::iterator map_it;
    for (map_it = persistenceCursors.begin();
         map_it != persistenceCursors.end(); map_it++) {
        (*(map_it->second.currentCheckpoint))->removeCursorName(map_it->second.name);
        map_it->second.currentCheckpoint = --(checkpointList.end());
        map_it->second.currentPos = checkpointList.back()->begin();

        (*(map_it->second.currentCheckpoint))->registerCursorName(map_it->second.name);
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

protocol_binary_response_status CheckpointManager::endHotReload(uint64_t total)  {
    LockHolder lh(queueLock);

    if (!doHotReload) {
        return PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
    }

    std::map<const std::string, CheckpointCursor>::iterator map_it;
    map_it = persistenceCursors.begin();
    for (; map_it != persistenceCursors.end(); map_it++) {
        map_it->second.offset += total-1;    // Should ignore the first dummy item
    }

    (*(onlineUpdateCursor.currentCheckpoint))->removeCursorName(onlineUpdateCursor.name);
    doHotReload = false;
    doOnlineUpdate = false;

    // Adjust for onlineupdate start and end items
    numItems -= 2;
    map_it = persistenceCursors.begin();
    for (; map_it != persistenceCursors.end(); ++map_it) {
        decrPersistenceCursorOffset(map_it->second, 2);
    }
    map_it = tapCursors.begin();
    for (; map_it != tapCursors.end(); ++map_it) {
        map_it->second.offset -= 2;
    }

    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

bool CheckpointManager::registerTAPCursor(const std::string &name, uint64_t checkpointId,
                                          bool closedCheckpointOnly, bool alwaysFromBeginning) {
    LockHolder lh(queueLock);
    assert(checkpointList.size() > 0);

    bool found = false;
    std::list<Checkpoint*>::iterator it = checkpointList.begin();
    for (; it != checkpointList.end(); it++) {
        if (checkpointId == (*it)->getId()) {
            found = true;
            break;
        }
    }

    // Get the current open_checkpoint_id. The cursor that grabs items from closed checkpoints
    // only walks the checkpoint datastructure until it reaches to the beginning of the
    // checkpoint with open_checkpoint_id. One of the typical use cases is the cursor for the
    // incremental backup client.
    uint64_t open_chk_id = getOpenCheckpointId_UNLOCKED();

    // If the tap cursor exists, remove its name from the checkpoint that is
    // currently referenced by the tap cursor.
    std::map<const std::string, CheckpointCursor>::iterator map_it = tapCursors.find(name);
    if (map_it != tapCursors.end()) {
        (*(map_it->second.currentCheckpoint))->removeCursorName(name);
    }

    if (!found) {
        // If the checkpoint to start with is not found, set the TAP cursor to the current
        // open checkpoint. This case requires the full materialization through backfill.
        it = --(checkpointList.end());
        CheckpointCursor cursor(name, it, (*it)->begin(),
                            numItems - ((*it)->getNumItems() + 1), // 1 is for checkpoint start item
                            closedCheckpointOnly, open_chk_id);
        tapCursors[name] = cursor;
        (*it)->registerCursorName(name);
    } else {
        size_t offset = 0;
        std::list<queued_item>::iterator curr;
        if (!alwaysFromBeginning &&
            map_it != tapCursors.end() &&
            (*(map_it->second.currentCheckpoint))->getId() == (*it)->getId()) {
            // If the cursor is currently in the checkpoint to start with, simply start from
            // its current position.
            curr = map_it->second.currentPos;
            offset = map_it->second.offset;
        } else {
            // Set the cursor's position to the begining of the checkpoint to start with
            curr = (*it)->begin();
            std::list<Checkpoint*>::iterator pos = checkpointList.begin();
            for (; pos != it; ++pos) {
                offset += (*pos)->getNumItems() + 2; // 2 is for checkpoint start and end items.
            }
        }

        CheckpointCursor cursor(name, it, curr, offset, closedCheckpointOnly, open_chk_id);
        tapCursors[name] = cursor;
        // Register the tap cursor's name to the checkpoint.
        (*it)->registerCursorName(name);
    }

    return found;
}

bool CheckpointManager::removeTAPCursor(const std::string &name) {
    LockHolder lh(queueLock);
    std::map<const std::string, CheckpointCursor>::iterator it = tapCursors.find(name);
    if (it == tapCursors.end()) {
        return false;
    }

    // We can simply remove the cursor's name from the checkpoint to which it currently belongs,
    // by calling
    // (*(it->second.currentCheckpoint))->removeCursorName(name);
    // However, we just want to do more sanity checks by looking at each checkpoint. This won't
    // cause much overhead because the max number of checkpoints allowed per vbucket is small.
    std::list<Checkpoint*>::iterator cit = checkpointList.begin();
    for (; cit != checkpointList.end(); cit++) {
        (*cit)->removeCursorName(name);
    }

    tapCursors.erase(it);
    return true;
}

uint64_t CheckpointManager::getCheckpointIdForTAPCursor(const std::string &name) {
    LockHolder lh(queueLock);
    std::map<const std::string, CheckpointCursor>::iterator it = tapCursors.find(name);
    if (it == tapCursors.end()) {
        return 0;
    }

    return (*(it->second.currentCheckpoint))->getId();
}

size_t CheckpointManager::getNumOfTAPCursors() {
    LockHolder lh(queueLock);
    return tapCursors.size();
}

size_t CheckpointManager::getNumCheckpoints() {
    LockHolder lh(queueLock);
    return checkpointList.size();
}

std::list<std::string> CheckpointManager::getTAPCursorNames() {
    LockHolder lh(queueLock);
    std::list<std::string> cursor_names;
    std::map<const std::string, CheckpointCursor>::iterator tap_it = tapCursors.begin();
        for (; tap_it != tapCursors.end(); ++tap_it) {
        cursor_names.push_back((tap_it->first));
    }
    return cursor_names;
}

bool CheckpointManager::isCheckpointCreationForHighMemUsage(const RCPtr<VBucket> &vbucket) {
    bool forceCreation = false;
    double current = static_cast<double>(stats.currentSize.get() + stats.memOverhead.get());
    // pesistence and tap cursors are all currently in the open checkpoint?
    bool allCursorsInOpenCheckpoint =
        (1 + tapCursors.size()) == checkpointList.back()->getNumberOfCursors() ? true : false;

    if (current > stats.mem_high_wat &&
        allCursorsInOpenCheckpoint &&
        (checkpointList.back()->getNumItems() >= MIN_CHECKPOINT_ITEMS ||
         checkpointList.back()->getNumItems() == vbucket->ht.getNumItems())) {
        forceCreation = true;
    }
    return forceCreation;
}

size_t CheckpointManager::removeClosedUnrefCheckpoints(const RCPtr<VBucket> &vbucket,
                                                       bool &newOpenCheckpointCreated) {

    // This function is executed periodically by the non-IO dispatcher.
    LockHolder lh(queueLock);
    assert(vbucket);
    uint64_t oldCheckpointId = 0;
    bool canCreateNewCheckpoint = false;
    if (checkpointList.size() < maxCheckpoints ||
        (checkpointList.size() == maxCheckpoints &&
         checkpointList.front()->getNumberOfCursors() == 0)) {
        canCreateNewCheckpoint = true;
    }
    if (vbucket->getState() == vbucket_state_active && !inconsistentSlaveCheckpoint &&
        canCreateNewCheckpoint) {

        // Check if this master active vbucket needs to create a new open checkpoint.
        // Use false for forceCreation to avoid creation of small checkpoints when above high-watermark
        oldCheckpointId = checkOpenCheckpoint_UNLOCKED(false, true);
    }
    newOpenCheckpointCreated = oldCheckpointId > 0 ? true : false;
    if (oldCheckpointId > 0) {
        // If the persistence cursor reached to the end of the old open checkpoint, move it to
        // the new open checkpoint.
        std::map<const std::string, CheckpointCursor>::iterator it = persistenceCursors.begin();
        for (; it != persistenceCursors.end(); ++it) {
            if ((*(it->second.currentCheckpoint))->getId() == oldCheckpointId) {
                if (++(it->second.currentPos) ==
                    (*(it->second.currentCheckpoint))->end()) {
                    moveCursorToNextCheckpoint(it->second);
                } else {
                    --(it->second.currentPos);
                }
            }
        }
        // If any of TAP cursors reached to the end of the old open checkpoint, move them to
        // the new open checkpoint.
        std::map<const std::string, CheckpointCursor>::iterator tap_it = tapCursors.begin();
        for (; tap_it != tapCursors.end(); ++tap_it) {
            CheckpointCursor &cursor = tap_it->second;
            if ((*(cursor.currentCheckpoint))->getId() == oldCheckpointId) {
                if (++(cursor.currentPos) == (*(cursor.currentCheckpoint))->end()) {
                    moveCursorToNextCheckpoint(cursor);
                } else {
                    --(cursor.currentPos);
                }
            }
        }
    }

    if (keepClosedCheckpoints && (checkpointList.size() <= maxCheckpoints)) {
        return 0;
    }

    size_t numUnrefItems = 0;
    size_t numCheckpointsRemoved = 0;
    std::list<Checkpoint*> unrefCheckpointList;
    std::list<Checkpoint*>::iterator it = checkpointList.begin();
    for (; it != checkpointList.end(); it++) {
        removeInvalidCursorsOnCheckpoint(*it);
        if ((*it)->getNumberOfCursors() > 0) {
            break;
        } else {
            if (getOpenCheckpointId_UNLOCKED() == (*it)->getId()) {
                break;
            }
            numUnrefItems += (*it)->getNumItems() + 1; // 1 is for checkpoint start item.
            ++numCheckpointsRemoved;
            if (keepClosedCheckpoints &&
                (checkpointList.size() - numCheckpointsRemoved) <= maxCheckpoints) {
                // Collect unreferenced closed checkpoints until the number of checkpoints is equal
                // to the number of max checkpoints allowed.
                ++it;
                break;
            }
        }
    }
    if (numUnrefItems > 0) {
        numItems -= numUnrefItems;
        std::map<const std::string, CheckpointCursor>::iterator map_it = persistenceCursors.begin();
        for (; map_it != persistenceCursors.end(); ++map_it) {
            decrPersistenceCursorOffset(map_it->second, numUnrefItems);
        }
        map_it = tapCursors.begin();
        for (; map_it != tapCursors.end(); ++map_it) {
            map_it->second.offset -= numUnrefItems;
        }
    }
    unrefCheckpointList.splice(unrefCheckpointList.begin(), checkpointList,
                               checkpointList.begin(), it);
    // If any cursor on a replica vbucket or downstream active vbucket receiving checkpoints from
    // the upstream master is very slow and causes more closed checkpoints in memory,
    // collapse those closed checkpoints into a single one to reduce the memory overhead.
    if (!keepClosedCheckpoints &&
        (vbucket->getState() == vbucket_state_replica ||
         (vbucket->getState() == vbucket_state_active && inconsistentSlaveCheckpoint))) {
        collapseClosedCheckpoints(unrefCheckpointList);
    }
    lh.unlock();

    std::list<Checkpoint*>::iterator chkpoint_it = unrefCheckpointList.begin();
    for (; chkpoint_it != unrefCheckpointList.end(); chkpoint_it++) {
        delete *chkpoint_it;
    }

    return numUnrefItems;
}

void CheckpointManager::removeInvalidCursorsOnCheckpoint(Checkpoint *pCheckpoint) {
    std::list<std::string> invalidCursorNames;
    const std::set<std::string> &cursors = pCheckpoint->getCursorNameList();
    std::set<std::string>::const_iterator cit = cursors.begin();
    std::map<const std::string, CheckpointCursor>::iterator mit;
    for (; cit != cursors.end(); ++cit) {
        // Check it with persistence cursors
        mit = persistenceCursors.find(*cit);
        if (mit != persistenceCursors.end()) {
            if (pCheckpoint != *(mit->second.currentCheckpoint)) {
                invalidCursorNames.push_back(*cit);
            }
        } else if ((*cit).compare(onlineUpdateCursor.name) == 0) { // OnlineUpdate cursor
            if (pCheckpoint != *(onlineUpdateCursor.currentCheckpoint)) {
                invalidCursorNames.push_back(*cit);
            }
        } else { // Check it with tap cursors
            mit = tapCursors.find(*cit);
            if (mit == tapCursors.end() || pCheckpoint != *(mit->second.currentCheckpoint)) {
                invalidCursorNames.push_back(*cit);
            }
        }
    }

    std::list<std::string>::iterator it = invalidCursorNames.begin();
    for (; it != invalidCursorNames.end(); ++it) {
        pCheckpoint->removeCursorName(*it);
    }
}

void CheckpointManager::collapseClosedCheckpoints(std::list<Checkpoint*> &collapsedChks) {
    // If there are one open checkpoint and more than one closed checkpoint, collapse those
    // closed checkpoints into one checkpoint to reduce the memory overhead.
    if (checkpointList.size() > 2) {
        std::set<std::string> slowCursors;
        std::set<std::string> fastCursors;
        std::list<Checkpoint*>::iterator lastClosedChk = checkpointList.end();
        --lastClosedChk; --lastClosedChk; // Move to the lastest closed checkpoint.
        fastCursors.insert((*lastClosedChk)->getCursorNameList().begin(),
                           (*lastClosedChk)->getCursorNameList().end());
        std::list<Checkpoint*>::reverse_iterator rit = checkpointList.rbegin();
        ++rit; ++rit;// Move to the second lastest closed checkpoint.
        size_t numDuplicatedItems = 0, numMetaItems = 0;
        for (; rit != checkpointList.rend(); ++rit) {
            size_t numAddedItems = (*lastClosedChk)->mergePrevCheckpoint(*rit);
            numDuplicatedItems += ((*rit)->getNumItems() - numAddedItems);
            numMetaItems += 1; // checkpoint start meta item
            slowCursors.insert((*rit)->getCursorNameList().begin(),
                              (*rit)->getCursorNameList().end());
        }
        // Reposition the slow cursors to the beginning of the last closed checkpoint.
        std::set<std::string>::iterator sit = slowCursors.begin();
        for (; sit != slowCursors.end(); ++sit) {
            std::map<const std::string, CheckpointCursor>::iterator mit;
            mit = persistenceCursors.find(*sit);
            if (mit != persistenceCursors.end()) { // Reposition persistence cursor
                mit->second.currentCheckpoint = lastClosedChk;
                mit->second.currentPos =  (*lastClosedChk)->begin();
                mit->second.offset = 0;
                (*lastClosedChk)->registerCursorName(mit->second.name);
            } else if ((*sit).compare(onlineUpdateCursor.name) == 0) { // onlineUpdate cursor
                onlineUpdateCursor.currentCheckpoint = lastClosedChk;
                onlineUpdateCursor.currentPos =  (*lastClosedChk)->begin();
                onlineUpdateCursor.offset = 0;
                (*lastClosedChk)->registerCursorName(onlineUpdateCursor.name);
            } else { // Reposition tap cursors
                mit = tapCursors.find(*sit);
                if (mit != tapCursors.end()) {
                    mit->second.currentCheckpoint = lastClosedChk;
                    mit->second.currentPos =  (*lastClosedChk)->begin();
                    mit->second.offset = 0;
                    (*lastClosedChk)->registerCursorName(mit->second.name);
                }
            }
        }

        numItems -= (numDuplicatedItems + numMetaItems);
        Checkpoint *pOpenCheckpoint = checkpointList.back();
        const std::set<std::string> &openCheckpointCursors = pOpenCheckpoint->getCursorNameList();
        fastCursors.insert(openCheckpointCursors.begin(), openCheckpointCursors.end());
        std::set<std::string>::const_iterator cit = fastCursors.begin();
        // Update the offset of each fast cursor.
        for (; cit != fastCursors.end(); ++cit) {
            std::map<const std::string, CheckpointCursor>::iterator mit = persistenceCursors.find(*cit);
            if (mit != persistenceCursors.end()) {
                decrPersistenceCursorOffset(mit->second, numDuplicatedItems + numMetaItems);
            } else if ((*cit).compare(onlineUpdateCursor.name) == 0) {
                onlineUpdateCursor.offset -= (numDuplicatedItems + numMetaItems);
            } else {
                mit = tapCursors.find(*cit);
                if (mit != tapCursors.end()) {
                    mit->second.offset -= (numDuplicatedItems + numMetaItems);
                }
            }
        }
        collapsedChks.splice(collapsedChks.end(), checkpointList,
                             checkpointList.begin(),  lastClosedChk);
    }
}

bool CheckpointManager::queueDirty(const queued_item &item, const RCPtr<VBucket> &vbucket) {
    LockHolder lh(queueLock);
    if (vbucket->getState() != vbucket_state_active &&
        checkpointList.back()->getState() == closed) {
        // Replica vbucket might receive items from the master even if the current open checkpoint
        // has been already closed, because some items from the backfill with an invalid token
        // are on the wire even after that backfill thread is closed. Simply ignore those items.
        return false;
    }

    // The current open checkpoint should be always the last one in the checkpoint list.
    assert(checkpointList.back()->getState() == opened);
    size_t numItemsBefore = getNumItemsForPersistence_UNLOCKED();
    if (checkpointList.back()->queueDirty(item, this) == NEW_ITEM) {
        ++numItems;
    }
    size_t numItemsAfter = getNumItemsForPersistence_UNLOCKED();

    assert(vbucket);
    bool canCreateNewCheckpoint = false;
    if (checkpointList.size() < maxCheckpoints ||
        (checkpointList.size() == maxCheckpoints &&
         checkpointList.front()->getNumberOfCursors() == 0)) {
        canCreateNewCheckpoint = true;
    }
    if (vbucket->getState() == vbucket_state_active && !inconsistentSlaveCheckpoint &&
        canCreateNewCheckpoint) {
        // Only the master active vbucket can create a next open checkpoint.
        checkOpenCheckpoint_UNLOCKED(false, true);
    }
    // Note that the creation of a new checkpoint on the replica vbucket will be controlled by TAP
    // mutation messages from the active vbucket, which contain the checkpoint Ids.

    return (numItemsAfter - numItemsBefore) > 0 ? true : false;
}

uint64_t CheckpointManager::getAllItemsFromCurrentPosition(CheckpointCursor &cursor,
                                                           uint64_t barrier,
                                                           std::vector<queued_item> &items,
                                                           size_t upperThreshold,
                                                           size_t *itemsShifted) {
    size_t count = 0;
    while (true) {
        if ( barrier > 0 )  {
            if ((*(cursor.currentCheckpoint))->getId() >= barrier) {
                break;
            }
        }
        while (++(cursor.currentPos) != (*(cursor.currentCheckpoint))->end()) {
            items.push_back(*(cursor.currentPos));
            count++;
            if (upperThreshold && count == upperThreshold) {
                break;
            }
            if (itemsShifted) {
                (*itemsShifted)++;
            }
        }
        if (upperThreshold && count == upperThreshold) {
            // No decrement on cursor.currentPos since this condition is always met through the 'break' in the
            // while loop above, hence not performing the increment in the while condition
            break;
        }
        if ((*(cursor.currentCheckpoint))->getState() == closed) {
            if (!moveCursorToNextCheckpoint(cursor)) {
                --(cursor.currentPos);
                break;
            }
        } else { // The cursor is currently in the open checkpoint and reached to
                 // the end() of the open checkpoint.
            --(cursor.currentPos);
            break;
        }
    }

    uint64_t checkpointId = 0;
    // Get the last closed checkpoint Id.
    if(checkpointList.size() > 0) {
        uint64_t id = (*(cursor.currentCheckpoint))->getId();
        checkpointId = id > 0 ? id - 1 : 0;
    }

    return checkpointId;
}

uint64_t CheckpointManager::getAllItemsForPersistence(std::vector<queued_item> &items, int id, size_t upperThreshold) {
    LockHolder lh(queueLock);
    // If doOnlineUpdate is true, get all the items up to the start of the onlineUpdate cursor,
    // otherwise get all the items up to the end of the current open checkpoint, honoring upperThreshold
    // in either case.
    uint64_t barrier = doOnlineUpdate ? (*(onlineUpdateCursor.currentCheckpoint))->getId() : 0;
    size_t count = 0;
    uint64_t checkpointId = getAllItemsFromCurrentPosition(*(persistenceVector.at(id)),
                                                           barrier, items, upperThreshold, &count);
    persistenceVector.at(id)->offset += count;
    return checkpointId;
}

uint64_t CheckpointManager::getAllItemsForTAPConnection(const std::string &name,
                                                    std::vector<queued_item> &items) {
    LockHolder lh(queueLock);
    std::map<const std::string, CheckpointCursor>::iterator it = tapCursors.find(name);
    if (it == tapCursors.end()) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "The cursor for TAP connection \"%s\" is not found in the checkpoint.\n",
                         name.c_str());
        return 0;
    }
    uint64_t checkpointId = getAllItemsFromCurrentPosition(it->second, 0, items);
    it->second.offset = numItems;
    return checkpointId;
}

uint64_t CheckpointManager::getAllItemsForOnlineUpdate(std::vector<queued_item> &items) {
    LockHolder lh(queueLock);
    uint64_t checkpointId = 0;
    if (doOnlineUpdate) {
        // Get all the items up to the end of the current open checkpoint
        checkpointId = getAllItemsFromCurrentPosition(onlineUpdateCursor, 0, items);
        onlineUpdateCursor.offset += items.size();
    }

    return checkpointId;
}

queued_item CheckpointManager::nextItem(const std::string &name, bool &isLastMutationItem) {
    LockHolder lh(queueLock);
    isLastMutationItem = false;
    std::map<const std::string, CheckpointCursor>::iterator it = tapCursors.find(name);
    if (it == tapCursors.end()) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "The cursor for TAP connection \"%s\" is not found in the checkpoint.\n",
                         name.c_str());
        queued_item qi(new QueuedItem("", 0xffff, queue_op_empty));
        return qi;
    }
    if (checkpointList.back()->getId() == 0) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "VBucket %d CheckpointManager: Wait for backfill completion...\n",
                         vbucketId);
        queued_item qi(new QueuedItem("", 0xffff, queue_op_empty));
        return qi;
    }

    CheckpointCursor &cursor = it->second;
    if ((*(it->second.currentCheckpoint))->getState() == closed) {
        return nextItemFromClosedCheckpoint(cursor, isLastMutationItem);
    } else {
        return nextItemFromOpenedCheckpoint(cursor, isLastMutationItem);
    }
}

queued_item CheckpointManager::nextItemFromClosedCheckpoint(CheckpointCursor &cursor,
                                                            bool &isLastMutationItem) {
    ++(cursor.currentPos);
    if (cursor.currentPos != (*(cursor.currentCheckpoint))->end()) {
        ++(cursor.offset);
        isLastMutationItem = isLastMutationItemInCheckpoint(cursor);
        return *(cursor.currentPos);
    } else {
        if (!moveCursorToNextCheckpoint(cursor)) {
            --(cursor.currentPos);
            queued_item qi(new QueuedItem("", 0xffff, queue_op_empty));
            return qi;
        }

        // The cursor already reached to the beginning of the checkpoint that had "open" state
        // when registered. Simply return an empty item so that the corresponding TAP client
        // can close the connection.
        if (cursor.closedCheckpointOnly &&
            cursor.openChkIdAtRegistration <= (*(cursor.currentCheckpoint))->getId()) {
            queued_item qi(new QueuedItem("", vbucketId, queue_op_empty));
            return qi;
        }

        if ((*(cursor.currentCheckpoint))->getState() == closed) { // the close checkpoint.
            ++(cursor.currentPos); // Move the cursor to point to the actual first item.
            ++(cursor.offset);
            isLastMutationItem = isLastMutationItemInCheckpoint(cursor);
            return *(cursor.currentPos);
        } else { // the open checkpoint.
            return nextItemFromOpenedCheckpoint(cursor, isLastMutationItem);
        }
    }
}

queued_item CheckpointManager::nextItemFromOpenedCheckpoint(CheckpointCursor &cursor,
                                                            bool &isLastMutationItem) {
    if (cursor.closedCheckpointOnly) {
        queued_item qi(new QueuedItem("", vbucketId, queue_op_empty));
        return qi;
    }

    ++(cursor.currentPos);
    if (cursor.currentPos != (*(cursor.currentCheckpoint))->end()) {
        ++(cursor.offset);
        isLastMutationItem = isLastMutationItemInCheckpoint(cursor);
        return *(cursor.currentPos);
    } else {
        --(cursor.currentPos);
        queued_item qi(new QueuedItem("", 0xffff, queue_op_empty));
        return qi;
    }
}

void CheckpointManager::clear(vbucket_state_t vbState) {
    LockHolder lh(queueLock);
    std::list<Checkpoint*>::iterator it = checkpointList.begin();
    // Remove all the checkpoints.
    while(it != checkpointList.end()) {
        delete *it;
        ++it;
    }
    checkpointList.clear();
    numItems = 0;
    mutationCounter = 0;

    uint64_t checkpointId = vbState == vbucket_state_active ? 1 : 0;
    // Add a new open checkpoint.
    addNewCheckpoint_UNLOCKED(checkpointId);

    // Reset all persistence cursors.
    std::map<const std::string, CheckpointCursor>::iterator cit = persistenceCursors.begin();
    for (; cit != persistenceCursors.end(); ++cit) {
        cit->second.currentCheckpoint = checkpointList.begin();
        cit->second.currentPos = checkpointList.front()->begin();
        cit->second.offset = 0;
        checkpointList.front()->registerCursorName(cit->second.name);
    }

    // Reset all the TAP cursors.
    cit = tapCursors.begin();
    for (; cit != tapCursors.end(); ++cit) {
        cit->second.currentCheckpoint = checkpointList.begin();
        cit->second.currentPos = checkpointList.front()->begin();
        cit->second.offset = 0;
        checkpointList.front()->registerCursorName(cit->second.name);
    }
}

bool CheckpointManager::moveCursorToNextCheckpoint(CheckpointCursor &cursor) {
    if ((*(cursor.currentCheckpoint))->getState() == opened) {
        return false;
    } else if ((*(cursor.currentCheckpoint))->getState() == closed) {
        std::list<Checkpoint*>::iterator currCheckpoint = cursor.currentCheckpoint;
        if (++currCheckpoint == checkpointList.end()) {
            return false;
        }
    }

    // Remove the cursor's name from its current checkpoint.
    (*(cursor.currentCheckpoint))->removeCursorName(cursor.name);
    // Move the cursor to the next checkpoint.
    ++(cursor.currentCheckpoint);
    cursor.currentPos = (*(cursor.currentCheckpoint))->begin();
    // Register the cursor's name to its new current checkpoint.
    (*(cursor.currentCheckpoint))->registerCursorName(cursor.name);
    return true;
}

uint64_t CheckpointManager::checkOpenCheckpoint_UNLOCKED(bool forceCreation, bool timeBound) {
    int checkpointId = 0;
    timeBound = timeBound &&
                (ep_real_time() - checkpointList.back()->getCreationTime()) >= checkpointPeriod;
    // Create the new open checkpoint if any of the following conditions is satisfied:
    // (1) force creation due to online update or high memory usage
    // (2) current checkpoint is reached to the max number of items allowed.
    // (3) time elapsed since the creation of the current checkpoint is greater than the threshold
    if (forceCreation ||
        checkpointList.back()->getNumItems() >= checkpointMaxItems ||
        (checkpointList.back()->getNumItems() > 0 && timeBound)) {

        checkpointId = checkpointList.back()->getId();
        closeOpenCheckpoint_UNLOCKED(checkpointId);
        addNewCheckpoint_UNLOCKED(checkpointId + 1);
    }
    return checkpointId;
}

// Returns true if this key with the cas value is present in any checkpoint. 
// Ignores cas if -1 is passed.
bool CheckpointManager::isKeyResidentInCheckpoints(const std::string &key, uint64_t cas) {
    LockHolder lh(queueLock);

    std::list<Checkpoint*>::iterator it = checkpointList.begin();
    uint64_t cas_from_checkpoint;
    bool found = false;

    // Check if a given key with its CAS value exists in any of the checkpoints.
    for (; it != checkpointList.end(); ++it) {
        cas_from_checkpoint = (*it)->getCasForKey(key);
        if (cas == (uint64_t)-1) {
            if (cas_from_checkpoint != (uint64_t)-1) {
                found = true;
                break;
            }
        } else {
            if (cas == cas_from_checkpoint) {
                found = true;
                break;
            }
        }
    }
    lh.unlock();
    return found;
}

size_t CheckpointManager::getNumItemsForTAPConnection(const std::string &name) {
    LockHolder lh(queueLock);
    size_t remains = 0;
    std::map<const std::string, CheckpointCursor>::iterator it = tapCursors.find(name);
    if (it != tapCursors.end()) {
        remains = (numItems >= it->second.offset) ? numItems - it->second.offset : 0;
    }
    return remains;
}

void CheckpointManager::decrTapCursorFromCheckpointEnd(const std::string &name) {
    LockHolder lh(queueLock);
    std::map<const std::string, CheckpointCursor>::iterator it = tapCursors.find(name);
    if (it != tapCursors.end() &&
        (*(it->second.currentPos))->getOperation() == queue_op_checkpoint_end) {
        --(it->second.offset);
        --(it->second.currentPos);
    }
}

bool CheckpointManager::isLastMutationItemInCheckpoint(CheckpointCursor &cursor) {
    std::list<queued_item>::iterator it = cursor.currentPos;
    ++it;
    if (it == (*(cursor.currentCheckpoint))->end() ||
        (*it)->getOperation() == queue_op_checkpoint_end) {
        return true;
    }
    return false;
}

bool CheckpointManager::checkAndAddNewCheckpoint(uint64_t id, bool &pCursorRepositioned) {
    LockHolder lh(queueLock);

    // Ignore CHECKPOINT_START message with ID 0 as 0 is reserved for representing backfill.
    if (id == 0) {
        pCursorRepositioned = false;
        return true;
    }
    // If the replica receives a checkpoint start message right after backfill completion,
    // simply set the current open checkpoint id to the one received from the active vbucket.
    if (checkpointList.back()->getId() == 0) {
        setOpenCheckpointId_UNLOCKED(id);
        pCursorRepositioned = id > 0 ? true : false;
        return true;
    }

    std::list<Checkpoint*>::iterator it = checkpointList.begin();
    // Check if a checkpoint exists with ID >= id.
    while (it != checkpointList.end()) {
        if (id <= (*it)->getId()) {
            break;
        }
        ++it;
    }

    if (it == checkpointList.end()) {
        if ((checkpointList.back()->getId() + 1) < id) {
            isCollapsedCheckpoint = true;
            uint64_t oid = getOpenCheckpointId_UNLOCKED();
            lastClosedCheckpointId = oid > 0 ? (oid - 1) : 0;
        } else if ((checkpointList.back()->getId() + 1) == id) {
            isCollapsedCheckpoint = false;
        }
        pCursorRepositioned = false;
        if (checkpointList.back()->getState() == opened &&
            checkpointList.back()->getNumItems() == 0) {
            // If the current open checkpoint doesn't have any items, simply set its id to
            // the one from the master node.
            setOpenCheckpointId_UNLOCKED(id);
            // Reposition all the cursors in the open checkpoint to the begining position
            // so that a checkpoint_start message can be sent again with the correct id.
            const std::set<std::string> &cursors = checkpointList.back()->getCursorNameList();
            std::set<std::string>::const_iterator cit = cursors.begin();
            for (; cit != cursors.end(); ++cit) {
                std::map<const std::string, CheckpointCursor>::iterator mit =
                    persistenceCursors.find(*cit);
                if (mit != persistenceCursors.end()) {
                    mit->second.currentPos = checkpointList.back()->begin();
                } else if ((*cit).compare(onlineUpdateCursor.name) == 0) { // OnlineUpdate cursor
                    onlineUpdateCursor.currentPos = checkpointList.back()->begin();
                } else { // TAP cursors
                    mit = tapCursors.find(*cit);
                    mit->second.currentPos = checkpointList.back()->begin();
                }
            }
            return true;
        } else {
            closeOpenCheckpoint_UNLOCKED(checkpointList.back()->getId());
            return addNewCheckpoint_UNLOCKED(id);
        }
    } else {
        bool ret = true;
        bool persistenceCursorReposition = false;
        std::set<std::string> persistenceList;
        std::set<std::string> tapClients;
        std::list<Checkpoint*>::iterator curr = it;
        for (; curr != checkpointList.end(); ++curr) {
            std::map<const std::string, CheckpointCursor>::iterator map_it = persistenceCursors.begin();
            for (; map_it != persistenceCursors.end(); ++map_it) {
                if (*(map_it->second.currentCheckpoint) == *curr) {
                    persistenceList.insert(map_it->first);
                    persistenceCursorReposition = true;
                }
            }
            map_it = tapCursors.begin();
            for (; map_it != tapCursors.end(); ++map_it) {
                if (*(map_it->second.currentCheckpoint) == *curr) {
                    tapClients.insert(map_it->first);
                }
            }
            if ((*curr)->getState() == closed) {
                numItems -= ((*curr)->getNumItems() + 1); // 1 is for checkpoint start item.
            } else if ((*curr)->getState() == opened) {
                numItems -= ((*curr)->getNumItems() + 1); // 1 is for checkpoint start.
            }
            delete *curr;
        }
        checkpointList.erase(it, checkpointList.end());

        ret = addNewCheckpoint_UNLOCKED(id);
        if (ret) {
            if (checkpointList.back()->getState() == closed) {
                checkpointList.back()->popBackCheckpointEndItem();
                checkpointList.back()->setState(opened);
            }
            std::set<std::string>::iterator set_it = persistenceList.begin();
            for (; set_it != persistenceList.end(); ++set_it) {
                std::map<const std::string, CheckpointCursor>::iterator map_it =
                    persistenceCursors.find(*set_it);
                map_it->second.currentCheckpoint = --(checkpointList.end());
                map_it->second.currentPos = checkpointList.back()->begin();
                map_it->second.offset = numItems - 1;
                checkpointList.back()->registerCursorName(map_it->second.name);
            }
            set_it = tapClients.begin();
            for (; set_it != tapClients.end(); ++set_it) {
                std::map<const std::string, CheckpointCursor>::iterator map_it =
                    tapCursors.find(*set_it);
                map_it->second.currentCheckpoint = --(checkpointList.end());
                map_it->second.currentPos = checkpointList.back()->begin();
                map_it->second.offset = numItems - 1;
                checkpointList.back()->registerCursorName(map_it->second.name);
            }
        }

        pCursorRepositioned = persistenceCursorReposition;
        return ret;
    }
}

bool CheckpointManager::hasNext(const std::string &name) {
    LockHolder lh(queueLock);
    std::map<const std::string, CheckpointCursor>::iterator it = tapCursors.find(name);
    if (it == tapCursors.end()) {
        return false;
    }

    bool hasMore = true;
    std::list<queued_item>::iterator curr = it->second.currentPos;
    ++curr;
    if (curr == (*(it->second.currentCheckpoint))->end() &&
        (*(it->second.currentCheckpoint))->getState() == opened) {
        hasMore = false;
    }
    return hasMore;
}

bool CheckpointManager::hasNextForPersistence(int id) {
    LockHolder lh(queueLock);
    bool hasMore = true;
    CheckpointCursor cursor = *(persistenceVector.at(id));
    std::list<queued_item>::iterator curr = cursor.currentPos;
    ++curr;
    if (curr == (*(cursor.currentCheckpoint))->end() &&
        (*(cursor.currentCheckpoint))->getState() == opened) {
        hasMore = false;
    }
    return hasMore;
}

void CheckpointManager::initializeCheckpointConfig(size_t checkpoint_period,
                                                   size_t checkpoint_max_items,
                                                   size_t max_checkpoints,
                                                   bool allow_inconsistency,
                                                   bool keep_closed_checkpoints) {
    if (!validateCheckpointMaxItemsParam(checkpoint_max_items) ||
        !validateCheckpointPeriodParam(checkpoint_period) ||
        !validateMaxCheckpointsParam(max_checkpoints)) {
        return;
    }
    checkpointPeriod = checkpoint_period;
    checkpointMaxItems = checkpoint_max_items;
    maxCheckpoints = max_checkpoints;
    inconsistentSlaveCheckpoint = allow_inconsistency;
    keepClosedCheckpoints = keep_closed_checkpoints;
}

bool CheckpointManager::validateCheckpointMaxItemsParam(size_t checkpoint_max_items) {
    if (checkpoint_max_items < MIN_CHECKPOINT_ITEMS ||
        checkpoint_max_items > MAX_CHECKPOINT_ITEMS) {
        std::stringstream ss;
        ss << "New checkpoint_max_items param value " << checkpoint_max_items
           << " is not ranged between the min allowed value " << MIN_CHECKPOINT_ITEMS
           << " and max value " << MAX_CHECKPOINT_ITEMS;
        getLogger()->log(EXTENSION_LOG_WARNING, NULL, ss.str().c_str());
        return false;
    }
    return true;
}

bool CheckpointManager::validateCheckpointPeriodParam(size_t checkpoint_period) {
    if (checkpoint_period < MIN_CHECKPOINT_PERIOD ||
        checkpoint_period > MAX_CHECKPOINT_PERIOD) {
        std::stringstream ss;
        ss << "New checkpoint_period param value " << checkpoint_period
           << " is not ranged between the min allowed value " << MIN_CHECKPOINT_PERIOD
           << " and max value " << MAX_CHECKPOINT_PERIOD;
        getLogger()->log(EXTENSION_LOG_WARNING, NULL, ss.str().c_str());
        return false;
    }
    return true;
}

bool CheckpointManager::validateMaxCheckpointsParam(size_t max_checkpoints) {
    if (max_checkpoints < DEFAULT_MAX_CHECKPOINTS ||
        max_checkpoints > MAX_CHECKPOINTS_UPPER_BOUND) {
        std::stringstream ss;
        ss << "New max_checkpoints param value " << max_checkpoints
           << " is not ranged between the min allowed value " << DEFAULT_MAX_CHECKPOINTS
           << " and max value " << MAX_CHECKPOINTS_UPPER_BOUND;
        getLogger()->log(EXTENSION_LOG_WARNING, NULL, ss.str().c_str());
        return false;
    }
    return true;
}

queued_item CheckpointManager::createCheckpointItem(uint64_t id,
                                                    uint16_t vbid,
                                                    enum queue_operation checkpoint_op) {
    assert(checkpoint_op == queue_op_checkpoint_start || checkpoint_op == queue_op_checkpoint_end);
    uint64_t cid = htonll(id);
    RCPtr<Blob> vblob(Blob::New((const char*)&cid, sizeof(cid)));
    queued_item qi(new QueuedItem("", vblob, vbid, checkpoint_op));
    return qi;
}
