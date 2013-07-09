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

#include <algorithm>

#include "ep_engine.h"
#include "tapconnmap.hh"
#include "tapconnection.hh"

/**
 * Dispatcher task to nuke a tap connection.
 */
class TapConnectionReaperCallback : public DispatcherCallback {
public:
    TapConnectionReaperCallback(EventuallyPersistentEngine &e, TapConnection *c)
        : engine(e), connection(c)
    {
        // Release resources reserved "upstream"
        const void *cookie = c->getCookie();
        if (cookie != NULL) {
            c->releaseReference();
        }
        std::stringstream ss;
        ss << "Reaping tap connection: " << connection->getName();
        descr = ss.str();
    }

    bool callback(Dispatcher &, TaskId) {
        if (connection->cleanSome()) {
            delete connection;
            return false;
        }
        return true;
    }

    std::string description() {
        return descr;
    }

private:
    EventuallyPersistentEngine &engine;
    TapConnection *connection;
    std::string descr;
};

void TapConnMap::disconnect(const void *cookie, int tapKeepAlive) {
    LockHolder clh(connMapMutex);
    std::map<const void*, TapConnection*>::iterator iter(map.find(cookie));
    if (iter != map.end()) {
        if (iter->second) {
            rel_time_t now = ep_current_time();
            TapConsumer *tc = dynamic_cast<TapConsumer*>(iter->second);
            if (tc || iter->second->doDisconnect()) {
                iter->second->setExpiryTime(now - 1);
            } else {
                iter->second->setExpiryTime(now + tapKeepAlive);
            }
            iter->second->setConnected(false);
        } else {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Found half-linked tap connection at: %p\n",
                             cookie);
        }
        map.erase(iter);

        clh.unlock();

        // Notify the daemon thread so that it may reap them..
        LockHolder lh(notifySync);
        notifySync.notify();
    }
}

bool TapConnMap::setEvents(const std::string &name,
                           std::list<queued_item> *q,
                           size_t &qlength,
                           size_t &qmemsize) {
    bool shouldNotify(true);
    bool found(false);
    LockHolder clh(connMapMutex);

    TapConnection *tc = findByName_UNLOCKED(name);
    if (tc) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc);
        assert(tp);
        found = true;
        tp->appendQueue(q, qlength, qmemsize);
        shouldNotify = tp->paused; // notify if paused
    }

    clh.unlock();
    if (shouldNotify) {
        LockHolder lh(notifySync);
        notifySync.notify();
    }

    return found;
}

ssize_t TapConnMap::backfillQueueDepth(const std::string &name) {
    ssize_t rv(-1);
    LockHolder clh(connMapMutex);

    TapConnection *tc = findByName_UNLOCKED(name);
    if (tc) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc);
        assert(tp);
        rv = tp->getBackfillRemaining();
    }

    return rv;
}

TapConnection* TapConnMap::findByName(const std::string &name) {
    LockHolder clh(connMapMutex);
    return findByName_UNLOCKED(name);
}

TapConnection* TapConnMap::findByName_UNLOCKED(const std::string&name) {
    TapConnection *rv(NULL);
    std::list<TapConnection*>::iterator iter;
    for (iter = all.begin(); iter != all.end(); ++iter) {
        TapConnection *tc = *iter;
        if (tc->getName() == name) {
            rv = tc;
        }
    }
    return rv;
}

void TapConnMap::getExpiredConnections_UNLOCKED(std::list<TapConnection*> &deadClients,
                                                std::list<TapConnection*> &regClients) {
    rel_time_t now = ep_current_time();

    std::list<TapConnection*>::iterator iter;
    for (iter = all.begin(); iter != all.end(); ++iter) {
        TapConnection *tc = *iter;
        if (tc->isConnected()) {
            continue;
        }

        TapProducer *tp = dynamic_cast<TapProducer*>(*iter);

        // Tap producer instances may override expiry 
        if (tp && tp->getExpiryTime() > now) {
            continue;
        }
        if (tc->getExpiryTime() <= now && !mapped(tc)) {
            if (tp) {
                if (!tp->suspended) {
                    deadClients.push_back(tc);
                    removeTapCursors_UNLOCKED(tp);
                }
            } else {
                deadClients.push_back(tc);
            }
            removeSession(tc->getName());
        } else if (tc->isReserved()) {
            if (tp == NULL || !tp->suspended) {
                // to avoid others to release it as well ;)
                tc->setReserved(false);
                regClients.push_back(tc);
            }
        }
    }

    // Remove them from the list of available tap connections...
    std::list<TapConnection*>::iterator ii;
    for (ii = deadClients.begin(); ii != deadClients.end(); ++ii) {
        all.remove(*ii);
    }
}

void TapConnMap::removeTapCursors_UNLOCKED(TapProducer *tp) {
    // If this TAP connection is not for the registered TAP client,
    // remove all the checkpoint cursors belonging to the TAP connection.
    if (tp && !tp->registeredTAPClient) {
        const VBucketMap &vbuckets = tp->engine.getEpStore()->getVBuckets();
        size_t numOfVBuckets = vbuckets.getSize();
        // Remove all the cursors belonging to the TAP connection to be purged.
        for (size_t i = 0; i < numOfVBuckets; ++i) {
            assert(i <= std::numeric_limits<uint16_t>::max());
            uint16_t vbid = static_cast<uint16_t>(i);
            RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
            if (!vb) {
                continue;
            }
            if (tp->vbucketFilter(vbid)) {
                vb->checkpointManager.removeTAPCursor(tp->name);
            }
        }
    }
}

void TapConnMap::addFlushEvent() {
    bool shouldNotify(false);
    LockHolder clh(connMapMutex);

    std::list<TapConnection*>::iterator iter;
    for (iter = all.begin(); iter != all.end(); iter++) {
        TapProducer *tc = dynamic_cast<TapProducer*>(*iter);
        if (tc && !tc->dumpQueue) {
            tc->flush();
            shouldNotify = true;
        }
    }

    clh.unlock();
    if (shouldNotify) {
        LockHolder lh(notifySync);
        notifySync.notify();
    }
}

TapConsumer *TapConnMap::newConsumer(const void* cookie, std::string &name, std::vector<uint16_t> &vbuckets)
{
    LockHolder clh(connMapMutex);

    // Check if any vbucket belongs to another tap consumer
    std::vector<uint16_t>::iterator vbit = vbuckets.begin();
    for (; vbit != vbuckets.end(); vbit++) {
        std::map<const void*, TapConnection*>::iterator it = map.begin();
        for (; it != map.end(); it++) {
            std::string type((*it).second->getType());

            if (type == "consumer" && (*it).second->vbucketFilter(*vbit)) {
                getLogger()->log(EXTENSION_LOG_INFO, NULL,
                                 "Failed to create tap consumer %s. VBucket %d already belongs to tap consumer %s\n",
                                 name.c_str(), *vbit, (*it).second->getName().c_str());
                return NULL;
            }
        }
    }

    TapConsumer *tap = new TapConsumer(engine, cookie, name, vbuckets);
    all.push_back(tap);
    map[cookie] = tap;
    return tap;
}

TapProducer *TapConnMap::newProducer(const void* cookie,
                                     const std::string &name,
                                     uint32_t flags,
                                     uint64_t backfillAge,
                                     int tapKeepAlive) {
    LockHolder clh(connMapMutex);
    TapProducer *tap(NULL), *oldTap(NULL);

    std::list<TapConnection*>::iterator iter;
    for (iter = all.begin(); iter != all.end(); ++iter) {
        tap = dynamic_cast<TapProducer*>(*iter);
        if (tap && tap->getName() == name) {
            tap->setExpiryTime((rel_time_t)-1);
            ++tap->reconnects;
            break;
        } else {
            tap = NULL;
        }
    }

    // Disconnects aren't quite immediate yet, so if we see a
    // connection request for a client *and* expiryTime is 0, we
    // should kill this guy off.
    if (tap != NULL) {
        std::map<const void*, TapConnection*>::iterator miter;
        for (miter = map.begin(); miter != map.end(); ++miter) {
            if (miter->second == tap) {
                break;
            }
        }

        if (tapKeepAlive == 0 || (tap->complete() && tap->idle())) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "The TAP channel (\"%s\") exists, but should be nuked\n",
                             name.c_str());
            tap->setName(TapConnection::getAnonName());
            tap->setDisconnect(true);
            tap->paused = true;
            oldTap = tap;
            tap = NULL;
        } else {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "The TAP channel (\"%s\") exists... grabbing the channel\n",
                             name.c_str());
            if (miter != map.end()) {
                TapProducer *n = new TapProducer(engine,
                                                 NULL,
                                                 TapConnection::getAnonName(),
                                                 0);
                n->setDisconnect(true);
                n->setConnected(false);
                n->paused = true;
                n->setSessionID((uint64_t) cookie);
                all.push_back(n);
                map[miter->first] = n;
            }
        }
    }

    bool reconnect = false;
    if (tap == NULL) {
        tap = new TapProducer(engine, cookie, name, flags);
        tap->setSessionID((uint64_t) cookie);
        addSession(name, (uint64_t) cookie);
        if (oldTap != NULL) {
            oldTap->transferItems(tap->backfilledItems);
        }
        all.push_back(tap);
    } else {
        if (tap->isReserved()) {
            assert(tap->getCookie() != NULL);
            tap->releaseReference();
        }
        tap->setCookie(cookie);
        tap->setReserved(true);
        tap->evaluateFlags();
        tap->rollback();
        tap->setConnected(true);
        tap->setDisconnect(false);
        reconnect = true;
    }

    tap->setBackfillAge(backfillAge, reconnect);
    setValidity(tap->getName(), cookie);

    map[cookie] = tap;
    return tap;
}

void TapConnMap::addSession(const std::string &name, uint64_t sessionID) {
    sessions[name] = sessionID;
}

void TapConnMap::removeSession(const std::string &name) {
    sessions.erase(name);
}

// These two methods are always called with a lock.
void TapConnMap::setValidity(const std::string &name,
                             const void* token) {
    validity[name] = token;
}
void TapConnMap::clearValidity(const std::string &name) {
    validity.erase(name);
}

// This is always called without a lock.
bool TapConnMap::checkValidity(const std::string &name,
                               const void* token) {
    LockHolder clh(connMapMutex);
    std::map<const std::string, const void*>::iterator viter =
        validity.find(name);
    return viter != validity.end() && viter->second == token;
}

bool TapConnMap::checkSessionValid(const std::string &name, uint64_t sessionID) {
    std::map<const std::string, uint64_t>::iterator viter =
        sessions.find(name);
    return viter != sessions.end() && viter->second == sessionID;
}

bool TapConnMap::checkConnectivity(const std::string &name) {
    LockHolder clh(connMapMutex);
    rel_time_t now = ep_current_time();
    TapConnection *tc = findByName_UNLOCKED(name);
    if (tc) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc);
        if (tp && (tp->isConnected() || tp->getExpiryTime() > now)) {
            return true;
        }
    }
    return false;
}

int TapConnMap::numBackfilledItems(const std::string &name, uint64_t sessionID) {
    LockHolder lh(connMapMutex);

    TapConnection *tc = findByName_UNLOCKED(name);
    if (tc && checkSessionValid(name, sessionID)) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc);
        assert (tp != NULL);
        return tp->numBackfilledItems();
    }

    return -1; // Handle this case for when the connection is lost
}

bool TapConnMap::checkBackfillCompletion(const std::string &name) {
    LockHolder clh(connMapMutex);
    bool rv = false;

    TapConnection *tc = findByName_UNLOCKED(name);
    if (tc) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc);
        assert(tp);
        rv = tp->checkBackfillCompletion();
    }
    return rv;
}

bool TapConnMap::mapped(TapConnection *tc) {
    bool rv = false;
    std::map<const void*, TapConnection*>::iterator it;
    for (it = map.begin(); it != map.end(); ++it) {
        if (it->second == tc) {
            rv = true;
        }
    }
    return rv;
}

bool TapConnMap::isPaused(TapProducer *tc) {
    return tc && tc->paused;
}

bool TapConnMap::shouldDisconnect(TapConnection *tc) {
    return tc && tc->doDisconnect();
}

void TapConnMap::shutdownAllTapConnections() {
    getLogger()->log(EXTENSION_LOG_INFO, NULL,
                     "Shutting down tap connections!");
    LockHolder clh(connMapMutex);
    // We should pause unless we purged some connections or
    // all queues have items.
    if (all.empty()) {
        return;
    }
    Dispatcher *d = engine.getEpStore()->getNonIODispatcher();
    std::list<TapConnection*>::iterator ii;
    for (ii = all.begin(); ii != all.end(); ++ii) {
        getLogger()->log(EXTENSION_LOG_INFO, NULL,
                         "Schedule cleanup of \"%s\"",
                         (*ii)->getName().c_str());
        d->schedule(shared_ptr<DispatcherCallback>
                    (new TapConnectionReaperCallback(engine, *ii)),
                    NULL, Priority::TapConnectionReaperPriority,
                    0, false, true);
    }
    all.clear();
    map.clear();
    validity.clear();
}

void TapConnMap::scheduleBackfill(const std::set<uint16_t> &backfillVBuckets) {
    LockHolder clh(connMapMutex);
    bool shouldNotify(false);
    rel_time_t now = ep_current_time();
    std::list<TapConnection*>::iterator it = all.begin();
    for (; it != all.end(); ++it) {
        TapConnection *tc = *it;
        TapProducer *tp = dynamic_cast<TapProducer*>(tc);
        if (!(tp && (tp->isConnected() || tp->getExpiryTime() > now))) {
            continue;
        }

        std::vector<uint16_t> vblist;
        std::set<uint16_t>::const_iterator vb_it = backfillVBuckets.begin();
        for (; vb_it != backfillVBuckets.end(); ++vb_it) {
            if (tp->checkVBucketFilter(*vb_it)) {
                vblist.push_back(*vb_it);
            }
        }
        if (vblist.size() > 0) {
            tp->scheduleBackfill(vblist);
            shouldNotify = true;
        }
    }
    clh.unlock();
    if (shouldNotify) {
        LockHolder lh(notifySync);
        notifySync.notify();
    }
}

void TapConnMap::resetReplicaChain() {
    LockHolder clh(connMapMutex);
    rel_time_t now = ep_current_time();
    std::list<TapConnection*>::iterator it = all.begin();
    for (; it != all.end(); ++it) {
        TapConnection *tc = *it;
        TapProducer *tp = dynamic_cast<TapProducer*>(tc);
        if (!(tp && (tp->isConnected() || tp->getExpiryTime() > now))) {
            continue;
        }
        // Get the list of vbuckets that each TAP producer is replicating
        const std::vector<uint16_t> &vblist = tp->getVBucketFilter().getVector();
        // TAP producer sends INITIAL_VBUCKET_STREAM messages to the destination to reset
        // replica vbuckets, and then backfills items to the destination.
        tp->scheduleBackfill(vblist);
    }
    clh.unlock();

    LockHolder lh(notifySync);
    notifySync.notify();
}

void TapConnMap::notifyIOThreadMain() {
    // To avoid connections to be stucked in a bogus state forever, we're going
    // to ping all connections that hasn't tried to walk the tap queue
    // for this amount of time..
    const int maxIdleTime = 5;

    bool addNoop = false;

    rel_time_t now = ep_current_time();
    if (now > engine.nextTapNoop && engine.tapNoopInterval != (size_t)-1) {
        addNoop = true;
        engine.nextTapNoop = now + engine.tapNoopInterval;
    }

    std::list<TapConnection*> deadClients;
    std::list<TapConnection*> registeredClients;

    LockHolder clh(connMapMutex);
    // We should pause unless we purged some connections or
    // all queues have items.
    getExpiredConnections_UNLOCKED(deadClients, registeredClients);
    bool shouldPause = deadClients.empty() && registeredClients.empty();
    bool noEvents = engine.mutation_count == 0;
    engine.mutation_count = 0;

    if (shouldPause) {
        shouldPause = noEvents;
    }
    // see if I have some channels that I have to signal..
    std::map<const void*, TapConnection*>::iterator iter;
    for (iter = map.begin(); iter != map.end(); ++iter) {
        TapProducer *tp = dynamic_cast<TapProducer*>(iter->second);
        if (tp != NULL) {
            if (tp->supportsAck() && (tp->getExpiryTime() < now) && tp->windowIsFull()) {
                shouldPause = false;
                tp->setDisconnect(true);
            } else if (addNoop) {
                tp->setTimeForNoop();
                shouldPause = false;
            } else if (tp->doDisconnect() || !tp->idle()) {
                shouldPause = false;
            } else if ((tp->lastWalkTime + maxIdleTime) < now) {
                shouldPause = false;
            }
        }
    }

    if (shouldPause) {
        double diff = engine.nextTapNoop - now;
        if (diff > 0) {
            clh.unlock();
            wait(diff);
            clh.lock();
        }

        if (engine.shutdown) {
            return;
        }

        getExpiredConnections_UNLOCKED(deadClients, registeredClients);
        now = ep_current_time();
    }

    // Collect the list of connections that need to be signaled.
    std::list<const void *> toNotify;
    for (iter = map.begin(); iter != map.end(); ++iter) {
        TapProducer *tp = dynamic_cast<TapProducer*>(iter->second);
        if (tp && (tp->paused || tp->doDisconnect()) && !tp->suspended) {
            if (!tp->notifySent || (tp->lastWalkTime + maxIdleTime < now)) {
                tp->notifySent.set(true);
                toNotify.push_back(iter->first);
            }
        }
    }

    if (!registeredClients.empty()) {
        std::list<TapConnection*>::iterator ii;
        for (ii = registeredClients.begin(); ii != registeredClients.end(); ++ii) {
            (*ii)->releaseReference(true);
        }
    }

    clh.unlock();

    // Delete all of the dead clients
    if (!deadClients.empty()) {
        Dispatcher *d = engine.getEpStore()->getNonIODispatcher();
        std::list<TapConnection*>::iterator ii;
        for (ii = deadClients.begin(); ii != deadClients.end(); ++ii) {
            d->schedule(shared_ptr<DispatcherCallback>
                        (new TapConnectionReaperCallback(engine, *ii)),
                        NULL, Priority::TapConnectionReaperPriority,
                        0, false, true);
        }
    }

    engine.notifyIOComplete(toNotify, ENGINE_SUCCESS);
}

bool TapConnMap::SetCursorToOpenCheckpoint(const std::string &name, uint16_t vbucket) {
    bool rv(false);
    LockHolder clh(connMapMutex);

    TapConnection *tc = findByName_UNLOCKED(name);
    if (tc) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc);
        assert(tp);
        rv = true;
        tp->SetCursorToOpenCheckpoint(vbucket);
    }

    return rv;
}

bool TapConnMap::getConnectionFlags(const std::string &name, uint32_t &flags) {
    bool rv(false);
    LockHolder clh(connMapMutex);

    TapConnection *tc = findByName_UNLOCKED(name);
    if (tc) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc);
        assert(tp);
        rv = true;
        flags = tp->getFlags();
    }

    return rv;
}

bool TapConnMap::closeTapConnectionByName(const std::string &name) {
    bool rv = false;
    LockHolder clh(connMapMutex);
    TapConnection *tc = findByName_UNLOCKED(name);
    if (tc) {
        TapProducer *tp = dynamic_cast<TapProducer*>(tc);
        if (tp) {
            tp->setRegisteredClient(false);
            removeTapCursors_UNLOCKED(tp);

            tp->setExpiryTime(ep_current_time() - 1);
            removeSession(name);
            tp->setName(TapConnection::getAnonName());
            tp->setDisconnect(true);
            tp->paused = true;
            rv = true;
            clh.unlock();
            LockHolder lh(notifySync);
            notifySync.notify();
        }
    }
    return rv;
}

/**
 * Increments reference count of validity token (cookie in
 * fact). NOTE: takes connMapMutex lock.
 */
ENGINE_ERROR_CODE TapConnMap::reserveValidityToken(const void *token) {
    LockHolder clh(connMapMutex);
    return engine.getServerApi()->cookie->reserve(token);
}

/**
 * Decrements and posibly frees/invalidate validity token (cookie
 * in fact). NOTE: this acquires connMapMutex lock.
 */
void TapConnMap::releaseValidityToken(const void *token) {
    LockHolder clh(connMapMutex);
    engine.getServerApi()->cookie->release(token);
}

void CompleteBackfillTapOperation::perform(TapProducer *tc, void *) {
    tc->completeBackfill();
}

void CompleteDiskBackfillTapOperation::perform(TapProducer *tc, void *) {
    tc->completeDiskBackfill();
}

void ScheduleDiskBackfillTapOperation::perform(TapProducer *tc, void *) {
    tc->scheduleDiskBackfill(count);
}

void ReceivedItemTapOperation::perform(TapProducer *tc, Item *arg) {
    tc->gotBGItem(arg, implicitEnqueue);
}

void CompletedBGFetchTapOperation::perform(TapProducer *tc,
                                           EventuallyPersistentEngine *) {
    tc->completedBGFetchJob();
}
