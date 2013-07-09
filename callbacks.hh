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
#ifndef CALLBACKS_H
#define CALLBACKS_H 1

#include <cassert>

#include "locks.hh"

class Item;
class StoredValue;

/**
 * Value for callback for GET operations.
 */
class GetValue {
public:
    GetValue() : value(NULL), storedValue(NULL), id(-1),
                 vb_version(-1), status(ENGINE_KEY_ENOENT) { }

    explicit GetValue(Item *v, ENGINE_ERROR_CODE s=ENGINE_SUCCESS,
                      uint64_t i = -1, uint16_t vbucket_version = -1,
                      StoredValue *sv = NULL) :
        value(v), storedValue(sv), id(i), vb_version(vbucket_version), status(s) { }

    /**
     * The value retrieved for the key.
     */
    Item* getValue() { return value; }

    /**
     * Engine code describing what happened.
     */
    ENGINE_ERROR_CODE getStatus() const { return status; }

    /**
     * Set the status code
     */
    void setStatus(ENGINE_ERROR_CODE s) { status = s; }

    /**
     * Get the item's underlying ID (if applicable).
     */
    uint64_t getId() { return id; }

    /**
     * Get the item's vbucket version (if applicable).
     */
    uint16_t getVBucketVersion() { return vb_version; }

    /**
     * Get the StoredValue instance associated with the item (if applicable).
     */
    StoredValue* getStoredValue() const {
        return storedValue;
    }

private:

    Item* value;
    StoredValue* storedValue;
    uint64_t id;
    uint16_t vb_version;
    ENGINE_ERROR_CODE status;
};

/**
 * Possible results from a callback.
 * SUCCESS: The value was processed successfully
 * RETRY: The value could not be processed right now. Try again
 * ABORT: Applicable for long running operations (e.g. dump). Encountered 
 *        unrecoverable error, abort the operation.
 */
typedef enum CallbackResult_t {
    CB_SUCCESS = 0,
    CB_RETRY,
    CB_ABORT
} CallbackResult;

/**
 * Interface for callbacks from storage APIs.
 */
template <typename RV>
class Callback {
public:

    virtual ~Callback() {}

    /**
     * Method called on callback.
     */
    virtual CallbackResult callback(RV &value) = 0;

    virtual void setStatus(int status) {
        myStatus = status;
    }

    virtual int getStatus() {
        return myStatus;
    }

private:

    int myStatus;
};

/**
 * Threadsafe callback implementation that just captures the value.
 */
template <typename T>
class RememberingCallback : public Callback<T> {
public:

    /**
     * Construct a remembering callback.
     */
    RememberingCallback() : fired(false), so() { }

    /**
     * Clean up (including lock resources).
     */
    ~RememberingCallback() {
    }

    /**
     * The callback implementation -- just store a value.
     */
    CallbackResult callback(T &value) {
        LockHolder lh(so);
        val = value;
        fired = true;
        so.notify();
        return CB_SUCCESS;
    }

    /**
     * Wait for a value to be available.
     *
     * This method will return immediately if a value is currently
     * available, otherwise it will wait indefinitely for a value
     * to arrive.
     */
    void waitForValue() {
        LockHolder lh(so);
        if (!fired) {
            so.wait();
        }
        assert(fired);
    }

    /**
     * The value that was captured from the callback.
     */
    T    val;
    /**
     * True if the callback has fired.
     */
    bool fired;

private:
    SyncObject so;

    DISALLOW_COPY_AND_ASSIGN(RememberingCallback);
};

#endif /* CALLBACKS_H */
