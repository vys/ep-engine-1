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
#ifndef ITEM_HH
#define ITEM_HH
#include "config.h"

#include <string>
#include <string.h>
#include <stdio.h>
#include <memcached/engine.h>

#include "crc32.hh"
#include "mutex.hh"
#include "locks.hh"
#include "atomic.hh"
#include "objectregistry.hh"
#include "stats.hh"
#include "crc32.hh"

/**
 * A blob is a minimal sized storage for data up to 2^32 bytes long.
 */
class Blob : public RCValue {
public:

    // Constructors.

    /**
     * Create a new Blob holding the given data.
     *
     * @param start the beginning of the data to copy into this blob
     * @param len the amount of data to copy in
     *
     * @return the new Blob instance
     */
    static Blob* New(const char *start, const size_t len) {
        size_t total_len = len + sizeof(Blob);
        Blob *t = new (::operator new(total_len)) Blob(start, len);
        assert(t->length() == len);
        return t;
    }

    /**
     * Create a new Blob holding the contents of the given string.
     *
     * @param s the string whose contents go into the blob
     *
     * @return the new Blob instance
     */
    static Blob* New(const std::string& s) {
        return New(s.data(), s.length());
    }

    /**
     * Create a new Blob pre-filled with the given character.
     *
     * @param len the size of the blob
     * @param c the character to fill the blob with
     *
     * @return the new Blob instance
     */
    static Blob* New(const size_t len, const char c) {
        size_t total_len = len + sizeof(Blob);
        Blob *t = new (::operator new(total_len)) Blob(c, len);
        assert(t->length() == len);
        return t;
    }

    // Actual accessorish things.

    /**
     * Get the pointer to the contents of this Blob.
     */
    const char* getData() const {
        return data;
    }

    /**
     * Get the length of this Blob value.
     */
    size_t length() const {
        return size;
    }

    /**
     * Get the size of this Blob instance.
     */
    size_t getSize() const {
        return size + sizeof(Blob);
    }

    /**
     * Get a std::string representation of this blob.
     */
    const std::string to_s() const {
        return std::string(data, size);
    }

    // This is necessary for making C++ happy when I'm doing a
    // placement new on fairly "normal" c++ heap allocations, just
    // with variable-sized objects.
    void operator delete(void* p) { ::operator delete(p); }

    ~Blob() {
        ObjectRegistry::onDeleteBlob(this);
    }

private:

    explicit Blob(const char *start, const size_t len) :
        size(static_cast<uint32_t>(len))
    {
        std::memcpy(data, start, len);
        ObjectRegistry::onCreateBlob(this);
    }

    explicit Blob(const char c, const size_t len) :
        size(static_cast<uint32_t>(len))
    {
        std::memset(data, c, len);
        ObjectRegistry::onCreateBlob(this);
    }

    const uint32_t size;
    char data[1];

    DISALLOW_COPY_AND_ASSIGN(Blob);
};

typedef RCPtr<Blob> value_t;

/**
 * The Item structure we use to pass information between the memcached
 * core and the backend. Please note that the kvstore don't store these
 * objects, so we do have an extra layer of memory copying :(
 */
class Item {
public:
    Item(const void* k, const size_t nk, const size_t nb,
         const uint32_t fl, const time_t exp, const std::string &_cksum, 
         uint64_t theCas = 0, int64_t i = -1, uint16_t vbid = 0, const time_t qtime = -1) :
        flags(fl), queued(qtime), exptime(exp), cas(theCas), id(i),
        vbucketId(vbid)
    {
        cksum.assign(_cksum);
        key.assign(static_cast<const char*>(k), nk);
        assert(id != 0);
        setData(NULL, nb);
        ObjectRegistry::onCreateItem(this);
    }
   
    Item(const void* k, const size_t nk, const size_t nb,
        const uint32_t fl, const time_t exp, const char *_cksum, const size_t nck,
        uint64_t theCas = 0, int64_t i = -1, uint16_t vbid = 0, const time_t qtime = -1) :
        flags(fl), queued(qtime), exptime(exp), cas(theCas), id(i),
        vbucketId(vbid)
    {
        if (nck > 0) {
            cksum.assign(_cksum, nck);
        } else {
            cksum.assign(DISABLED_CRC_STR);
        } 
        key.assign(static_cast<const char*>(k), nk);
        assert(id != 0);
        setData(NULL, nb);
        ObjectRegistry::onCreateItem(this);
    }

    Item(const std::string &k, const uint32_t fl, const time_t exp,
         const void *dta, const size_t nb, const std::string &_cksum, 
         uint64_t theCas = 0, int64_t i = -1, uint16_t vbid = 0, const time_t qtime = -1) :
        flags(fl), queued(qtime), exptime(exp), cas(theCas), id(i),
        vbucketId(vbid)
    {
        cksum.assign(_cksum);
        key.assign(k);
        assert(id != 0);
        setData(static_cast<const char*>(dta), nb);
        ObjectRegistry::onCreateItem(this);
    }

    Item(const std::string &k, const uint32_t fl, const time_t exp,
         const value_t &val, const std::string &_cksum, uint64_t theCas = 0,  
         int64_t i = -1, uint16_t vbid = 0, const time_t qtime = -1) :
        flags(fl), queued(qtime), exptime(exp), value(val), cas(theCas), id(i), vbucketId(vbid)
    {
        cksum.assign(_cksum);
        assert(id != 0);
        key.assign(k);
        ObjectRegistry::onCreateItem(this);
    }

    Item(const void *k, uint16_t nk, const uint32_t fl, const time_t exp,
         const void *dta, const size_t nb, const void *_cksum, const size_t cb,
         uint64_t theCas = 0, int64_t i = -1, uint16_t vbid = 0, const time_t qtime = -1) :
        flags(fl), queued(qtime), exptime(exp), cas(theCas), id(i), vbucketId(vbid)
    {
        cksum.assign(static_cast<const char*>(_cksum), cb);
        assert(id != 0);
        key.assign(static_cast<const char*>(k), nk);
        setData(static_cast<const char*>(dta), nb);
        ObjectRegistry::onCreateItem(this);
    }

    ~Item() {
        ObjectRegistry::onDeleteItem(this);
    }

    const char *getData() const {
        return value->getData();
    }

    value_t& getValue() {
        return value;
    }

    const std::string &getKey() const {
        return key;
    }

    int64_t getId() const {
        return id;
    }

    void setId(int64_t to) {
        id = to;
    }

    int getNKey() const {
        return static_cast<int>(key.length());
    }

    uint32_t getNBytes() const {
        return static_cast<uint32_t>(value->length());
    }

    time_t getQueuedTime() const {
        return queued;
    }

    void setQueuedTime(time_t to) {
        queued = to;
    }

    time_t getExptime() const {
        return exptime;
    }

    uint32_t getFlags() const {
        return flags;
    }
    
    const char *getCksumMeta() const {
        return cksum.c_str();
    }

    const char *getCksumData() const {
        return cksum.c_str()+5;
    }

    const std::string &getCksum() const {
        return cksum;
    }

    uint64_t getCas() const {
        return cas;
    }

    void setCas() {
        cas = nextCas();
    }

    void setCas(uint64_t ncas) {
        cas = ncas;
    }

    void setValue(const value_t &v) {
        value.reset(v);
    }

    void setFlags(uint32_t f) {
        flags = f;
    }

    void setCksum(const char *ck) {
        cksum.assign(ck);
    }

    void setCksum(std::string ck) {
        cksum.assign(ck);
    }

    void setCksumMeta(std::string &ck) {
        cksum.replace(0, ck.size(), ck);
    }

    void setCksumData(std::string &ck) {
        cksum.replace(5, ck.size(), ck);
    }

    void setCorruptCksumFlag() {
        cksum.replace(0, CORRUPT_CRC_LEN, CORRUPT_CRC);    
    }

    void setExpTime(time_t exp_time) {
        exptime = exp_time;
    }

    /**
     * Append another item to this item
     *
     * @param item the item to append to this one
     * @return true if success
     */
    bool append(Item &item);

    /**
     * Prepend another item to this item
     *
     * @param item the item to prepend to this one
     * @return true if success
     */
    bool prepend(Item &item);

    uint16_t getVBucketId(void) const {
        return vbucketId;
    }

    void setVBucketId(uint16_t to) {
        vbucketId = to;
    }

    /**
     * Check if this item is expired or not.
     *
     * @param asOf the time to be compared with this item's expiry time
     * @return true if this item's expiry time < asOf
     */
    bool isExpired(time_t asOf) const {
        if (getExptime() != 0 && getExptime() < asOf) {
            return true;
        }
        return false;
    }

    size_t size() {

        size_t sz = sizeof(Item) + key.size() + cksum.size();
        if (value) {
            sz += value->getSize();
        }
        return sz;
    }

    size_t overhead() {
        return sizeof(Item) + key.size();
    }

private:
    /**
     * Set the item's data. This is only used by constructors, so we
     * make it private.
     */
    void setData(const char *dta, const size_t nb) {
        Blob *data;
        if (dta == NULL) {
            data = Blob::New(nb, '\0');
        } else {
            data = Blob::New(dta, nb);
        }

        assert(data);
        value.reset(data);
    }

    uint32_t flags;
    time_t queued;
    time_t exptime;
    std::string key;
    value_t value;
    std::string cksum;
    uint64_t cas;
    int64_t id;
    uint16_t vbucketId;

    static uint64_t nextCas(void) {
        uint64_t ret;
        ret = casCounter++;

        return ret;
    }

    static Atomic<uint64_t> casCounter;
    DISALLOW_COPY_AND_ASSIGN(Item);
};

#endif
