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
#ifndef _META_H_
#define _META_H_

#include<iostream>
#include<string>
#include<map>

#ifdef DEBUG
typedef uint32_t rel_time_t;
#else
#include "locks.hh"
#endif

typedef std::map<std::string, std::string> metamap_t;

class UseMap {
public:
    bool insert(std::string &key, std::string &meta);
    bool remove(std::string &key);
    std::string get(std::string &key);
    void destroy();
private:
    metamap_t mapData ;
};

template <typename T>
class Opr {
public:
    bool insert (std::string &key, std::string &metaData) {
        return data.insert(key, metaData);
    }
    bool remove(std::string &key) {
        return data.remove(key);
    } 
    std::string get(std::string &key) {
        return data.get(key);
    }
    void destroy() {
        data.destroy();
    }
private:
    T data;
};

class Node {
public:
    Node():exp(0) { }
#ifdef DEBUG
    pthread_mutex_t l;
#else
    Mutex l;

#endif
    rel_time_t exp;
    Opr<UseMap> stored; 
};

class HashMetaData {
public:
    bool setMetaData(std::string &key, rel_time_t exp,  std::string &metaData);
    std::string getMetaData(std::string &key, rel_time_t exp);
    bool freeMetaData(std::string &key, rel_time_t exp);
    void initialize(size_t timeout);

    static HashMetaData *getInstance() {
        if (!instance) {
            instance = new HashMetaData();
        }
        return instance;
    }

    Node *getBucket(rel_time_t t) {
        return &nodes[t%maxLockTimeout];
    }

private:
    HashMetaData() {
#ifdef DEBUG
        for (int i=0; i< 30; i++) {
            pthread_mutex_init(&nodes[i].l, NULL);
        }  
#endif
    }

    size_t maxLockTimeout;
    static HashMetaData * instance; 
    Node *nodes;
};

#ifdef DEBUG 
class SpinLockHolder {
public:
    SpinLockHolder(pthread_mutex_t &p):q(p) {
        pthread_mutex_lock(&p);
    }
    ~SpinLockHolder() {
        pthread_mutex_unlock(&q);
    }
private:
    pthread_mutex_t q;    
};

typedef SpinLockHolder LockHolder;
#endif

#endif
