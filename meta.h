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

//namespace meta {
typedef std::map<std::string, std::string> metaMap_t;
static const int max_lock_timeout = 30;

class useMap {
public:
    bool insert(std::string &key, std::string &meta);
    bool remove(std::string &key);
    std::string get(std::string &key);
    void destroy();
private:
    metaMap_t mapData ;
};

template <typename T>
class opr {
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
    //SpinLock l;     
    Mutex l;
	
#endif
    rel_time_t exp;
    opr<useMap> stored; 
};

class hashMetaData {
public:
    bool setMetaData(std::string &key, rel_time_t exp,  std::string &metaData);
    std::string getMetaData(std::string &key, rel_time_t exp);
    bool freeMetaData(std::string &key, rel_time_t exp);

    static hashMetaData *getInstance() {
        if (!instance) {
            instance = new hashMetaData();
        }
        return instance;
    }

    Node *getBucket(rel_time_t t) {
        return &nodes[t%30];
    }
 
private:
    hashMetaData() {
        //memset((void *)&nodes, 0, sizeof(Node) * max_lock_timeout);
#ifdef DEBUG
        for (int i=0; i< max_lock_timeout; i++) {
            pthread_mutex_init(&nodes[i].l, NULL);
        }        
#endif
    }

    static hashMetaData * instance; 
    Node nodes[max_lock_timeout];
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
