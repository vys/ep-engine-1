#include<iostream>
#include<map>
#include<string>
#include<assert.h>
#include "meta.h"

hashMetaData *hashMetaData::instance = NULL;

bool hashMetaData::setMetaData(std::string &key, rel_time_t exp,  std::string &metaData) { 
    Node *n = getBucket(exp);
    LockHolder lh(n->l);
    assert(n->exp <= exp); 
    if (n->exp < exp) {
        n->stored.destroy();
    } 
    n->exp = exp;
    return n->stored.insert(key, metaData);       
}

std::string hashMetaData::getMetaData(std::string &key, rel_time_t exp) {
    Node *n = getBucket(exp);
    LockHolder lh(n->l);
    return n->stored.get(key);    
}

bool hashMetaData::freeMetaData(std::string &key, rel_time_t exp) {
    Node *n = getBucket(exp);
    LockHolder lh(n->l);
    return n->stored.remove(key);    
}

bool useMap::insert(std::string &key, std::string &meta) {
    metaMap_t::iterator itr;
    if ((itr = mapData.find(key)) != mapData.end()) {
        assert(0);
        return false;
    }
    mapData[key] = meta;
    return true;
}

bool useMap::remove(std::string &key) {
    metaMap_t::iterator itr;
    if ((itr = mapData.find(key)) == mapData.end()) {
        assert(0);
        return false;
    }
    mapData.erase(itr);
    return true;
}

std::string useMap::get(std::string &key) {
    metaMap_t::iterator itr;
    if ((itr = mapData.find(key)) != mapData.end()) {
        return itr->second;
    }
    return "";
}

void useMap::destroy() {
    metaMap_t::iterator itr = mapData.begin();
    while (itr != mapData.end()) {
        mapData.erase(itr++); 
    }
}

#ifdef DEBUG
int main() {
rel_time_t t = 30;
std::string key = "test";
std::string test1 ="test123";
hashMetaData::getInstance()->setMetaData(key, t, test1);
std::cout << hashMetaData::getInstance()->getMetaData(key, t);
}
#endif

