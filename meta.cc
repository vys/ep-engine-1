#include<iostream>
#include<map>
#include<string>
#include<assert.h>
#include "meta.hh"

HashMetaData *HashMetaData::instance = NULL;

void HashMetaData::initialize(size_t maxGetlTimeOut)
{
    maxLockTimeout = maxGetlTimeOut;
    nodes = new Node[maxLockTimeout];
    if (!nodes) {
        assert(0);
    }
}

bool HashMetaData::setMetaData(std::string &key, rel_time_t exp,  std::string &metaData) { 
    Node *n = getBucket(exp);
    LockHolder lh(n->l);
    assert(n->exp <= exp); 
    if (n->exp <= exp) {
        n->stored.destroy();
    } 
    n->exp = exp;
    return n->stored.insert(key, metaData);       
}

std::string HashMetaData::getMetaData(std::string &key, rel_time_t exp) {
    Node *n = getBucket(exp);
    LockHolder lh(n->l);
    return n->stored.get(key);    
}

bool HashMetaData::freeMetaData(std::string &key, rel_time_t exp) {
    Node *n = getBucket(exp);
    LockHolder lh(n->l);
    return n->stored.remove(key);    
}

bool UseMap::insert(std::string &key, std::string &meta) {
    metamap_t::iterator itr;
    if ((itr = mapData.find(key)) != mapData.end()) {
        assert(0);
        return false;
    }
    mapData[key] = meta;
    return true;
}

bool UseMap::remove(std::string &key) {
    metamap_t::iterator itr;
    if ((itr = mapData.find(key)) != mapData.end()) {
        mapData.erase(itr);
    }
    return true;
}

std::string UseMap::get(std::string &key) {
    metamap_t::iterator itr;
    if ((itr = mapData.find(key)) != mapData.end()) {
        return itr->second;
    }
    return "";
}

void UseMap::destroy() {
    metamap_t::iterator itr = mapData.begin();
    while (itr != mapData.end()) {
        mapData.erase(itr++); 
    }
}

#ifdef DEBUG
int main() {
rel_time_t t = 30;
std::string key = "test";
std::string test1 ="test123";
HashMetaData::getInstance()->initialize(30);
HashMetaData::getInstance()->setMetaData(key, t, test1);
std::cout << HashMetaData::getInstance()->getMetaData(key, t);
}
#endif

