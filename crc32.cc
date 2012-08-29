#include <iostream>
#include "crc32.hh"
#include "item.hh"

/*initialize the classes for the data integrity*/
Crc32 *DataIntegrity::m_crc = new Crc32();
DisabledCksum *DataIntegrity::m_dis = new DisabledCksum(); 

DataIntegrity *DataIntegrity::getDi(const char *str) {
    switch (GET_CRC_META(str)) {
        case CRC32:
            return m_crc;
        default:
            /*returned the dummy class*/
            return m_dis;
    }
}

bool DataIntegrity::validateCksumMetaData(const char *cksum) {
    return (strlen(cksum) > 4 && (GET_CRC_META(cksum) == DISABLED_CRC ||
                GET_CRC_META(cksum) == CRC32) && cksum[4] == ':');
}

bool DataIntegrity::isDataCorrupt(const std::string &str) {
    return (str.compare(0, 4, CORRUPT_CRC) == 0);         
}  

std::string Crc32::getCksum(const char *key, int keyLen, const char *old, int offset) {
    unsigned int crc = ~(old ? strtol(old, NULL, 16) : 0);

    for (int i = offset; i<keyLen; i++) {
        __CRC32(crc, key[i]);
    }

    std::stringstream ss;
    ss << std::setfill('0') << std::setw(8) << std::hex << ~crc;
    return ss.str();    
}

std::string Crc32::getCksum(Item *it) {
    uint32_t flags = ntohl(it->getFlags());
    const char *c = (getCksum((char*)&flags, sizeof(uint32_t))).c_str();
    return getCksum(it->getValue()->getData(), it->getNBytes(), c);
}

bool Crc32::hasCksum(std::string cksum) {
    return cksum.size() > 4 && 
        GET_CRC_META(cksum.c_str()) == CRC32;
}

bool Crc32::verifyCksum(Item *it) {
    return (strncmp(it->getCksumData(), getCksum(it).c_str(), 8) == 0);
}

