#ifndef JEMALLOC_STATS_HH
#define JEMALLOC_STATS_HH 1

class JemallocStats {
public:
    static size_t getJemallocMapped();
    static size_t getJemallocAllocated();
    static size_t getJemallocActive();
};
#endif
