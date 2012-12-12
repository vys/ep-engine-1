#include <unistd.h>
#include <stdint.h>
#include "jemalloc_stats.hh"
#include <jemalloc/jemalloc.h>

size_t JemallocStats::getJemallocMapped() {
    size_t param = -1;
    size_t sz_am = sizeof(param);
    uint64_t epoch = 1;
    size_t epoch_size = sizeof(epoch);
    mallctl("epoch", &epoch, &epoch_size, &epoch, sizeof(epoch));
    mallctl("stats.mapped", &param, &sz_am, NULL, 0);
    return param;
}

size_t JemallocStats::getJemallocAllocated() {
    size_t param = -1;
    size_t sz_am = sizeof(param);
    mallctl("stats.allocated", &param, &sz_am, NULL, 0);
    return param;
}

size_t JemallocStats::getJemallocActive() {
    size_t param = -1;
    size_t sz_am = sizeof(param);
    mallctl("stats.active", &param, &sz_am, NULL, 0);
    return param;
}
