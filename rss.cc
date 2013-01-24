#include <fstream>
#include <cstdlib>
#include <cassert>

#if defined __APPLE__ && defined __MACH__
#include <mach/task.h>
#include <mach/mach_init.h>
#endif

#include "config.h"
#include "ep.hh"

#ifdef HAVE_JEMALLOC_JEMALLOC_H
#define JEMALLOC_MANGLE
#include "jemalloc/jemalloc.h"
#endif

#include "rss.hh"

size_t GetSelfRSS() {
#if defined __APPLE__ && defined __MACH__
    task_t task = MACH_PORT_NULL;

    if (task_for_pid(current_task(), getpid(), &task) == KERN_SUCCESS) {
        struct task_basic_info t_info;
        mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;
        task_info(task, TASK_BASIC_INFO, (task_info_t)&t_info, &t_info_count);
        return t_info.resident_size;
    }
    assert(1 == 0 && "Could not get task_for_pid");
    return 0; // should never reach here.
#else
    char buf[30];
    std::ifstream ifs;
    ifs.rdbuf()->pubsetbuf(buf,30);
    ifs.open("/proc/self/statm");
    size_t tSize = 0, resident = 0;
    ifs >> tSize >> resident;
    ifs.close();

    long page_size = sysconf(_SC_PAGE_SIZE);
    size_t rss = resident * page_size;
    assert(rss != 0);
    return rss;
#endif
}

void scrub_memory() {
#ifdef HAVE_JEMALLOC_JEMALLOC_H
    /*
    size_t sz;

    // get number of arenas
    unsigned int narenas = 0;
    sz = sizeof(unsigned int);
    mallctl("arenas.narenas", &narenas, &sz, NULL, 0);

    // purge unused dirty pages in arenas
    std::stringstream ss;
    ss << "arena." << narenas << ".purge";
    */
    mallctl("thread.tcache.flush", NULL, NULL, NULL, 0);
    mallctl("arenas.purge", NULL, NULL, NULL, 0);
    getLogger()->log(EXTENSION_LOG_INFO, NULL, "scrub_memory completed");
#endif
}

void print_allocator_stats() {
#ifdef HAVE_JEMALLOC_JEMALLOC_H
        malloc_stats_print(NULL, NULL, NULL);
#endif
}
