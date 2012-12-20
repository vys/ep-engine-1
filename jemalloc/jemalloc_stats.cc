#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "jemalloc_stats.hh"
#include "locks.hh"

#if defined __APPLE__ && defined __MACH__
#include <mach/task.h>
#include <mach/mach_init.h>
#else
Mutex rssLock;
#endif

#define BUF_SIZE 1024
size_t GetSelfRSS();

size_t GetSelfRSS() {
#if defined __APPLE__ && defined __MACH__
    task_t task = MACH_PORT_NULL;

    if (task_for_pid(current_task(), getpid(), &task) == KERN_SUCCESS) {
        struct task_basic_info t_info;
        mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;
        task_info(task, TASK_BASIC_INFO, (task_info_t)&t_info, &t_info_count);
        return t_info.resident_size;
    }
    return 0;
#else
    static int fd = -1;
    char buf[BUF_SIZE];

    LockHolder lh(rssLock);
    if (fd < 1 ) {
        fd = open("/proc/self/statm", O_RDONLY);
    }

    int n = 0;
    if ((n = lseek(fd, 0, SEEK_SET)) < 0) {
        assert(0);
    }
    n = read(fd, buf, BUF_SIZE);
    if (n < 1) {
        assert(0);
        return 0;
    }
    lh.unlock();

    n = 0;

    int i;
    bool found = false;
    for (i = 0; i < BUF_SIZE; i++) {
        if (buf[i] == ' ') {
            n++;
        }
        if (n == 1) {
            found = true;
            break;
        }
    }

    return found ? atoll(&buf[i]) * 4096 : 0;
#endif
}

size_t JemallocStats::getJemallocMapped() {
    return GetSelfRSS();

}
#if 0
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
#endif
