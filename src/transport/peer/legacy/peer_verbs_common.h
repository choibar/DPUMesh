#ifndef DMESH_PEER_VERBS_COMMON_H
#define DMESH_PEER_VERBS_COMMON_H

/* ARM-only helpers shared by the SEND, WRITE+IMM and WRITE+Sync carriers.
 * Include after enabling GNU extensions for the mirrored memfd mapping. */
#include <fcntl.h>
#include <infiniband/verbs.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>
#include "peer_write_protocol.h"

static inline enum ibv_mtu peer_verbs_mtu(uint16_t bytes)
{
    switch (bytes) {
    case 256: return IBV_MTU_256;
    case 512: return IBV_MTU_512;
    case 1024: return IBV_MTU_1024;
    case 2048: return IBV_MTU_2048;
    case 4096: return IBV_MTU_4096;
    default: return 0;
    }
}

static inline int peer_verbs_nonblock(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    return flags < 0 ? -1 : fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static inline int peer_verbs_epoll_add(int epfd, int fd)
{
    struct epoll_event ev = {.events = EPOLLIN, .data = {.fd = fd}};
    return epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
}

static inline uint64_t peer_verbs_now_ns(void)
{
    struct timespec t;
    if (clock_gettime(CLOCK_MONOTONIC, &t))
        abort();
    return (uint64_t)t.tv_sec * 1000000000ull + (uint64_t)t.tv_nsec;
}

static inline int peer_write_capacity_valid(uint32_t capacity)
{
    long page = sysconf(_SC_PAGESIZE);
    return page > 0 && peer_write_capacity_in_range(capacity) && capacity % (uint64_t)page == 0;
}

/* Two adjacent mappings of one sealed backing object make a wrapped record
 * contiguous. The caller unregisters memory before unmapping 2 * bytes. */
static inline int peer_write_map_buffer(uint8_t **out, size_t bytes, const char *name)
{
    int fd = memfd_create(name, MFD_CLOEXEC | MFD_ALLOW_SEALING);
    if (fd < 0)
        return -1;
    if (ftruncate(fd, (off_t)bytes) ||
        fcntl(fd, F_ADD_SEALS, F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL)) {
        close(fd);
        return -1;
    }
    void *base = mmap(NULL, 2 * bytes, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (base == MAP_FAILED) {
        close(fd);
        return -1;
    }
    int ok = mmap(base, bytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0) != MAP_FAILED &&
        mmap((uint8_t *)base + bytes, bytes, PROT_READ | PROT_WRITE,
             MAP_SHARED | MAP_FIXED, fd, 0) != MAP_FAILED;
    close(fd);
    if (!ok) {
        munmap(base, 2 * bytes);
        return -1;
    }
    *out = base;
    return 0;
}

#endif
