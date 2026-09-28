#include "service_resolve.h"
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <netdb.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>

#define TARGET_MAX (DMESH_TARGET_HOST_MAX + 7)   /* host + ":65535" + NUL */

struct answer {
    char target[TARGET_MAX];
    int error;                         /* 0, or ENOENT for a name with no address */
    uint32_t ipv4;
    uint16_t port;
    uint64_t expires_ns;               /* 0 = empty or invalidated */
};
struct address { uint32_t ipv4; uint16_t port; };

static pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;
static struct answer answers[DMESH_RESOLVE_ENTRIES];
static unsigned next_answer;
static struct address ids[DMESH_RESOLVE_IDS];
static int n_ids;
static char targets[DMESH_TARGETS_MAX][TARGET_MAX];
static uint16_t target_ports[DMESH_TARGETS_MAX];
static int n_targets = -1;             /* -1 until $DPUMESH_TARGETS is read */
static int targets_error;
static uint64_t clock_offset;
static int (*gai)(const char *, const char *, const struct addrinfo *, struct addrinfo **) = getaddrinfo;
static void (*freeai)(struct addrinfo *) = freeaddrinfo;

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec + clock_offset;
}
/* Splits "<host>:<port>". 0, or -1 + EINVAL. */
static int parse_target(const char *target, char *host, uint16_t *port)
{
    const char *colon = target ? strrchr(target, ':') : NULL;
    size_t len = colon ? (size_t)(colon - target) : 0;
    if (!len || len > DMESH_TARGET_HOST_MAX) { errno = EINVAL; return -1; }
    for (size_t i = 0; i < len; ++i)
        if (!isalnum((unsigned char)target[i]) && target[i] != '.' && target[i] != '-' && target[i] != '_') {
            errno = EINVAL; return -1;
        }
    char *end;
    long p = strtol(colon + 1, &end, 10);
    if (!isdigit((unsigned char)colon[1]) || *end || p < 1 || p > 65535) { errno = EINVAL; return -1; }
    memcpy(host, target, len); host[len] = 0;
    *port = (uint16_t)p;
    return 0;
}
/* A name without an IPv4 address is absent; any other failure is no answer. */
static int gai_errno(int rc)
{
    switch (rc) {
    case EAI_NONAME:
#ifdef EAI_NODATA
    case EAI_NODATA:
#endif
#ifdef EAI_ADDRFAMILY
    case EAI_ADDRFAMILY:
#endif
        return ENOENT;
    case EAI_MEMORY: return ENOMEM;
    default: return EAGAIN;
    }
}
/* Caller holds mu. */
static struct answer *cached(const char *target, uint64_t now)
{
    for (int i = 0; i < DMESH_RESOLVE_ENTRIES; ++i)
        if (answers[i].expires_ns > now && !strcmp(answers[i].target, target)) return &answers[i];
    return NULL;
}
/* target -> ipv4:port. An IPv4 literal is its own answer; a name goes through
 * the cache. A missing name is cached like an address; no answer is not, so
 * the next lookup asks again. */
static int lookup(const char *target, uint32_t *ipv4, uint16_t *port)
{
    char host[DMESH_TARGET_HOST_MAX + 1];
    if (parse_target(target, host, port) != 0) return -1;
    if (inet_pton(AF_INET, host, ipv4) == 1) return 0;
    pthread_mutex_lock(&mu);
    struct answer *a = cached(target, now_ns());
    int error = a ? a->error : 0;
    if (a) *ipv4 = a->ipv4;
    pthread_mutex_unlock(&mu);
    if (a) { if (error) { errno = error; return -1; } return 0; }

    struct addrinfo hints = { .ai_family = AF_INET, .ai_socktype = SOCK_STREAM }, *res = NULL;
    int rc = gai(host, NULL, &hints, &res);
    error = rc ? gai_errno(rc) : 0;
    if (!rc) {
        if (res && res->ai_family == AF_INET && res->ai_addr)
            *ipv4 = ((const struct sockaddr_in *)res->ai_addr)->sin_addr.s_addr;
        else error = ENOENT;
        freeai(res);
    }
    if (error && error != ENOENT) { errno = error; return -1; }

    pthread_mutex_lock(&mu);
    a = NULL;
    for (int i = 0; i < DMESH_RESOLVE_ENTRIES && !a; ++i)
        if (!strcmp(answers[i].target, target)) a = &answers[i];
    if (!a) { a = &answers[next_answer]; next_answer = (next_answer + 1) % DMESH_RESOLVE_ENTRIES; }
    snprintf(a->target, sizeof(a->target), "%s", target);
    a->error = error; a->ipv4 = error ? 0 : *ipv4; a->port = *port;
    a->expires_ns = now_ns() + DMESH_RESOLVE_TTL_NS;
    pthread_mutex_unlock(&mu);
    if (error) { errno = error; return -1; }
    return 0;
}
/* Caller holds mu. */
static int intern(uint32_t ipv4, uint16_t port)
{
    for (int i = 0; i < n_ids; ++i)
        if (ids[i].ipv4 == ipv4 && ids[i].port == port) return i;
    if (n_ids == DMESH_RESOLVE_IDS) { errno = ENOSPC; return -1; }
    ids[n_ids] = (struct address){ ipv4, port };
    return n_ids++;
}
static int intern_locked(uint32_t ipv4, uint16_t port)
{
    pthread_mutex_lock(&mu);
    int id = intern(ipv4, port);
    pthread_mutex_unlock(&mu);
    return id;
}
/* $DPUMESH_TARGETS, read once: targets separated by commas or whitespace.
 * Caller holds mu. */
static void load_targets(void)
{
    if (n_targets >= 0) return;
    n_targets = 0;
    const char *list = getenv("DPUMESH_TARGETS");
    while (list && *(list += strspn(list, ", \t\n"))) {
        size_t len = strcspn(list, ", \t\n");
        char host[DMESH_TARGET_HOST_MAX + 1];
        if (len >= TARGET_MAX || n_targets == DMESH_TARGETS_MAX) { targets_error = EINVAL; return; }
        memcpy(targets[n_targets], list, len); targets[n_targets][len] = 0;
        if (parse_target(targets[n_targets], host, &target_ports[n_targets]) != 0) { targets_error = EINVAL; return; }
        ++n_targets;
        list += len;
    }
}

int dmesh_target_resolve(const char *target)
{
    uint32_t ipv4; uint16_t port;
    if (lookup(target, &ipv4, &port) != 0) return -1;
    return intern_locked(ipv4, port);
}
int dmesh_target_member(uint32_t ipv4, uint16_t port)
{
    pthread_mutex_lock(&mu);
    load_targets();
    int error = targets_error, n = n_targets;
    pthread_mutex_unlock(&mu);
    if (error) { errno = error; return -1; }
    int unknown = 0;
    for (int i = 0; i < n; ++i) {
        uint32_t a; uint16_t p;
        if (target_ports[i] != port) continue;
        if (lookup(targets[i], &a, &p) != 0) { unknown |= errno != ENOENT; continue; }
        if (a == ipv4) return intern_locked(a, p);
    }
    errno = unknown ? EAGAIN : ENOENT;
    return -1;
}
int dmesh_target_addr(int id, uint32_t *ipv4, uint16_t *port)
{
    pthread_mutex_lock(&mu);
    int known = id >= 0 && id < n_ids;
    if (known) { *ipv4 = ids[id].ipv4; *port = ids[id].port; }
    pthread_mutex_unlock(&mu);
    if (!known) { errno = ENOENT; return -1; }
    return 0;
}
void dmesh_target_invalidate(uint32_t ipv4, uint16_t port)
{
    pthread_mutex_lock(&mu);
    for (int i = 0; i < DMESH_RESOLVE_ENTRIES; ++i)
        if (!answers[i].error && answers[i].ipv4 == ipv4 && answers[i].port == port)
            answers[i].expires_ns = 0;
    pthread_mutex_unlock(&mu);
}

#ifdef DMESH_RESOLVE_TEST
void dmesh_resolve_test_hooks(int (*g)(const char *, const char *, const struct addrinfo *, struct addrinfo **),
                              void (*f)(struct addrinfo *))
{
    gai = g ? g : getaddrinfo;
    freeai = f ? f : freeaddrinfo;
}
void dmesh_resolve_test_advance(uint64_t ns) { clock_offset += ns; }
void dmesh_resolve_test_reset(void)
{
    pthread_mutex_lock(&mu);
    memset(answers, 0, sizeof(answers)); next_answer = 0;
    n_ids = 0; n_targets = -1; targets_error = 0;
    pthread_mutex_unlock(&mu);
}
#endif
