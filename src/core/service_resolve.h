#ifndef DMESH_SERVICE_RESOLVE_H
#define DMESH_SERVICE_RESOLVE_H
#include <stdint.h>

/* Service targets are "<host>:<port>": host is a DNS name ("name" in the
 * calling Pod's namespace, "name.namespace", a full name) or an IPv4 literal;
 * port is the Service port. The cluster DNS answers every name and the
 * process stores no address of its own. Answers are cached per target for
 * DMESH_RESOLVE_TTL_NS; each resolved IPv4:port is interned to a process-local
 * id, the same id for the same address, and ids are never reused. */
#define DMESH_RESOLVE_TTL_NS  5000000000ull
#define DMESH_RESOLVE_IDS     1024
#define DMESH_RESOLVE_ENTRIES 256
#define DMESH_TARGET_HOST_MAX 253
#define DMESH_TARGETS_MAX     64

/* The id of target's address, or -1 with EINVAL (malformed target), ENOENT
 * (no such name, or no IPv4 address), EAGAIN (DNS temporarily unavailable),
 * ENOSPC (id space exhausted) or ENOMEM. */
int dmesh_target_resolve(const char *target);
/* Membership for the preload facade: the id when ipv4:port is the current
 * address of a target listed in $DPUMESH_TARGETS; otherwise -1 with ENOENT
 * (not meshed), EAGAIN (a listed target on this port has no answer, so
 * membership is unknown) or EINVAL (the list is malformed). */
int dmesh_target_member(uint32_t ipv4_net, uint16_t port_host);
/* The address an id was interned for. 0, or -1 with ENOENT. */
int dmesh_target_addr(int id, uint32_t *ipv4_net, uint16_t *port_host);
/* Drop cached answers that resolved to ipv4:port; the next lookup asks DNS. */
void dmesh_target_invalidate(uint32_t ipv4_net, uint16_t port_host);

#ifdef DMESH_RESOLVE_TEST
struct addrinfo;
void dmesh_resolve_test_hooks(int (*gai)(const char *, const char *,
                                         const struct addrinfo *,
                                         struct addrinfo **),
                              void (*freeai)(struct addrinfo *));
void dmesh_resolve_test_advance(uint64_t ns);
void dmesh_resolve_test_reset(void);
#endif
#endif
