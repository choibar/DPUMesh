#ifndef PEER_IDENTITY_H
#define PEER_IDENTITY_H

/*
 * What the broker can prove about the process on the other end of an accepted
 * socket, from the kernel and the kubelet rather than from the application:
 *
 *   addrs     the IPv4 addresses local to the peer's network namespace
 *             (/proc/<pid>/net/fib_trie), the only ones it may LISTEN on or
 *             send from
 *   pod_uid   the Pod of its cgroup (/proc/<pid>/cgroup), "" outside a Pod
 *   workload  the Pod's mesh identity, <sa>.<ns>.serviceaccount.identity.linkerd.<domain>,
 *             from the service account token the kubelet projected into the
 *             Pod (read on the host side, so the Pod cannot alter it); ""
 *             when the Pod mounts no token
 *
 * /proc is read under the peer's pidfd (SO_PEERPIDFD) and the pidfd is
 * checked afterwards, so a peer that exited cannot hand its pid's identity to
 * a newcomer. No DOCA types appear here.
 */

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#define PEER_MAX_ADDRS 16
#define PEER_UID_LEN 40              /* a Pod UID: 36 characters */
#define PEER_WORKLOAD_LEN 64         /* the Comch workload field */

enum peer_verify {
	PEER_VERIFY_NONE = 0,        /* trust the application's claims (bare metal, benchmarks) */
	PEER_VERIFY_ADDR,            /* LISTEN and source addresses must be the peer's own */
	PEER_VERIFY_POD,             /* ADDR, the peer must be in a Pod, and its workload is stamped */
};

struct peer_identity {
	pid_t pid;
	uid_t uid;
	uint32_t addrs[PEER_MAX_ADDRS];  /* network byte order */
	int naddrs;
	char pod_uid[PEER_UID_LEN];
	char workload[PEER_WORKLOAD_LEN];
};

struct peer_policy {
	enum peer_verify verify;
	const char *proc_root;           /* "/proc" */
	const char *kubelet_root;        /* "/var/lib/kubelet" */
	const char *trust_domain;        /* "cluster.local" */
};

/* "none" | "addr" | "pod"; -1 when unknown. */
int peer_verify_parse(const char *name);
const char *peer_verify_name(enum peer_verify verify);

/* Fills `id` for the socket's peer. Returns 0, or -1 with errno when the
 * policy requires something that cannot be established (EPERM: not in a Pod
 * under PEER_VERIFY_POD; ESRCH: the peer exited while it was read). */
int peer_identity_read(int sock, const struct peer_policy *policy, struct peer_identity *id);

/* The address (network byte order) is local to the peer's namespace. */
int peer_owns_addr(const struct peer_identity *id, uint32_t addr);

/* Parsers, exposed for tests. Each returns 0 or -1 with errno. */
int peer_parse_fib_trie(const char *text, uint32_t *addrs, int max, int *count);
int peer_parse_cgroup(const char *text, char *pod_uid, size_t len);
int peer_parse_token(const char *jwt, const char *pod_uid, const char *trust_domain, char *workload, size_t len);

#endif /* PEER_IDENTITY_H */
