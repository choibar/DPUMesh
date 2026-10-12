#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "peer_identity.h"

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <glob.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>

#ifndef SO_PEERPIDFD
#define SO_PEERPIDFD 77              /* Linux 6.5 */
#endif

#define PEER_FILE_MAX (256 * 1024)   /* fib_trie of a Pod is a few KiB */

int peer_verify_parse(const char *name)
{
	if (strcmp(name, "none") == 0) return PEER_VERIFY_NONE;
	if (strcmp(name, "addr") == 0) return PEER_VERIFY_ADDR;
	if (strcmp(name, "pod") == 0) return PEER_VERIFY_POD;
	return -1;
}

const char *peer_verify_name(enum peer_verify verify)
{
	return verify == PEER_VERIFY_POD ? "pod" : verify == PEER_VERIFY_ADDR ? "addr" : "none";
}

int peer_owns_addr(const struct peer_identity *id, uint32_t addr)
{
	for (int i = 0; i < id->naddrs; ++i)
		if (id->addrs[i] == addr) return 1;
	return 0;
}

/* Reads a small file whole; NULL with errno. */
static char *read_text(const char *path)
{
	int fd = open(path, O_RDONLY | O_CLOEXEC);
	if (fd < 0) return NULL;
	char *buf = malloc(PEER_FILE_MAX + 1);
	size_t len = 0;
	ssize_t n = 0;
	while (buf != NULL && len < PEER_FILE_MAX && (n = read(fd, buf + len, PEER_FILE_MAX - len)) > 0)
		len += (size_t)n;
	int saved = buf == NULL ? ENOMEM : n < 0 ? errno : 0;
	close(fd);
	if (saved) { free(buf); errno = saved; return NULL; }
	buf[len] = '\0';
	return buf;
}

/* fib_trie lists every prefix of every table; a local address is a leaf
 * ("|-- 10.244.0.7") followed by "/32 host LOCAL". Loopback is not an
 * address a peer can be reached at, so it is left out. */
int peer_parse_fib_trie(const char *text, uint32_t *addrs, int max, int *count)
{
	struct in_addr leaf = {0};
	int have_leaf = 0;

	*count = 0;
	while (*text) {
		size_t len = strcspn(text, "\n");
		char line[128];
		if (len < sizeof(line)) {
			memcpy(line, text, len);
			line[len] = '\0';
			const char *p = strstr(line, "|-- ");
			if (p != NULL) {
				have_leaf = inet_pton(AF_INET, p + 4, &leaf) == 1;
			} else if (have_leaf && strstr(line, "/32 host LOCAL") != NULL &&
				   (ntohl(leaf.s_addr) >> 24) != 127) {
				int dup = 0;
				for (int i = 0; i < *count; ++i) dup |= addrs[i] == leaf.s_addr;
				if (!dup && *count == max) { errno = E2BIG; return -1; }
				if (!dup) addrs[(*count)++] = leaf.s_addr;
			}
		}
		text += len + (text[len] == '\n');
	}
	return 0;
}

/* A Pod UID as the kubelet names cgroups: "pod" and 36 characters, '-' or
 * '_' at 8, 13, 18 and 23 (the systemd driver swaps the dashes). */
int peer_parse_cgroup(const char *text, char *pod_uid, size_t len)
{
	if (len < 37) { errno = EINVAL; return -1; }
	for (const char *p = strstr(text, "pod"); p != NULL; p = strstr(p + 3, "pod")) {
		const char *u = p + 3;
		int ok = 1;
		for (int i = 0; i < 36 && ok; ++i) {
			char c = u[i];
			ok = (i == 8 || i == 13 || i == 18 || i == 23) ? (c == '-' || c == '_') : isxdigit((unsigned char)c);
		}
		if (!ok || isxdigit((unsigned char)u[36])) continue;
		for (int i = 0; i < 36; ++i) pod_uid[i] = u[i] == '_' ? '-' : (char)tolower((unsigned char)u[i]);
		pod_uid[36] = '\0';
		return 0;
	}
	errno = ENOENT;
	return -1;
}

static int b64url_value(char c)
{
	if (c >= 'A' && c <= 'Z') return c - 'A';
	if (c >= 'a' && c <= 'z') return c - 'a' + 26;
	if (c >= '0' && c <= '9') return c - '0' + 52;
	if (c == '-' || c == '+') return 62;
	if (c == '_' || c == '/') return 63;
	return -1;
}

/* Decodes base64url without padding; returns the length or -1. */
static long b64url_decode(const char *in, size_t len, char *out, size_t cap)
{
	uint32_t acc = 0;
	int bits = 0;
	size_t n = 0;
	for (size_t i = 0; i < len; ++i) {
		if (in[i] == '=') break;
		int v = b64url_value(in[i]);
		if (v < 0) return -1;
		acc = (acc << 6) | (uint32_t)v;
		bits += 6;
		if (bits >= 8) {
			bits -= 8;
			if (n + 1 >= cap) return -1;
			out[n++] = (char)((acc >> bits) & 0xff);
		}
	}
	out[n] = '\0';
	return (long)n;
}

/* The string value of "key" at or after `from`: "key" : "value". */
static int json_string(const char *from, const char *key, char *out, size_t cap)
{
	char quoted[64];
	snprintf(quoted, sizeof(quoted), "\"%s\"", key);
	const char *p = from ? strstr(from, quoted) : NULL;
	if (p == NULL) return -1;
	p += strlen(quoted);
	while (*p == ' ') ++p;
	if (*p++ != ':') return -1;
	while (*p == ' ') ++p;
	if (*p++ != '"') return -1;
	size_t n = 0;
	while (*p && *p != '"' && *p != '\\') {
		if (n + 1 >= cap) return -1;
		out[n++] = *p++;
	}
	if (*p != '"') return -1;
	out[n] = '\0';
	return 0;
}

/* An object value "key" : { ... } at or after `from`: points at its '{'. */
static const char *json_object(const char *from, const char *key)
{
	char quoted[64];
	snprintf(quoted, sizeof(quoted), "\"%s\"", key);
	const char *p = from ? strstr(from, quoted) : NULL;
	if (p == NULL) return NULL;
	p += strlen(quoted);
	while (*p == ' ') ++p;
	if (*p++ != ':') return NULL;
	while (*p == ' ') ++p;
	return *p == '{' ? p : NULL;
}

static int dns_name_ok(const char *s)
{
	if (*s == '\0') return 0;
	for (; *s; ++s)
		if (!(islower((unsigned char)*s) || isdigit((unsigned char)*s) || *s == '-' || *s == '.')) return 0;
	return 1;
}

/* The kubelet's bound token: {"kubernetes.io": {"namespace": ns, "pod": {"uid"},
 * "serviceaccount": {"name"}}}. Its signature is not checked: the broker reads
 * it from the kubelet's own directory, which the Pod cannot write. */
int peer_parse_token(const char *jwt, const char *pod_uid, const char *trust_domain, char *workload, size_t len)
{
	const char *dot1 = strchr(jwt, '.'), *dot2 = dot1 ? strchr(dot1 + 1, '.') : NULL;
	char claims[8192], ns[64], sa[64], uid[PEER_UID_LEN];
	if (dot2 == NULL || b64url_decode(dot1 + 1, (size_t)(dot2 - dot1 - 1), claims, sizeof(claims)) < 0) {
		errno = EBADMSG;
		return -1;
	}
	const char *k8s = json_object(claims, "kubernetes.io");
	const char *pod = json_object(k8s, "pod"), *account = json_object(k8s, "serviceaccount");
	if (k8s == NULL || json_string(k8s, "namespace", ns, sizeof(ns)) != 0 || account == NULL ||
	    json_string(account, "name", sa, sizeof(sa)) != 0 || !dns_name_ok(ns) || !dns_name_ok(sa)) {
		errno = EBADMSG;
		return -1;
	}
	/* Pod-bound tokens name their Pod: it must be the peer's. */
	if (pod_uid && *pod_uid && (pod == NULL || json_string(pod, "uid", uid, sizeof(uid)) != 0 || strcmp(uid, pod_uid))) {
		errno = EPERM;
		return -1;
	}
	int n = snprintf(workload, len, "%s.%s.serviceaccount.identity.linkerd.%s", sa, ns, trust_domain);
	if (n < 0 || (size_t)n >= len) { errno = ENAMETOOLONG; return -1; }
	return 0;
}

/* The workload of a Pod from the token the kubelet projected for it. */
static void pod_workload(const struct peer_policy *policy, struct peer_identity *id)
{
	char pattern[512];
	glob_t found;
	snprintf(pattern, sizeof(pattern), "%s/pods/%s/volumes/kubernetes.io~projected/*/token",
		 policy->kubelet_root, id->pod_uid);
	if (glob(pattern, 0, NULL, &found) != 0) {
		fprintf(stderr, "dpumesh_broker: pod %s mounts no service account token: unauthenticated\n", id->pod_uid);
		return;
	}
	for (size_t i = 0; i < found.gl_pathc && !id->workload[0]; ++i) {
		char *jwt = read_text(found.gl_pathv[i]);
		if (jwt == NULL) continue;
		jwt[strcspn(jwt, "\r\n")] = '\0';
		if (peer_parse_token(jwt, id->pod_uid, policy->trust_domain, id->workload, sizeof(id->workload)) != 0) {
			fprintf(stderr, "dpumesh_broker: pod %s token %s: %s\n", id->pod_uid, found.gl_pathv[i], strerror(errno));
			id->workload[0] = '\0';
		}
		free(jwt);
	}
	globfree(&found);
}

int peer_identity_read(int sock, const struct peer_policy *policy, struct peer_identity *id)
{
	struct ucred cred = {0};
	socklen_t len = sizeof(cred);
	int pidfd = -1;
	char path[256];

	memset(id, 0, sizeof(*id));
	if (getsockopt(sock, SOL_SOCKET, SO_PEERCRED, &cred, &len) != 0) return -1;
	id->pid = cred.pid;
	id->uid = cred.uid;
	if (policy->verify == PEER_VERIFY_NONE) return 0;

	len = sizeof(pidfd);
	if (getsockopt(sock, SOL_SOCKET, SO_PEERPIDFD, &pidfd, &len) != 0) pidfd = -1;

	snprintf(path, sizeof(path), "%s/%d/net/fib_trie", policy->proc_root, (int)id->pid);
	char *text = read_text(path);
	int rc = text != NULL ? peer_parse_fib_trie(text, id->addrs, PEER_MAX_ADDRS, &id->naddrs) : -1;
	free(text);
	if (rc == 0) {
		snprintf(path, sizeof(path), "%s/%d/cgroup", policy->proc_root, (int)id->pid);
		text = read_text(path);
		if (text == NULL) rc = -1;
		else if (peer_parse_cgroup(text, id->pod_uid, sizeof(id->pod_uid)) != 0) id->pod_uid[0] = '\0';
		free(text);
	}
	/* What was read belongs to the peer only if the peer is still the process
	 * the pid named: a pidfd outlives its process and refuses signals then. */
	if (rc == 0 && pidfd >= 0 && syscall(SYS_pidfd_send_signal, pidfd, 0, NULL, 0) != 0) {
		errno = ESRCH;
		rc = -1;
	}
	int saved = errno;
	if (pidfd >= 0) close(pidfd);
	if (rc != 0) { errno = saved; return -1; }

	if (policy->verify == PEER_VERIFY_POD) {
		if (!id->pod_uid[0]) { errno = EPERM; return -1; }
		pod_workload(policy, id);
	}
	return 0;
}
