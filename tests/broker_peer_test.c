/* peer_identity: what the broker proves about a client, from /proc and the
 * kubelet's directory (here fake trees with the real formats). No DOCA. */
#include <assert.h>
#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>
#include "peer_identity.h"

/* A Pod's /proc/<pid>/net/fib_trie: both tables list the local addresses. */
static const char fib_trie[] =
    "Main:\n"
    "  +-- 0.0.0.0/0 3 0 5\n"
    "     |-- 0.0.0.0\n"
    "        /0 universe UNICAST\n"
    "     +-- 10.244.0.0/24 2 0 2\n"
    "        +-- 10.244.0.0/28 2 0 2\n"
    "           |-- 10.244.0.0\n"
    "              /24 link UNICAST\n"
    "           |-- 10.244.0.7\n"
    "              /32 host LOCAL\n"
    "        |-- 10.244.0.255\n"
    "           /32 link BROADCAST\n"
    "     +-- 127.0.0.0/8 2 0 2\n"
    "        |-- 127.0.0.1\n"
    "           /32 host LOCAL\n"
    "Local:\n"
    "  +-- 0.0.0.0/0 3 0 5\n"
    "     |-- 10.244.0.7\n"
    "        /32 host LOCAL\n";

static const char cgroup_systemd[] =
    "0::/kubepods.slice/kubepods-besteffort.slice/"
    "kubepods-besteffort-pod6b1f0c2e_8a7d_4c3b_9e21_0d5f4a3b2c1d.slice/"
    "cri-containerd-0123456789abcdef.scope\n";
static const char cgroup_cgroupfs[] =
    "0::/../../kubepods/burstable/pod6b1f0c2e-8a7d-4c3b-9e21-0d5f4a3b2c1d/0123456789abcdef\n";
static const char cgroup_host[] = "0::/user.slice/user-1002.slice/session-3.scope\n";

#define POD_UID "6b1f0c2e-8a7d-4c3b-9e21-0d5f4a3b2c1d"
/* A kubelet-bound token of shop/web in Pod POD_UID (signature not checked). */
static const char token[] =
    "eyJhbGciOiJSUzI1NiIsImtpZCI6ImsifQ."
    "eyJhdWQiOlsiaHR0cHM6Ly9rdWJlcm5ldGVzLmRlZmF1bHQuc3ZjLmNsdXN0ZXIubG9jYWwiXSwiZXhwIjoxNzkxMDAwMDAwLCJpYXQiOjE3"
    "NjAwMDAwMDAsImlzcyI6Imh0dHBzOi8va3ViZXJuZXRlcy5kZWZhdWx0LnN2Yy5jbHVzdGVyLmxvY2FsIiwianRpIjoiYTEiLCJrdWJlcm5l"
    "dGVzLmlvIjp7Im5hbWVzcGFjZSI6InNob3AiLCJub2RlIjp7Im5hbWUiOiJqZXQyIiwidWlkIjoiMGYxZTJkM2MtNGI1YS02OTc4LTg3OTYt"
    "YTViNGMzZDJlMWYwIn0sInBvZCI6eyJuYW1lIjoid2ViLTdkNGI5IiwidWlkIjoiNmIxZjBjMmUtOGE3ZC00YzNiLTllMjEtMGQ1ZjRhM2Iy"
    "YzFkIn0sInNlcnZpY2VhY2NvdW50Ijp7Im5hbWUiOiJ3ZWIiLCJ1aWQiOiIxMTExMTExMS0yMjIyLTMzMzMtNDQ0NC01NTU1NTU1NTU1NTUi"
    "fSwid2FybmFmdGVyIjoxNzYwMDAzNjA3fSwibmJmIjoxNzYwMDAwMDAwLCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6c2hvcDp3ZWIi"
    "fQ.c2ln";

static void write_file(const char *path, const char *text)
{
    char dir[512];
    snprintf(dir, sizeof(dir), "%s", path);
    for (char *p = dir + 1; *p; ++p)
        if (*p == '/') { *p = '\0'; (void)mkdir(dir, 0700); *p = '/'; }
    FILE *f = fopen(path, "w");
    assert(f && fputs(text, f) >= 0 && fclose(f) == 0);
}

static void test_parsers(void)
{
    uint32_t addrs[4];
    int n = -1;
    assert(peer_parse_fib_trie(fib_trie, addrs, 4, &n) == 0 && n == 1);
    assert(addrs[0] == inet_addr("10.244.0.7"));     /* once, loopback left out */
    assert(peer_parse_fib_trie(fib_trie, addrs, 0, &n) == -1 && errno == E2BIG);

    char uid[PEER_UID_LEN];
    assert(peer_parse_cgroup(cgroup_systemd, uid, sizeof(uid)) == 0 && strcmp(uid, POD_UID) == 0);
    assert(peer_parse_cgroup(cgroup_cgroupfs, uid, sizeof(uid)) == 0 && strcmp(uid, POD_UID) == 0);
    assert(peer_parse_cgroup(cgroup_host, uid, sizeof(uid)) == -1 && errno == ENOENT);

    char workload[PEER_WORKLOAD_LEN];
    assert(peer_parse_token(token, POD_UID, "cluster.local", workload, sizeof(workload)) == 0);
    assert(strcmp(workload, "web.shop.serviceaccount.identity.linkerd.cluster.local") == 0);
    assert(peer_parse_token(token, "00000000-0000-0000-0000-000000000000", "cluster.local", workload,
                            sizeof(workload)) == -1 && errno == EPERM);       /* another Pod's token */
    assert(peer_parse_token(token, POD_UID, "a-trust-domain-far-too-long-for-the-comch-field.example", workload,
                            sizeof(workload)) == -1 && errno == ENAMETOOLONG);
    assert(peer_parse_token("no-dots", POD_UID, "cluster.local", workload, sizeof(workload)) == -1 &&
           errno == EBADMSG);
    assert(peer_verify_parse("pod") == PEER_VERIFY_POD && peer_verify_parse("yes") == -1);
}

/* peer_identity_read over a socketpair: the peer is this process, its /proc
 * and kubelet directory are fakes under a temporary root. */
static void test_read(void)
{
    char root[] = "/tmp/broker-peer-XXXXXX", path[1024], proc[128], kubelet[128];
    assert(mkdtemp(root) != NULL);
    snprintf(proc, sizeof(proc), "%s/proc", root);
    snprintf(kubelet, sizeof(kubelet), "%s/kubelet", root);
    snprintf(path, sizeof(path), "%s/%d/net/fib_trie", proc, (int)getpid());
    write_file(path, fib_trie);
    snprintf(path, sizeof(path), "%s/%d/cgroup", proc, (int)getpid());
    write_file(path, cgroup_systemd);

    int sv[2];
    assert(socketpair(AF_UNIX, SOCK_SEQPACKET, 0, sv) == 0);
    struct peer_policy policy = { PEER_VERIFY_NONE, proc, kubelet, "cluster.local" };
    struct peer_identity id;
    assert(peer_identity_read(sv[0], &policy, &id) == 0 && id.pid == getpid() && id.naddrs == 0);

    policy.verify = PEER_VERIFY_ADDR;
    assert(peer_identity_read(sv[0], &policy, &id) == 0);
    assert(id.naddrs == 1 && peer_owns_addr(&id, inet_addr("10.244.0.7")) && !peer_owns_addr(&id, inet_addr("10.244.0.8")));
    assert(strcmp(id.pod_uid, POD_UID) == 0 && id.workload[0] == '\0');

    policy.verify = PEER_VERIFY_POD;          /* no token mounted: unauthenticated */
    assert(peer_identity_read(sv[0], &policy, &id) == 0 && id.workload[0] == '\0');
    snprintf(path, sizeof(path), "%s/pods/%s/volumes/kubernetes.io~projected/kube-api-access-x/token", kubelet, POD_UID);
    write_file(path, token);
    assert(peer_identity_read(sv[0], &policy, &id) == 0);
    assert(strcmp(id.workload, "web.shop.serviceaccount.identity.linkerd.cluster.local") == 0);

    snprintf(path, sizeof(path), "%s/%d/cgroup", proc, (int)getpid());
    write_file(path, cgroup_host);            /* not in a Pod */
    errno = 0;
    assert(peer_identity_read(sv[0], &policy, &id) == -1 && errno == EPERM);
    policy.verify = PEER_VERIFY_ADDR;         /* addr does not need a Pod */
    assert(peer_identity_read(sv[0], &policy, &id) == 0 && id.pod_uid[0] == '\0');

    close(sv[0]); close(sv[1]);
    snprintf(path, sizeof(path), "rm -rf %s", root);
    assert(system(path) == 0);
}

int main(void)
{
    test_parsers();
    test_read();
    puts("broker_peer_test: netns addresses, Pod UID, token workload, verification modes: PASS");
    return 0;
}
