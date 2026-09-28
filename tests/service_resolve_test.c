#define DMESH_RESOLVE_TEST 1
#include "src/core/service_resolve.h"
#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* A DNS stand-in: one name, its answer (an address or a getaddrinfo error),
 * and a count of the queries that reached it. */
static const char *dns_name;
static const char *dns_addr;
static int dns_rc, dns_calls;
static int fake_gai(const char *host, const char *serv, const struct addrinfo *hints, struct addrinfo **res)
{
    assert(!serv && hints->ai_family == AF_INET);
    ++dns_calls;
    if (!dns_name || strcmp(host, dns_name)) return EAI_NONAME;
    if (dns_rc) return dns_rc;
    struct { struct addrinfo ai; struct sockaddr_in sin; } *r = calloc(1, sizeof(*r));
    assert(r);
    r->sin.sin_family = AF_INET;
    r->sin.sin_addr.s_addr = inet_addr(dns_addr);
    r->ai.ai_family = AF_INET;
    r->ai.ai_addr = (struct sockaddr *)&r->sin;
    r->ai.ai_addrlen = sizeof(r->sin);
    *res = &r->ai;
    return 0;
}
static void fake_free(struct addrinfo *ai) { free(ai); }
static void use_dns(const char *name, const char *addr, int rc)
{
    dns_name = name; dns_addr = addr; dns_rc = rc;
}
static void expect_error(int rc, int error) { assert(rc == -1 && errno == error); }

int main(void)
{
    uint32_t ip; uint16_t port;

    /* Grammar: "<host>:<port>" with a DNS name or IPv4 host. */
    char long_host[DMESH_TARGET_HOST_MAX + 8];
    memset(long_host, 'a', DMESH_TARGET_HOST_MAX + 1);
    strcpy(long_host + DMESH_TARGET_HOST_MAX + 1, ":80");
    const char *bad[] = { "", "echo", "echo:", ":80", "echo:0", "echo:65536", "echo:+80",
                          "echo: 80", "echo:80x", "[::1]:80", "::1:80", "a b:80", "a,b:80", long_host };
    for (size_t i = 0; i < sizeof(bad) / sizeof(*bad); ++i) expect_error(dmesh_target_resolve(bad[i]), EINVAL);
    expect_error(dmesh_target_resolve(NULL), EINVAL);

    /* IPv4 literals need no DNS; one address, one id. */
    int a = dmesh_target_resolve("10.0.0.1:80");
    assert(a >= 0 && dmesh_target_resolve("10.0.0.1:80") == a);
    int b = dmesh_target_resolve("10.0.0.1:81");
    assert(b >= 0 && b != a);
    assert(!dmesh_target_addr(a, &ip, &port) && ip == inet_addr("10.0.0.1") && port == 80);
    expect_error(dmesh_target_addr(-1, &ip, &port), ENOENT);
    expect_error(dmesh_target_addr(b + 1, &ip, &port), ENOENT);
    int local = dmesh_target_resolve("localhost:9095");
    assert(local >= 0 && !dmesh_target_addr(local, &ip, &port) && ip == inet_addr("127.0.0.1"));

    /* Names: cached for one TTL, then asked again; a moved Service gets a new id
     * while the old id keeps its address. */
    dmesh_resolve_test_reset();
    dmesh_resolve_test_hooks(fake_gai, fake_free);
    use_dns("echo.shop", "10.96.0.15", 0); dns_calls = 0;
    int echo = dmesh_target_resolve("echo.shop:9095");
    assert(echo >= 0 && dns_calls == 1);
    assert(dmesh_target_resolve("echo.shop:9095") == echo && dns_calls == 1);
    dmesh_resolve_test_advance(DMESH_RESOLVE_TTL_NS);
    use_dns("echo.shop", "10.96.0.16", 0);
    int moved = dmesh_target_resolve("echo.shop:9095");
    assert(moved >= 0 && moved != echo && dns_calls == 2);
    assert(!dmesh_target_addr(echo, &ip, &port) && ip == inet_addr("10.96.0.15") && port == 9095);
    assert(!dmesh_target_addr(moved, &ip, &port) && ip == inet_addr("10.96.0.16"));

    /* Invalidation drops the cached answer for that address. */
    dmesh_target_invalidate(inet_addr("10.96.0.16"), 9095);
    assert(dmesh_target_resolve("echo.shop:9095") == moved && dns_calls == 3);

    /* A missing name is an answer and is cached; no answer is not. */
    dns_calls = 0;
    expect_error(dmesh_target_resolve("gone:80"), ENOENT);
    expect_error(dmesh_target_resolve("gone:80"), ENOENT);
    assert(dns_calls == 1);
    use_dns("flaky", NULL, EAI_AGAIN);
    expect_error(dmesh_target_resolve("flaky:80"), EAGAIN);
    expect_error(dmesh_target_resolve("flaky:80"), EAGAIN);
    assert(dns_calls == 3);
    use_dns("broken", NULL, EAI_FAIL);
    expect_error(dmesh_target_resolve("broken:80"), EAGAIN);

    /* Membership: only listed targets are meshed; an unanswered listed target
     * on the dialed port makes the answer unknown, never "not meshed". */
    dmesh_resolve_test_reset();
    assert(!setenv("DPUMESH_TARGETS", " echo.shop:9095,\t10.0.0.9:80 ", 1));
    use_dns("echo.shop", "10.96.0.15", 0);
    int m = dmesh_target_member(inet_addr("10.96.0.15"), 9095);
    assert(m >= 0 && m == dmesh_target_resolve("echo.shop:9095"));
    assert(dmesh_target_member(inet_addr("10.0.0.9"), 80) >= 0);
    expect_error(dmesh_target_member(inet_addr("10.96.0.15"), 9096), ENOENT);
    expect_error(dmesh_target_member(inet_addr("10.1.1.1"), 9095), ENOENT);
    dmesh_resolve_test_advance(DMESH_RESOLVE_TTL_NS);
    use_dns("echo.shop", NULL, EAI_AGAIN);
    expect_error(dmesh_target_member(inet_addr("10.1.1.1"), 9095), EAGAIN);
    assert(dmesh_target_member(inet_addr("10.0.0.9"), 80) >= 0);
    expect_error(dmesh_target_member(inet_addr("10.1.1.1"), 7777), ENOENT);
    use_dns(NULL, NULL, 0);
    expect_error(dmesh_target_member(inet_addr("10.1.1.1"), 9095), ENOENT);

    dmesh_resolve_test_reset();
    assert(!setenv("DPUMESH_TARGETS", "echo.shop:9095,echo", 1));
    expect_error(dmesh_target_member(inet_addr("10.96.0.15"), 9095), EINVAL);
    dmesh_resolve_test_reset();
    assert(!unsetenv("DPUMESH_TARGETS"));
    expect_error(dmesh_target_member(inet_addr("10.96.0.15"), 9095), ENOENT);

    /* The id space is bounded, and ids are never reused. */
    dmesh_resolve_test_reset();
    dmesh_resolve_test_hooks(NULL, NULL);
    char target[32];
    for (int i = 0; i < DMESH_RESOLVE_IDS; ++i) {
        snprintf(target, sizeof(target), "10.%d.%d.1:80", i / 256, i % 256);
        assert(dmesh_target_resolve(target) == i);
    }
    expect_error(dmesh_target_resolve("10.200.0.1:80"), ENOSPC);
    assert(dmesh_target_resolve("10.0.0.1:80") == 0);

    puts("service resolution: grammar, DNS cache and ids, fail-closed membership: PASS");
    return 0;
}
