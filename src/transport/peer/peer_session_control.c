#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "peer_session_control.h"
#include "peer_local_control.h"

#include <errno.h>
#include <fcntl.h>
#include <openssl/crypto.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

struct peer_session_control {
    struct peer_authority *authority;
    struct dmesh_local_control *local;
    char *path, *keys;
    uint64_t next_poll_ms, verified_ms;
};

static uint64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static unsigned local_request(void *ctx, const struct dmesh_local_request *request)
{
    struct peer_session_control *c = ctx;
    if (request->operation == DMESH_LOCAL_PING)
        return DMESH_LOCAL_OK;
    if (request->operation != DMESH_LOCAL_UNREGISTER && request->operation != DMESH_LOCAL_STATUS)
        return DMESH_LOCAL_INVALID;

    /* Idempotent, durable revocation. STATUS must not report OK until the
     * dispatcher has received both flow and RDMA retirement acknowledgments. */
    if (request->operation == DMESH_LOCAL_UNREGISTER)
        peer_authority_retire_nonce(c->authority, request->connection_id);
    return peer_authority_nonce_present(c->authority, request->connection_id) ?
        DMESH_LOCAL_PENDING : DMESH_LOCAL_OK;
}

static void local_retired(void *ctx)
{
    struct peer_session_control *c = ctx;
    peer_authority_retire_all(c->authority);
}

static int available(void *ctx)
{
    struct peer_session_control *c = ctx;
    struct peer_authority_view *view = peer_authority_acquire(c->authority);
    int ready = view != NULL;
    peer_authority_release(view);
    unsigned cursor = 0;
    struct peer_local_registration r;
    if (peer_authority_retiring(c->authority, &cursor, &r))
        ready = 0;
    return ready;
}

static int refresh(struct peer_session_control *c)
{
    int fd = open(c->path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0)
        return errno;

    struct stat st;
    if (fstat(fd, &st) || !S_ISREG(st.st_mode) || st.st_size <= 0 ||
        st.st_size > DMESH_TOPOLOGY_MAX_BYTES) {
        close(fd);
        return EINVAL;
    }

    size_t size = (size_t)st.st_size, offset = 0;
    char *document = malloc(size);
    if (!document) {
        close(fd);
        return ENOMEM;
    }
    while (offset < size) {
        ssize_t got = read(fd, document + offset, size - offset);
        if (got < 0 && errno == EINTR)
            continue;
        if (got <= 0)
            break;
        offset += (size_t)got;
    }
    char extra;
    ssize_t trailing = read(fd, &extra, 1);
    close(fd);

    enum dmesh_topology_result result = DMESH_TOPOLOGY_UNREADABLE;
    if (offset == size && trailing == 0)
        result = peer_authority_adopt(c->authority, document, size, c->keys);
    free(document);

    if (result != DMESH_TOPOLOGY_ADOPTED && result != DMESH_TOPOLOGY_SAME_VERSION) {
        fprintf(stderr, "[peer authority] topology rejected: %s\n", dmesh_topology_result_name(result));
        return EACCES;
    }
    c->verified_ms = now_ms();
    return 0;
}

static const char *required(const char *name)
{
    const char *value = getenv(name);
    if (!value || !*value)
        fprintf(stderr, "[peer authority] %s is required\n", name);
    return value && *value ? value : NULL;
}

int peer_session_control_new(unsigned capacity, struct peer_session_control **out)
{
    if (!out)
        return EINVAL;
    *out = NULL;
    const char *enabled = getenv("DMESH_PEER_REGISTRATION");
    if (!enabled || !strcmp(enabled, "0"))
        return 0;
    if (strcmp(enabled, "1"))
        return EINVAL;

    const char *cluster = required("DPUMESH_CLUSTER_ID");
    const char *node = required("DPUMESH_NODE_NAME");
    const char *path = required("DPUMESH_TOPOLOGY_FILE");
    const char *keys = required("DPUMESH_CONTROLLER_KEY_DIR");
    const char *address = required("DPUMESH_LOCAL_ADDRESS");
    const char *port_text = required("DPUMESH_LOCAL_PORT");
    const char *ca = required("DPUMESH_LOCAL_CA");
    const char *cert = required("DPUMESH_LOCAL_CERT");
    const char *key = required("DPUMESH_LOCAL_KEY");
    const char *host_uri = required("DPUMESH_LOCAL_HOST_URI");
    if (!cluster || !node || !path || !keys || !address || !port_text || !ca || !cert || !key || !host_uri)
        return EINVAL;

    char *end;
    errno = 0;
    unsigned long port = strtoul(port_text, &end, 10);
    if (errno || *end || !port || port > 65535)
        return EINVAL;

    struct peer_session_control *c = calloc(1, sizeof(*c));
    if (!c)
        return ENOMEM;
    int error = peer_authority_new(cluster, node, capacity, &c->authority);
    if (error) {
        free(c);
        return error;
    }
    c->path = strdup(path);
    c->keys = strdup(keys);
    if (!c->path || !c->keys || refresh(c)) {
        peer_session_control_free(c);
        return EINVAL;
    }

    c->local = dmesh_local_control_create(address, (unsigned)port, ca, cert, key, host_uri,
                                          local_request, local_retired, available, c);
    if (!c->local) {
        peer_session_control_free(c);
        return EIO;
    }
    *out = c;
    return 0;
}

void peer_session_control_progress(struct peer_session_control *c)
{
    if (!c)
        return;

    dmesh_local_control_progress(c->local);
    uint64_t now = now_ms();
    if (now >= c->next_poll_ms) {
        c->next_poll_ms = now + 1000;
        (void)refresh(c);
    }
}

struct peer_authority *peer_session_control_authority(struct peer_session_control *c)
{
    return c ? c->authority : NULL;
}

int peer_session_control_healthy(struct peer_session_control *c)
{
    uint8_t session[16];
    if (!c || now_ms() - c->verified_ms > 3000)
        return 0;
    int healthy = dmesh_local_control_session(c->local, session) == 0;
    OPENSSL_cleanse(session, sizeof(session));
    return healthy;
}

int peer_session_control_register(struct peer_session_control *c, uint64_t id, uint64_t epoch,
                                  const uint8_t nonce[32],
                                  const struct dmesh_registration_assertion_msg *assertion)
{
    uint8_t session[16];
    if (!c || dmesh_local_control_session(c->local, session))
        return EACCES;

    int error = peer_authority_register(c->authority, id, epoch, session, nonce,
                                        (uint64_t)time(NULL), assertion);
    OPENSSL_cleanse(session, sizeof(session));
    return error;
}

int peer_session_control_free(struct peer_session_control *c)
{
    if (!c)
        return 0;

    /* Destroying the control calls local_retired; leave the authority alive
     * for that callback and refuse to free registrations still in use. */
    dmesh_local_control_destroy(c->local);
    c->local = NULL;
    int error = peer_authority_free(c->authority);
    if (error)
        return error;
    free(c->path);
    free(c->keys);
    free(c);
    return 0;
}
