#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "local_control_session.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <openssl/crypto.h>
#include <openssl/rand.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/* Paired-host TLS control; private to this registration session owner. */
typedef unsigned (*dmesh_local_dispatch)(void *, const struct dmesh_local_request *);
typedef void (*dmesh_local_retire)(void *);
typedef int (*dmesh_local_available)(void *);

struct dmesh_local_control;
static void dmesh_local_control_destroy(struct dmesh_local_control *);

struct dmesh_local_control {
    SSL_CTX *ctx;
    SSL *ssl;
    int listener, fd, authenticated;
    char *host_uri;
    uint8_t session[DMESH_CONTROL_SESSION_SIZE];
    uint64_t sequence;
    time_t activity;
    size_t received, sent, pending;
    struct dmesh_local_request request, previous;
    struct dmesh_local_response response, previous_response;
    dmesh_local_dispatch dispatch;
    dmesh_local_retire retire;
    dmesh_local_available available;
    void *owner;
};
static uint64_t decode(const uint8_t *p, size_t n)
{ uint64_t v = 0; for (size_t i = 0; i < n; i++) v |= (uint64_t)p[i] << (i * 8); return v; }
static void encode(uint8_t *p, uint64_t v, size_t n)
{ for (size_t i = 0; i < n; i++) p[i] = (uint8_t)(v >> (i * 8)); }
static int zeros(const void *ptr, size_t n)
{ const uint8_t *p = ptr; for (size_t i = 0; i < n; i++) if (p[i]) return 0; return 1; }
static time_t monotonic_seconds(void)
{ struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return t.tv_sec; }
static void disconnect_peer(struct dmesh_local_control *c)
{
    if (c->authenticated) c->retire(c->owner);
    c->authenticated = 0;
    SSL_free(c->ssl); c->ssl = NULL;
    if (c->fd >= 0) close(c->fd);
    c->fd = -1; c->received = c->sent = c->pending = 0; c->sequence = 0;
    memset(c->session, 0, sizeof(c->session));
}
static int peer_allowed(struct dmesh_local_control *c)
{
    if (SSL_get_verify_result(c->ssl) != X509_V_OK) return 0;
    X509 *cert = SSL_get1_peer_certificate(c->ssl);
    if (!cert) return 0;
    GENERAL_NAMES *names = X509_get_ext_d2i(cert, NID_subject_alt_name, NULL, NULL);
    int count = 0, matched = 0;
    for (int i = 0; names && i < sk_GENERAL_NAME_num(names); i++) {
        const GENERAL_NAME *name = sk_GENERAL_NAME_value(names, i);
        if (name->type != GEN_URI) continue;
        count++;
        const ASN1_STRING *uri = name->d.uniformResourceIdentifier;
        matched = ASN1_STRING_length(uri) == (int)strlen(c->host_uri) &&
                  memcmp(ASN1_STRING_get0_data(uri), c->host_uri, strlen(c->host_uri)) == 0;
    }
    GENERAL_NAMES_free(names); X509_free(cert);
    return count == 1 && matched;
}
static struct dmesh_local_control *dmesh_local_control_create(
    const char *address, unsigned port, const char *ca, const char *certificate,
    const char *key, const char *host_uri, dmesh_local_dispatch dispatch,
    dmesh_local_retire retire, dmesh_local_available available, void *owner)
{
    if (!address || !ca || !certificate || !key || !host_uri || !*host_uri ||
        !port || port > 65535 || !dispatch || !retire || !available) return NULL;
    struct dmesh_local_control *c = calloc(1, sizeof(*c));
    if (!c) return NULL;
    c->fd = c->listener = -1; c->host_uri = strdup(host_uri);
    c->dispatch = dispatch; c->retire = retire; c->available = available; c->owner = owner;
    c->ctx = SSL_CTX_new(TLS_server_method());
    if (!c->host_uri || !c->ctx ||
        !SSL_CTX_set_min_proto_version(c->ctx, TLS1_3_VERSION) ||
        !SSL_CTX_load_verify_locations(c->ctx, ca, NULL) ||
        !SSL_CTX_use_certificate_chain_file(c->ctx, certificate) ||
        !SSL_CTX_use_PrivateKey_file(c->ctx, key, SSL_FILETYPE_PEM) ||
        !SSL_CTX_check_private_key(c->ctx)) goto failed;
    SSL_CTX_set_verify(c->ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, NULL);
    SSL_CTX_set_session_cache_mode(c->ctx, SSL_SESS_CACHE_OFF);
    SSL_CTX_set_num_tickets(c->ctx, 0);
    SSL_CTX_set_max_early_data(c->ctx, 0);
    struct sockaddr_in a = { .sin_family = AF_INET, .sin_port = htons(port) };
    if (inet_pton(AF_INET, address, &a.sin_addr) != 1) goto failed;
    c->listener = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    int one = 1;
    if (c->listener < 0 || setsockopt(c->listener, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) ||
        bind(c->listener, (struct sockaddr *)&a, sizeof(a)) || listen(c->listener, 8)) goto failed;
    return c;
failed:
    dmesh_local_control_destroy(c); return NULL;
}
static void reply(struct dmesh_local_control *c, uint64_t seq, unsigned status)
{
    memcpy(c->response.magic, DMESH_LOCAL_MAGIC, 8);
    memcpy(c->response.session, c->session, sizeof(c->session));
    encode(c->response.sequence, seq, 8); encode(c->response.status, status, 4);
    c->sent = 0; c->pending = sizeof(c->response);
}
static int wants_io(SSL *ssl, int result)
{ int e = SSL_get_error(ssl, result); return e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE; }
static int dmesh_local_control_progress(struct dmesh_local_control *c)
{
    if (!c) return 0;
    int fd = accept4(c->listener, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (fd >= 0) {
        if (c->ssl || !c->available(c->owner)) close(fd);
        else {
            c->fd = fd; c->ssl = SSL_new(c->ctx);
            if (!c->ssl || !SSL_set_fd(c->ssl, fd)) { disconnect_peer(c); return 1; }
            SSL_set_accept_state(c->ssl); c->activity = monotonic_seconds();
        }
    }
    if (!c->ssl) return 0;
    if (monotonic_seconds() - c->activity >= (c->authenticated ? 15 : 3)) goto failed;
    if (!c->authenticated) {
        int r = SSL_accept(c->ssl);
        if (r != 1) { if (wants_io(c->ssl, r)) return 0; goto failed; }
        if (!peer_allowed(c) || !c->available(c->owner) || RAND_bytes(c->session, sizeof(c->session)) != 1) goto failed;
        c->authenticated = 1; c->activity = monotonic_seconds(); reply(c, 0, DMESH_LOCAL_OK);
    }
    if (c->pending) {
        int r = SSL_write(c->ssl, (uint8_t *)&c->response + c->sent, c->pending - c->sent);
        if (r <= 0) { if (wants_io(c->ssl, r)) return 0; goto failed; }
        c->sent += r;
        if (c->sent == c->pending) c->pending = c->sent = 0;
        return 1;
    }
    int r = SSL_read(c->ssl, (uint8_t *)&c->request + c->received, sizeof(c->request) - c->received);
    if (r <= 0) { if (wants_io(c->ssl, r)) return 0; goto failed; }
    c->received += r;
    if (c->received != sizeof(c->request)) return 1;
    c->received = 0;
    uint64_t seq = decode(c->request.sequence, 8);
    if (memcmp(c->session, c->request.session, sizeof(c->session)) || !zeros(c->request.reserved, 7)) goto failed;
    if (seq && seq == c->sequence && !memcmp(&c->request, &c->previous, sizeof(c->request))) {
        c->response = c->previous_response; c->pending = sizeof(c->response);
    } else {
        if (!seq || seq <= c->sequence) goto failed;
        unsigned status;
        if (c->request.operation == DMESH_LOCAL_PING)
            status = zeros(c->request.connection_id, sizeof(c->request.connection_id)) ? DMESH_LOCAL_OK : DMESH_LOCAL_INVALID;
        else status = c->dispatch(c->owner, &c->request);
        c->sequence = seq; c->previous = c->request;
        reply(c, seq, status); c->previous_response = c->response;
    }
    c->activity = monotonic_seconds();
    return 1;
failed:
    disconnect_peer(c); return 1;
}
/* Copy the current authenticated session id; a new connection gets a fresh id. */
static int dmesh_local_control_session(const struct dmesh_local_control *c,
                                uint8_t out[DMESH_CONTROL_SESSION_SIZE])
{
    if (!c || !c->ssl || !c->authenticated) return -1;
    memcpy(out, c->session, sizeof(c->session));
    return 0;
}
static void dmesh_local_control_destroy(struct dmesh_local_control *c)
{
    if (!c) return;
    disconnect_peer(c);
    if (c->listener >= 0) close(c->listener);
    SSL_CTX_free(c->ctx); free(c->host_uri); free(c);
}

/* Registration, topology refresh and public session API. */
struct local_control_session {
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
    struct local_control_session *c = ctx;
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
    struct local_control_session *c = ctx;
    peer_authority_retire_all(c->authority);
}

static int available(void *ctx)
{
    struct local_control_session *c = ctx;
    struct peer_authority_view *view = peer_authority_acquire(c->authority);
    int ready = view != NULL;
    peer_authority_release(view);
    unsigned cursor = 0;
    struct peer_local_registration r;
    if (peer_authority_retiring(c->authority, &cursor, &r))
        ready = 0;
    return ready;
}

static int refresh(struct local_control_session *c)
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

int local_control_session_new(unsigned capacity, struct local_control_session **out)
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

    struct local_control_session *c = calloc(1, sizeof(*c));
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
        local_control_session_free(c);
        return EINVAL;
    }

    c->local = dmesh_local_control_create(address, (unsigned)port, ca, cert, key, host_uri,
                                          local_request, local_retired, available, c);
    if (!c->local) {
        local_control_session_free(c);
        return EIO;
    }
    *out = c;
    return 0;
}

void local_control_session_progress(struct local_control_session *c)
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

struct peer_authority *local_control_session_authority(struct local_control_session *c)
{
    return c ? c->authority : NULL;
}

int local_control_session_healthy(struct local_control_session *c)
{
    uint8_t session[16];
    if (!c || now_ms() - c->verified_ms > 3000)
        return 0;
    int healthy = dmesh_local_control_session(c->local, session) == 0;
    OPENSSL_cleanse(session, sizeof(session));
    return healthy;
}

int local_control_session_register(struct local_control_session *c, uint64_t id, uint64_t epoch,
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

int local_control_session_free(struct local_control_session *c)
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
