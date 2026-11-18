#include "peer_crypto_client.h"
#include "peer_crypto_ipc.h"
#include "peer_config.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>
#include <openssl/crypto.h>

#define TX_MAX (PEER_CRYPTO_IPC_TX_FRAMES * PEER_CRYPTO_IPC_FRAME_MAX)

struct inflight {
    int used;
    struct peer_crypto_completion key;
};

struct peer_crypto_client {
    char path[108];
    int fd, epfd, ready; /* ready: HELLO received */
    uint64_t next_connect_ns;
    uint8_t tx[TX_MAX];
    size_t tx_len;
    uint8_t rx[PEER_CRYPTO_IPC_FRAME_MAX];
    size_t rx_len;
    struct inflight inflight[PEER_CRYPTO_IPC_INFLIGHT_MAX];
    struct peer_crypto_completion pending[PEER_CRYPTO_IPC_INFLIGHT_MAX];
    unsigned pending_head, pending_count;
};

static uint64_t now_ns(void)
{
    struct timespec t;
    if (clock_gettime(CLOCK_MONOTONIC, &t))
        return 0;

    return (uint64_t)t.tv_sec * 1000000000ull + (uint64_t)t.tv_nsec;
}

static void queue_completion(struct peer_crypto_client *c, const struct peer_crypto_completion *d)
{
    if (c->pending_count == PEER_CRYPTO_IPC_INFLIGHT_MAX)
        return;

    c->pending[(c->pending_head + c->pending_count) % PEER_CRYPTO_IPC_INFLIGHT_MAX] = *d;
    c->pending_count++;
}

/* Every request outstanding on a lost owner ends as unknown outcome: the
 * hardware state left with the process and nobody can say what it did. */
static void disconnect(struct peer_crypto_client *c)
{
    if (c->fd >= 0) {
        epoll_ctl(c->epfd, EPOLL_CTL_DEL, c->fd, NULL);
        close(c->fd);
    }

    c->fd = -1;
    c->ready = 0;
    c->rx_len = 0;
    OPENSSL_cleanse(c->tx, sizeof(c->tx));
    c->tx_len = 0;

    for (unsigned i = 0; i < PEER_CRYPTO_IPC_INFLIGHT_MAX; i++) {
        if (!c->inflight[i].used)
            continue;

        struct peer_crypto_completion d = c->inflight[i].key;
        d.status = -2;
        queue_completion(c, &d);
        c->inflight[i].used = 0;
    }

    c->next_connect_ns = now_ns() + PEER_CRYPTO_IPC_RECONNECT_NS;
}

static void try_connect(struct peer_crypto_client *c)
{
    if (c->fd >= 0 || now_ns() < c->next_connect_ns)
        return;

    c->next_connect_ns = now_ns() + PEER_CRYPTO_IPC_RECONNECT_NS;

    int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0)
        return;

    struct sockaddr_un addr = {.sun_family = AF_UNIX};
    memcpy(addr.sun_path, c->path, sizeof(addr.sun_path));

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) && errno != EINPROGRESS) {
        close(fd);
        return;
    }

    /* Readable-only: a writable socket would keep the descriptor the manager
     * sleeps on permanently ready. A connect still in progress surfaces as
     * EAGAIN on the first receive and as a hangup if it fails. */
    struct epoll_event e = {.events = EPOLLIN | EPOLLRDHUP};
    if (epoll_ctl(c->epfd, EPOLL_CTL_ADD, fd, &e)) {
        close(fd);
        return;
    }

    c->fd = fd;
}

static int flush_tx(struct peer_crypto_client *c)
{
    while (c->tx_len) {
        ssize_t n = send(c->fd, c->tx, c->tx_len, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return 0;

            return -1;
        }

        OPENSSL_cleanse(c->tx, (size_t)n);
        memmove(c->tx, c->tx + n, c->tx_len - (size_t)n);
        c->tx_len -= (size_t)n;
    }
    return 0;
}

static int consume(struct peer_crypto_client *c)
{
    for (;;) {
        uint16_t type;
        uint32_t body_len;
        int rc = peer_crypto_ipc_header_decode(c->rx, c->rx_len, &type, &body_len);
        if (rc < 0)
            return -1;
        if (rc > 0 || c->rx_len < PEER_CRYPTO_IPC_HEADER_LEN + body_len)
            return 0;

        const uint8_t *body = c->rx + PEER_CRYPTO_IPC_HEADER_LEN;
        if (type == PEER_CRYPTO_IPC_HELLO) {
            unsigned major, minor;
            if (peer_crypto_ipc_hello_decode(body, body_len, &major, &minor))
                return -1;

            c->ready = 1;
        } else if (type == PEER_CRYPTO_IPC_COMPLETION) {
            struct peer_crypto_completion d;
            if (peer_crypto_ipc_completion_decode(body, body_len, &d))
                return -1;

            for (unsigned i = 0; i < PEER_CRYPTO_IPC_INFLIGHT_MAX; i++) {
                struct inflight *f = &c->inflight[i];
                if (f->used && f->key.operation == d.operation &&
                    f->key.token.manager == d.token.manager &&
                    f->key.token.generation == d.token.generation &&
                    f->key.token.slot == d.token.slot && f->key.action == d.action) {
                    f->used = 0;
                    queue_completion(c, &d);
                    break;
                }
            }
        } else
            return -1; /* the owner never sends requests */

        size_t frame = PEER_CRYPTO_IPC_HEADER_LEN + body_len;
        memmove(c->rx, c->rx + frame, c->rx_len - frame);
        c->rx_len -= frame;
    }
}

static void pump(struct peer_crypto_client *c)
{
    try_connect(c);

    if (c->fd < 0)
        return;
    if (flush_tx(c) < 0) {
        disconnect(c);
        return;
    }

    for (;;) {
        ssize_t n = recv(c->fd, c->rx + c->rx_len, sizeof(c->rx) - c->rx_len, 0);
        if (n == 0) {
            disconnect(c);
            return;
        }
        if (n < 0) {
            if (errno == EINTR)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;

            disconnect(c);
            return;
        }

        c->rx_len += (size_t)n;

        if (consume(c) < 0) {
            disconnect(c);
            return;
        }
        if (c->rx_len == sizeof(c->rx)) {
            disconnect(c);
            return;
        } /* oversized frame */
    }
}

static int ipc_submit(void *ctx, const struct peer_crypto_request *r)
{
    struct peer_crypto_client *c = ctx;
    pump(c);

    if (c->fd < 0 || !c->ready || !peer_crypto_ipc_request_valid(r))
        return -1;

    struct inflight *slot = NULL;
    for (unsigned i = 0; i < PEER_CRYPTO_IPC_INFLIGHT_MAX && !slot; i++)
        if (!c->inflight[i].used)
            slot = &c->inflight[i];

    uint8_t frame[PEER_CRYPTO_IPC_FRAME_MAX];
    size_t len;
    if (!slot || peer_crypto_ipc_request_encode(r, frame, sizeof(frame), &len))
        return slot ? -1 : 0;
    if (sizeof(c->tx) - c->tx_len < len) {
        OPENSSL_cleanse(frame, sizeof(frame));
        return 0;
    }

    memcpy(c->tx + c->tx_len, frame, len);
    c->tx_len += len;
    OPENSSL_cleanse(frame, sizeof(frame));
    slot->used = 1;
    slot->key = (struct peer_crypto_completion){.token = r->token,
                                                .operation = r->operation,
                                                .action = r->action,
                                                .epoch = r->binding.epoch};
    memcpy(slot->key.session, r->binding.session, 16);
    memcpy(slot->key.association, r->binding.association, 16);

    if (flush_tx(c) < 0)
        disconnect(c); /* the queued completion (-2) reports it */

    return 1;
}

static int ipc_poll(void *ctx, struct peer_crypto_completion *out)
{
    struct peer_crypto_client *c = ctx;
    pump(c);

    if (!c->pending_count)
        return 0;

    *out = c->pending[c->pending_head];
    c->pending_head = (c->pending_head + 1) % PEER_CRYPTO_IPC_INFLIGHT_MAX;
    c->pending_count--;

    return 1;
}

static int ipc_fd(void *ctx)
{
    return ((struct peer_crypto_client *)ctx)->epfd;
}

static int ipc_healthy(void *ctx)
{
    struct peer_crypto_client *c = ctx;
    pump(c);
    return c->fd >= 0 && c->ready;
}

struct peer_crypto_adapter peer_crypto_client_adapter(struct peer_crypto_client *c)
{
    return (struct peer_crypto_adapter){
        .ctx = c, .submit = ipc_submit, .poll = ipc_poll, .fd = ipc_fd, .healthy = ipc_healthy};
}

int peer_crypto_client_new(const char *path,
                        struct peer_crypto_client **out,
                        char *error,
                        size_t error_len)
{
    if (error && error_len)
        error[0] = 0;
    if (!out)
        return -1;

    *out = NULL;

    if (!path || !*path || strlen(path) >= sizeof(((struct peer_crypto_client *)0)->path)) {
        if (error && error_len)
            snprintf(error, error_len, "crypto socket path missing or too long");
        return -1;
    }

    struct peer_crypto_client *c = calloc(1, sizeof(*c));
    if (!c)
        return -1;

    strcpy(c->path, path);
    c->fd = -1;
    c->epfd = epoll_create1(EPOLL_CLOEXEC);
    if (c->epfd < 0) {
        free(c);

        if (error && error_len)
            snprintf(error, error_len, "epoll: %s", strerror(errno));
        return -1;
    }

    try_connect(c);
    *out = c;

    return 0;
}

void peer_crypto_client_free(struct peer_crypto_client *c)
{
    if (!c)
        return;

    if (c->fd >= 0)
        close(c->fd);
    close(c->epfd);
    OPENSSL_cleanse(c, sizeof(*c));
    free(c);
}
