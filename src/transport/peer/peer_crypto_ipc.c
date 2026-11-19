#include "peer_crypto_ipc.h"
#include <string.h>
#include <openssl/crypto.h>

/* ---- codec ------------------------------------------------------------- */

struct cursor {
    uint8_t *p;
    size_t left;
    int bad;
};

static void put(struct cursor *c, const void *p, size_t n)
{
    if (c->bad || n > c->left) {
        c->bad = 1;
        return;
    }

    memcpy(c->p, p, n);
    c->p += n;
    c->left -= n;
}

static void get(struct cursor *c, void *p, size_t n)
{
    if (c->bad || n > c->left) {
        c->bad = 1;
        return;
    }

    memcpy(p, c->p, n);
    c->p += n;
    c->left -= n;
}

static void number(struct cursor *c, uint64_t v, unsigned width)
{
    uint8_t b[8];
    for (unsigned i = 0; i < width; i++)
        b[width - 1 - i] = (uint8_t)(v >> (8 * i));

    put(c, b, width);
}

static uint64_t take(struct cursor *c, unsigned width)
{
    uint8_t b[8] = {0};
    uint64_t v = 0;
    get(c, b, width);

    for (unsigned i = 0; i < width; i++)
        v = (v << 8) | b[i];

    return v;
}

static void header(struct cursor *c, uint16_t type, uint32_t body_len)
{
    put(c, "DMCR", 4);
    number(c, PEER_CRYPTO_IPC_VERSION, 2);
    number(c, type, 2);
    number(c, body_len, 4);
    number(c, 0, 4);
}

int peer_crypto_ipc_header_decode(const void *buf, size_t len, uint16_t *type, uint32_t *body_len)
{
    if (!type || !body_len)
        return -1;

    *type = 0;
    *body_len = 0;

    if (!buf && len)
        return -1;
    if (len < PEER_CRYPTO_IPC_HEADER_LEN)
        return 1;

    struct cursor c = {(uint8_t *)buf, PEER_CRYPTO_IPC_HEADER_LEN, 0};
    char magic[4];
    get(&c, magic, 4);

    unsigned version = (unsigned)take(&c, 2);
    *type = (uint16_t)take(&c, 2);
    *body_len = (uint32_t)take(&c, 4);

    uint32_t reserved = (uint32_t)take(&c, 4);
    if (c.bad || memcmp(magic, "DMCR", 4) || version != PEER_CRYPTO_IPC_VERSION || reserved ||
        *type < PEER_CRYPTO_IPC_HELLO || *type > PEER_CRYPTO_IPC_COMPLETION ||
        *body_len > PEER_CRYPTO_IPC_BODY_MAX) {
        *type = 0;
        *body_len = 0;
        return -1;
    }

    return 0;
}

int peer_crypto_ipc_hello_encode(unsigned major, unsigned minor, void *buf, size_t cap, size_t *len)
{
    if (len)
        *len = 0;
    if (!buf || !len || major > 0xffff || minor > 0xffff)
        return -1;

    struct cursor c = {buf, cap, 0};
    header(&c, PEER_CRYPTO_IPC_HELLO, 4);
    number(&c, major, 2);
    number(&c, minor, 2);

    if (c.bad)
        return -1;

    *len = cap - c.left;

    return 0;
}

int peer_crypto_ipc_hello_decode(const void *body, size_t len, unsigned *major, unsigned *minor)
{
    if (!major || !minor)
        return -1;

    *major = *minor = 0;

    if (!body || len != 4)
        return -1;

    struct cursor c = {(uint8_t *)body, len, 0};
    *major = (unsigned)take(&c, 2);
    *minor = (unsigned)take(&c, 2);

    return c.bad ? -1 : 0;
}

int peer_crypto_ipc_request_valid(const struct peer_crypto_request *r)
{
    return r && r->action >= PEER_CRYPTO_INSTALL_RX && r->action <= PEER_CRYPTO_REMOVE_ALL &&
           r->token.manager && r->token.generation && r->operation && r->local_endpoint <= 1 &&
           r->lane_count && r->lane_count <= PEER_ASSOC_LANES_MAX && r->binding.epoch;
}

int peer_crypto_ipc_request_encode(const struct peer_crypto_request *r,
                                   void *buf,
                                   size_t cap,
                                   size_t *len)
{
    if (len)
        *len = 0;
    if (!buf || !len || !peer_crypto_ipc_request_valid(r))
        return -1;

    struct cursor c = {buf, cap, 0};
    size_t body = 8 + 8 + 4 + 8 + 2 + 2 + 8 + 8 + 16 + 16 + 4 + 4 + 20 + 2 +
                  r->lane_count * (8 + 3 + 3 + 16 + 16);
    header(&c, PEER_CRYPTO_IPC_REQUEST, (uint32_t)body);
    number(&c, r->token.manager, 8);
    number(&c, r->token.generation, 8);
    number(&c, r->token.slot, 4);
    number(&c, r->operation, 8);
    number(&c, (unsigned)r->action, 2);
    number(&c, r->local_endpoint, 2);
    number(&c, r->binding.epoch, 8);
    number(&c, r->old_epoch, 8);
    put(&c, r->binding.session, 16);
    put(&c, r->binding.association, 16);
    number(&c, r->binding.spi[0], 4);
    number(&c, r->binding.spi[1], 4);
    put(&c, r->material, 20);
    number(&c, r->lane_count, 2);

    for (unsigned i = 0; i < r->lane_count; i++) {
        const struct peer_sec_lane *l = &r->lanes[i];
        number(&c, l->id, 8);
        number(&c, l->qpn[0], 3);
        number(&c, l->qpn[1], 3);
        put(&c, l->gid[0], 16);
        put(&c, l->gid[1], 16);
    }

    if (c.bad)
        return -1;

    *len = cap - c.left;

    return 0;
}

int peer_crypto_ipc_request_decode(const void *body, size_t len, struct peer_crypto_request *out)
{
    if (!out)
        return -1;

    memset(out, 0, sizeof(*out));

    if (!body)
        return -1;

    struct peer_crypto_request r = {0};
    struct cursor c = {(uint8_t *)body, len, 0};
    r.token.manager = take(&c, 8);
    r.token.generation = take(&c, 8);
    r.token.slot = (uint32_t)take(&c, 4);
    r.operation = take(&c, 8);
    r.action = (enum peer_crypto_action)take(&c, 2);
    r.local_endpoint = (unsigned)take(&c, 2);
    r.binding.epoch = take(&c, 8);
    r.old_epoch = take(&c, 8);
    get(&c, r.binding.session, 16);
    get(&c, r.binding.association, 16);
    r.binding.spi[0] = (uint32_t)take(&c, 4);
    r.binding.spi[1] = (uint32_t)take(&c, 4);
    get(&c, r.material, 20);
    r.lane_count = (unsigned)take(&c, 2);
    if (c.bad || r.lane_count > PEER_ASSOC_LANES_MAX) {
        OPENSSL_cleanse(&r, sizeof(r));
        return -1;
    }

    for (unsigned i = 0; i < r.lane_count; i++) {
        struct peer_sec_lane *l = &r.lanes[i];
        l->id = take(&c, 8);
        l->qpn[0] = (uint32_t)take(&c, 3);
        l->qpn[1] = (uint32_t)take(&c, 3);
        get(&c, l->gid[0], 16);
        get(&c, l->gid[1], 16);
    }

    if (c.bad || c.left || !peer_crypto_ipc_request_valid(&r)) {
        OPENSSL_cleanse(&r, sizeof(r));
        return -1;
    }

    *out = r;
    OPENSSL_cleanse(&r, sizeof(r));

    return 0;
}

int peer_crypto_ipc_completion_encode(const struct peer_crypto_completion *d,
                                      void *buf,
                                      size_t cap,
                                      size_t *len)
{
    if (len)
        *len = 0;
    if (!buf || !len || !d || d->status > 0 || d->status < -2)
        return -1;

    struct cursor c = {buf, cap, 0};
    header(&c, PEER_CRYPTO_IPC_COMPLETION, 8 + 8 + 4 + 8 + 2 + 2 + 16 + 16 + 8);
    number(&c, d->token.manager, 8);
    number(&c, d->token.generation, 8);
    number(&c, d->token.slot, 4);
    number(&c, d->operation, 8);
    number(&c, (unsigned)d->action, 2);
    number(&c, (uint16_t)(int16_t)d->status, 2);
    put(&c, d->session, 16);
    put(&c, d->association, 16);
    number(&c, d->epoch, 8);

    if (c.bad)
        return -1;

    *len = cap - c.left;

    return 0;
}

int peer_crypto_ipc_completion_decode(const void *body,
                                      size_t len,
                                      struct peer_crypto_completion *out)
{
    if (!out)
        return -1;

    memset(out, 0, sizeof(*out));

    if (!body)
        return -1;

    struct cursor c = {(uint8_t *)body, len, 0};
    out->token.manager = take(&c, 8);
    out->token.generation = take(&c, 8);
    out->token.slot = (uint32_t)take(&c, 4);
    out->operation = take(&c, 8);
    out->action = (enum peer_crypto_action)take(&c, 2);
    out->status = (int16_t)take(&c, 2);
    get(&c, out->session, 16);
    get(&c, out->association, 16);
    out->epoch = take(&c, 8);

    if (c.bad || c.left || out->status > 0 || out->status < -2 ||
        out->action < PEER_CRYPTO_INSTALL_RX || out->action > PEER_CRYPTO_REMOVE_ALL) {
        memset(out, 0, sizeof(*out));
        return -1;
    }

    return 0;
}
