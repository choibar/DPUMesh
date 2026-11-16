#include <assert.h>
#include <errno.h>
#include <openssl/evp.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include "src/transport/peer/peer_authority.h"

static const char *uid = "12345678-1234-1234-1234-123456789abc";
static EVP_PKEY *signer;
static char keys[] = "/tmp/dmesh-authority-XXXXXX";

static size_t document(char out[4096], unsigned version, int present, const char *ip)
{
    int n = snprintf(out, 4096, "version=%u\nnode=node-a,192.0.2.1:4790,"
        "1111111111111111111111111111111111111111111111111111111111111111\n", version);
    if (present)
        n += snprintf(out + n, 4096 - n, "pod=%s,node-a,bench,default,%s\n", uid, ip);

    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    unsigned char signature[64];
    size_t length = sizeof(signature);
    assert(EVP_DigestSignInit(ctx, NULL, NULL, NULL, signer) == 1);
    assert(EVP_DigestSign(ctx, signature, &length, (unsigned char *)out, n) == 1);
    EVP_MD_CTX_free(ctx);
    n += snprintf(out + n, 4096 - n, "signature=test,");
    for (unsigned i = 0; i < length; ++i)
        n += snprintf(out + n, 4096 - n, "%02x", signature[i]);
    out[n++] = '\n';
    return (size_t)n;
}

static void assertion(struct dmesh_registration_assertion_msg *out,
                       const unsigned char key[16], const unsigned char nonce[32])
{
    memset(out, 0, sizeof(*out));
    out->type = DMESH_MSG_REG_ASSERTION;
    out->version = DMESH_LOCAL_VERSION;
    struct dmesh_workload_identity *id = &out->identity;
    id->type = DMESH_IDENTITY_TYPE;
    id->version = DMESH_IDENTITY_VERSION;
    dmesh_wire_put_u64_le(id->issued_at_le, 100);
    dmesh_wire_put_u64_le(id->expires_at_le, 130);
    dmesh_wire_put_u64_le(id->channel_generation_le, 1);
    memset(id->daemon_incarnation, 3, 16);
    memcpy(id->nonce, nonce, 32);
    strcpy(id->cluster_id, "cluster-a");
    strcpy(id->node_name, "node-a");
    strcpy(id->namespace_name, "bench");
    strcpy(id->pod_uid, uid);
    strcpy(id->pod_name, "client");
    strcpy(id->service_account, "default");
    strcpy(id->container_name, "client");
    memset(id->container_id, 'a', 64);
    strcpy(id->pod_ip, "10.1.0.2");
    assert(dmesh_registration_seal(key, out) == 0);
}

int main(void)
{
    assert(mkdtemp(keys));
    unsigned char seed[32], public_key[32];
    memset(seed, 7, sizeof(seed));
    signer = EVP_PKEY_new_raw_private_key(EVP_PKEY_ED25519, NULL, seed, sizeof(seed));
    assert(signer);
    size_t length = sizeof(public_key);
    assert(EVP_PKEY_get_raw_public_key(signer, public_key, &length) == 1);
    char path[256];
    snprintf(path, sizeof(path), "%s/test.key", keys);
    FILE *file = fopen(path, "wb");
    assert(file && fwrite(public_key, 1, sizeof(public_key), file) == sizeof(public_key));
    assert(!fclose(file) && !chmod(path, 0600));

    struct peer_authority *a;
    assert(!peer_authority_new("cluster-a", "node-a", 4, &a));
    char doc[4096];
    size_t bytes = document(doc, 1, 1, "10.1.0.2");
    assert(peer_authority_adopt(a, doc, bytes, keys) == DMESH_TOPOLOGY_ADOPTED);
    assert(peer_authority_adopt(a, doc, bytes, keys) == DMESH_TOPOLOGY_SAME_VERSION);

    struct peer_authority_view *old = peer_authority_acquire(a);
    assert(peer_authority_pod(old, uid));
    bytes = document(doc, 1, 1, "10.1.0.3");
    assert(peer_authority_adopt(a, doc, bytes, keys) == DMESH_TOPOLOGY_MALFORMED);
    doc[bytes - 3] ^= 1;
    assert(peer_authority_adopt(a, doc, bytes, keys) == DMESH_TOPOLOGY_BAD_SIG);

    unsigned char key[16], nonce[32];
    memset(key, 5, sizeof(key));
    memset(nonce, 9, sizeof(nonce));
    struct dmesh_registration_assertion_msg reg;
    assertion(&reg, key, nonce);
    assert(!peer_authority_register(a, 1, 1, key, nonce, 110, &reg));
    assert(!peer_authority_register(a, 1, 1, key, nonce, 110, &reg));
    assert(peer_authority_register(a, 2, 1, key, nonce, 110, &reg) == EBUSY);
    assert(peer_authority_register(a, 1, 2, key, nonce, 110, &reg) == EBUSY);
    assert(peer_authority_free(a) == EBUSY);
    assert(peer_authority_retired_done(a, 1, 1) == EBUSY);
    assert(!peer_authority_hold(a, 1, 1));
    const _Atomic unsigned *active = NULL;
    assert(!peer_authority_watch(a, 1, 1, &active));
    assert(active && atomic_load_explicit(active, memory_order_acquire) == 1);

    reg.identity.pod_ip[7] = '3';
    assert(peer_authority_register(a, 1, 1, key, nonce, 110, &reg) == EACCES);
    assertion(&reg, key, nonce);
    assert(peer_authority_register(a, 1, 1, key, nonce, 131, &reg) == EACCES);

    bytes = document(doc, 2, 0, NULL);
    assert(peer_authority_adopt(a, doc, bytes, keys) == DMESH_TOPOLOGY_ADOPTED);
    assert(atomic_load_explicit(active, memory_order_acquire) == 0);
    assert(peer_authority_register(a, 2, 1, key, nonce, 110, &reg) == EAGAIN);
    reg.mac[0] ^= 1;
    assert(peer_authority_register(a, 2, 1, key, nonce, 110, &reg) == EACCES);
    reg.mac[0] ^= 1;
    struct peer_local_registration r;
    assert(!peer_authority_session(a, 1, 1, &r));
    unsigned cursor = 0;
    assert(peer_authority_retiring(a, &cursor, &r));
    assert(r.session_id == 1 && r.session_epoch == 1);
    assert(peer_authority_pod(old, uid)->ip_be == 0x0a010002);
    assert(peer_authority_tables(old)->version == 1);

    bytes = document(doc, 1, 1, "10.1.0.2");
    assert(peer_authority_adopt(a, doc, bytes, keys) == DMESH_TOPOLOGY_ROLLBACK);
    bytes = document(doc, 3, 1, "10.1.0.2");
    assert(peer_authority_adopt(a, doc, bytes, keys) == DMESH_TOPOLOGY_ADOPTED);
    assert(peer_authority_register(a, 1, 1, key, nonce, 110, &reg) == ESTALE);
    assert(peer_authority_hold(a, 1, 1) == ESTALE);
    assert(peer_authority_retired_done(a, 1, 1) == EBUSY);
    assert(!peer_authority_unhold(a, 1, 1));
    assert(peer_authority_retired_done(a, 1, 1) == EBUSY);
    assert(!peer_authority_unhold(a, 1, 1));
    assert(!peer_authority_retired_done(a, 1, 1));

    nonce[0]++;
    assertion(&reg, key, nonce);
    dmesh_wire_put_u64_le(reg.identity.channel_generation_le, 2);
    assert(!dmesh_registration_seal(key, &reg));
    assert(!peer_authority_register(a, 1, 2, key, nonce, 110, &reg));
    assert(peer_authority_retired_done(a, 1, 1) == ENOENT);
    assert(peer_authority_session(a, 1, 2, &r));
    peer_authority_retire_nonce(a, nonce);
    assert(!peer_authority_session(a, 1, 2, &r));
    assert(!peer_authority_retired_done(a, 1, 2));
    assert(!peer_authority_free(a));
    assert(peer_authority_pod(old, uid));
    peer_authority_release(old);

    EVP_PKEY_free(signer);
    assert(!unlink(path) && !rmdir(keys));
    puts("peer authority: signature, rollback, replay, revocation, reuse fence, snapshot lifetime: PASS");
}
