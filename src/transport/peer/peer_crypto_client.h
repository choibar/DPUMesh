#ifndef DMESH_PEER_CRYPTO_CLIENT_H
#define DMESH_PEER_CRYPTO_CLIENT_H
#include "peer_crypto.h"

/* The runtime side: a `peer_crypto_adapter` that connects to the owner's
 * socket, reconnects when it drops, and answers the manager honestly when it
 * cannot reach hardware. Requests in flight when the owner disappears are
 * completed as unknown outcome (-2); an install that cannot be sent is refused
 * (-1); `healthy` reports whether an owner is connected right now. */
struct peer_crypto_client;
int peer_crypto_client_new(const char *socket_path,
                        struct peer_crypto_client **out,
                        char *error,
                        size_t error_len);
void peer_crypto_client_free(struct peer_crypto_client *);
struct peer_crypto_adapter peer_crypto_client_adapter(struct peer_crypto_client *);
#endif
