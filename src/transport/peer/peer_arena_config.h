#ifndef DMESH_PEER_ARENA_CONFIG_H
#define DMESH_PEER_ARENA_CONFIG_H
/* Proxy arena geometry. Native WRITE uses independent contiguous byte rings. */
#define PEER_ARENA_CHUNK_BYTES (64u * 1024u)
#define PEER_ARENA_CHUNKS 1024u
#define PEER_ARENA_ALIGN 128u
#define PEER_ARENA_HEADROOM 64u
#define PEER_ARENA_BYTES ((size_t)PEER_ARENA_CHUNKS * PEER_ARENA_CHUNK_BYTES)
#endif
