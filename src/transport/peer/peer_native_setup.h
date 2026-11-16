#ifndef DMESH_PEER_NATIVE_SETUP_H
#define DMESH_PEER_NATIVE_SETUP_H
#include <stdint.h>
#define PEER_NATIVE_SETUP_MAX 768u

/* Opaque SDK connection export, confined to authenticated daemon control.
 * No application/Pod pointers or capabilities may select this metadata. */
struct peer_native_setup {
    uint16_t bytes;
    uint8_t data[PEER_NATIVE_SETUP_MAX];
};
#endif
