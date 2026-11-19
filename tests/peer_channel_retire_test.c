#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "src/transport/peer/peer_channel.h"

struct peer_pair_protocol;
void peer_pair_protocol_free(struct peer_pair_protocol *wire)
{
    assert(!wire);
}

int main(void)
{
    struct dmesh_peer_table *table = calloc(1, sizeof(*table));
    assert(table);
    struct dmesh_peer_channel *c = &table->channels[0];
    c->in_use = 1;
    c->state = DMESH_PEER_OPEN;
    c->rx_frame = malloc(64);
    assert(c->rx_frame);

    assert(dmesh_peer_reap_closed(table) == 0);
    c->state = DMESH_PEER_CLOSED;
    c->local_refs = 1;
    c->retirement_refs = 1;
    assert(dmesh_peer_reap_closed(table) == 0);

    /* Reader release alone cannot recycle a channel with outstanding DMA. */
    c->local_refs = 0;
    assert(dmesh_peer_reap_closed(table) == 0);
    c->retirement_refs = 0;
    assert(dmesh_peer_reap_closed(table) == 1);
    assert(!c->in_use && !c->rx_frame);
    assert(dmesh_peer_reap_closed(table) == 0);

    free(table);
    puts("peer channel retirement: reader and DMA fences: PASS");
}
