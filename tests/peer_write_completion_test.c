#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
/* Exercise the private helpers in their production translation units.
 * Unused hardware paths are discarded by the test linker. */
#include "src/transport/peer/peer_carrier_write_imm.c"
#include "src/transport/peer/peer_write_imm_dpa.c"
#include "device/peer_write_imm_dpa_internal.h"

static void retain(void *owner, void *cookie)
{
    (void)cookie;
    ++*(unsigned *)owner;
}

static void release(void *owner, void *cookie)
{
    (void)cookie;
    assert(*(unsigned *)owner);
    --*(unsigned *)owner;
}

static void prefix_releases_only_completed_sources(void)
{
    const uint32_t window = 128 * 1024;
    struct peer_write_completion q = {0};
    struct peer_write_tx_ring ring = {0};
    unsigned owners[3] = {0};
    const uint32_t bytes[] = {1024, 65536, 512};
    assert(!peer_write_completion_init(&q, window));

    for (unsigned i = 0; i < 3; i++) {
        uint64_t end = q.posted_bytes + bytes[i];
        assert(!peer_write_tx_ring_hold(&ring, end, &owners[i], NULL, retain, release));
        assert(!peer_write_completion_post(&q, bytes[i], end));
        assert(!peer_write_tx_ring_post(&ring, bytes[i], window));
        assert(q.posted_id == i + 1 && owners[i] == 1);
    }
    assert(q.length[1].bytes_minus_one == UINT16_MAX); /* Full 64KiB WRITE. */

    uint64_t end = UINT64_MAX;
    assert(!peer_write_completion_done(&q, 0, &end) && end == 0);
    assert(owners[0] && owners[1] && owners[2]); /* Consumed is not completed. */
    assert(!peer_write_completion_done(&q, 2, &end) && end == 66560);
    assert(!peer_write_tx_ring_done(&ring, end));
    assert(!owners[0] && !owners[1] && owners[2]);
    assert(!peer_write_completion_done(&q, 2, &end) && end == 66560);
    assert(!peer_write_tx_ring_done(&ring, end)); /* Duplicate scan is harmless. */
    assert(peer_write_completion_done(&q, 1, &end) == -1); /* Stale. */
    assert(peer_write_completion_done(&q, 4, &end) == -1); /* Not submitted. */
    assert(q.completed_id == 2 && q.completed_bytes == 66560 && owners[2]);
    assert(!peer_write_completion_done(&q, 3, &end) && end == 67072);
    assert(!peer_write_tx_ring_done(&ring, end));
    assert(!owners[2] && !ring.holds);
    peer_write_completion_free(&q);
}

static void one_owner_can_span_multiple_requests(void)
{
    struct peer_write_completion q = {0};
    struct peer_write_tx_ring ring = {0};
    unsigned owner = 0;
    assert(!peer_write_completion_init(&q, 65536));
    for (unsigned i = 1; i <= 3; i++) {
        assert(!peer_write_tx_ring_hold(&ring, i * 64, &owner, NULL, retain, release));
        assert(!peer_write_completion_post(&q, 64, i * 64));
        assert(!peer_write_tx_ring_post(&ring, 64, 65536));
    }
    assert(owner == 1 && ring.holds == 1);
    uint64_t end;
    assert(!peer_write_completion_done(&q, 2, &end));
    assert(!peer_write_tx_ring_done(&ring, end) && owner == 1);
    assert(!peer_write_completion_done(&q, 3, &end));
    assert(!peer_write_tx_ring_done(&ring, end) && owner == 0);
    peer_write_completion_free(&q);
}

static void wrap_growth_and_reuse(void)
{
    struct peer_write_completion q = {0};
    assert(!peer_write_completion_init(&q, 65536));
    uint32_t initial_id = UINT32_MAX - 20;
    uint64_t initial_bytes = (UINT64_C(1) << 32) + 128;
    q.posted_id = q.completed_id = initial_id;
    q.posted_bytes = q.completed_bytes = initial_bytes;
    uint64_t total = initial_bytes, end;
    for (unsigned i = 0; i < 80; i++) {
        uint32_t bytes = 64 + i;
        total += bytes;
        assert(!peer_write_completion_post(&q, bytes, total));
    }
    assert(q.capacity == 128 && q.posted_id == initial_id + 80u);
    /* A legitimate completion can itself have ID zero. */
    assert(!peer_write_completion_done(&q, 0, &end));
    uint64_t expected = initial_bytes;
    for (unsigned i = 0; i < 21; i++) expected += 64 + i;
    assert(end == expected && q.completed_id == 0);
    assert(!peer_write_completion_done(&q, q.posted_id, &end) && end == total);
    for (unsigned i = 0; i < 150; i++) {
        total += 64;
        assert(!peer_write_completion_post(&q, 64, total));
    }
    assert(q.capacity == 256);
    assert(!peer_write_completion_done(&q, q.posted_id, &end) && end == total);
    peer_write_completion_free(&q);
}

static void cancellation_limits_and_bad_lengths(void)
{
    struct peer_write_completion q = {0};
    uint64_t end = 999;
    assert(!peer_write_completion_init(&q, 128));
    assert(peer_write_completion_post(&q, 3, 3) == -1);
    assert(peer_write_completion_post(&q, 65537, 65537) == -1);
    assert(peer_write_completion_post(&q, 64, 65) == -1);
    assert(!peer_write_completion_post(&q, 64, 64));
    peer_write_completion_cancel(&q); /* Publication failed: reuse the ID. */
    assert(q.posted_id == 0 && q.posted_bytes == 0);
    assert(!peer_write_completion_post(&q, 64, 64));
    assert(!peer_write_completion_post(&q, 64, 128));
    assert(peer_write_completion_post(&q, 64, 192) == -1);
    q.length[1].bytes_minus_one = 0; /* Reject a corrupt prefix atomically. */
    assert(peer_write_completion_done(&q, 2, &end) == -1);
    assert(q.completed_id == 0 && q.completed_bytes == 0 && end == 999);
    q.length[1].bytes_minus_one = 63;
    assert(!peer_write_completion_done(&q, 2, &end) && end == 128);
    peer_write_completion_free(&q);

    assert(!peer_write_completion_init(&q, 65536));
    q.posted_bytes = q.completed_bytes = UINT64_MAX - 63;
    assert(peer_write_completion_post(&q, 64, 0) == -1);
    peer_write_completion_free(&q);
}

static void fence_statistics(void)
{
    struct peer_write_fence_stats stats = {0};
    for (unsigned depth = 1; depth <= 32; depth++)
        peer_write_fence_stats_sample(&stats, depth);
    assert(stats.samples == 32 && stats.depth_sum == 528 && stats.peak == 32);
    assert(stats.histogram[0] == 1 && stats.histogram[1] == 3 && stats.histogram[2] == 4);
    assert(stats.histogram[3] == 8 && stats.histogram[4] == 16);
    stats.histogram[4] = UINT32_MAX;
    peer_write_fence_stats_sample(&stats, 32);
    assert(stats.histogram[4] == UINT32_MAX && stats.samples == 33);
}

int main(void)
{
    assert(sizeof(struct wim_fence) == 8);
    assert(WIM_FENCES == WIM_IMM_RQ && WIM_FENCES == 32);
    assert(sizeof(((struct wim_lane *)0)->fence) == 256);
    assert(sizeof(struct peer_write_completion_length) == 2);
    assert(sizeof(struct peer_write_tx_hold) == 48);
    assert(sizeof(struct wim_lane) % 64 == 0);
    assert(sizeof(struct wim_status) == 192);
    assert(sizeof(struct wim_fence_snapshot) == 64);
    prefix_releases_only_completed_sources();
    one_owner_can_span_multiple_requests();
    wrap_growth_and_reuse();
    cancellation_limits_and_bad_lengths();
    fence_statistics();
    puts("peer_write_completion_test: ok");
    return 0;
}
