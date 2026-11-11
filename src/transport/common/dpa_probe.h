#ifndef DMESH_DPA_PROBE_H
#define DMESH_DPA_PROBE_H

#include <stdint.h>

/*
 * Measurements taken before DPA flow multiplexing is built (feature/dpa-mux,
 * docs/2026-10-13_dpa-mux-probe.md). One extra DPA thread runs next to the
 * per-flow threads and either reads several live connections' forward rings
 * (POLL) or copies from two connections' host buffers through one producer
 * (FAULT). The native kernel never sees this state. Every field is 64-bit so
 * the ARM and DPA toolchains agree on the layout.
 */

#define MUX_PROBE_MAX_RINGS 16u
#define MUX_PROBE_R_STEPS   5u     /* R = 1, 2, 4, 8, 16 rings per pass */
#define MUX_PROBE_VARIANTS  4u
#define MUX_PROBE_CHUNK     1000u  /* POLL passes per activation */
#define MUX_PROBE_SLICE     256u   /* FAULT copies per activation */
#define MUX_PROBE_POST_STALL 16u   /* FAULT copies from the other source after a stall */

enum mux_probe_mode { MUX_PROBE_POLL = 1, MUX_PROBE_FAULT = 2 };

/* POLL variants. One pass reads R 64-bit words from host memory:
 *   LOOKUP     R rings' producer_tail, buf_arr lookup inside the pass,
 *              one window invalidate per pass (the native kernel's pattern)
 *   CACHED     the same words through pointers resolved once
 *   SAME_RING  R descriptor lines of ring 0: PCIe reads without switching rings
 *   INV_EACH   R rings, invalidate before every read */
enum mux_probe_variant {
    MUX_PROBE_LOOKUP = 0,
    MUX_PROBE_CACHED = 1,
    MUX_PROBE_SAME_RING = 2,
    MUX_PROBE_INV_EACH = 3,
};

struct mux_probe_stat {
    uint64_t min, max, sum, passes;   /* __dpa_thread_cycles() per pass */
};

struct mux_probe_state {
    /* Probe endpoint */
    uint64_t dpa_producer;
    uint64_t dpa_producer_comp;
    uint64_t dpa_dev;
    uint64_t mode;

    /* POLL */
    uint64_t num_rings;
    uint64_t passes;                  /* per (variant, R) */
    uint64_t cursor;                  /* next (variant, R) */
    uint64_t ring_buf_arr[MUX_PROBE_MAX_RINGS];
    struct mux_probe_stat stat[MUX_PROBE_VARIANTS][MUX_PROBE_R_STEPS];
    uint64_t cal_us, cal_cycles;      /* cycles in a timed 2 ms spin */
    uint64_t sink;

    /* FAULT: copy_len bytes from src i to dst + i * copy_len, i = iter & 1 */
    uint64_t src_mmap[2], src_addr[2];
    uint64_t dst_mmap, dst_addr;
    uint64_t copy_len, gap_us, timeout_us;
    uint64_t iter;
    uint64_t ok[2], err[2], first_err_iter[2], last_err_type[2];
    uint64_t credit_waits;            /* activations ended by an empty ARM consumer */
    uint64_t stalled_src;             /* 0, or 1 + the source whose CQE never came */
    /* After a stall: copies from the other source only, to see whether the
     * shared producer still completes work queued behind the stuck copy. */
    uint64_t stall_iter;
    uint64_t post_ok, post_err, post_timeouts, post_last_err_type;

    /* Control */
    volatile uint64_t stop;
    volatile uint64_t done;
};

_Static_assert(sizeof(struct mux_probe_state) % 8 == 0, "probe state holds 64-bit fields only");

#endif
