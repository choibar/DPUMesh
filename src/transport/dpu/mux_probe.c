/* Launcher and reader for the DPA multiplexing probe (mux_probe.h). */
#include "mux_probe.h"

#include <doca_build_config.h>

#ifdef DOCA_ARCH_DPU

#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <doca_buf_array.h>
#include <doca_comch_consumer.h>
#include <doca_comch_producer.h>
#include <doca_dpa.h>
#include <doca_log.h>
#include <doca_mmap.h>

#include "buffer.h"
#include "comch_msgq.h"
#include "dpa.h"
#include "dpa_common.h"
#include "dpa_probe.h"
#include "object.h"

DOCA_LOG_REGISTER(MUX_PROBE);

extern doca_dpa_func_t run_mux_probe;
extern doca_dpa_func_t thread_init_rpc;

struct dmesh_mux_probe {
    struct objects *objs;
    struct dmesh_doca_dpa_thread thread;
    struct dmesh_dpa_endpoint ep;
    struct doca_mmap *scratch_mmap;
    void *scratch;
    struct mux_probe_state state;   /* image sent at start, then the last read */
    int slots[MUX_PROBE_MAX_RINGS];
    int nslots;
    uint64_t arm_msgs[2];           /* FAULT: completion messages received, per source */
    uint64_t arm_other;             /* messages whose position names neither source */
};

/* FAULT: a copy's message reaches the ARM only once that copy has completed,
 * and its position names the source (i * copy_len). */
static void probe_received(void *owner, uint32_t pos, uint32_t len, uint32_t count)
{
    struct dmesh_mux_probe *p = owner;
    (void)len;
    (void)count;
    if (pos == 0)
        p->arm_msgs[0]++;
    else if (pos == p->state.copy_len)
        p->arm_msgs[1]++;
    else
        p->arm_other++;
}

static const char *const variant_names[MUX_PROBE_VARIANTS] = {
    "lookup", "cached", "same_ring", "inv_each",
};
static const uint32_t r_steps[MUX_PROBE_R_STEPS] = {1, 2, 4, 8, 16};

/* The native pool's thread creation with the probe entry and state size. */
static doca_error_t probe_thread_create(struct dmesh_doca_dpa_thread *t)
{
    doca_error_t result = DMESH_DPA_CALL(doca_dpa_mem_alloc(t->dpa, sizeof(struct mux_probe_state),
                                                            &t->local_storage));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_dpa_thread_create(t->dpa, &t->thread));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_dpa_thread_set_func_arg(t->thread, run_mux_probe, 0));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_dpa_thread_set_local_storage(t->thread, t->local_storage));
    if (result == DOCA_SUCCESS && t->pin_eu != 0) {
        struct doca_dpa_eu_affinity *affinity = NULL; /* kept for the thread's lifetime */
        result = DMESH_DPA_CALL(doca_dpa_eu_affinity_create(t->dpa, &affinity));
        if (result == DOCA_SUCCESS)
            result = DMESH_DPA_CALL(doca_dpa_eu_affinity_set(affinity, t->pin_eu - 1));
        if (result == DOCA_SUCCESS)
            result = DMESH_DPA_CALL(doca_dpa_thread_set_affinity(t->thread, affinity));
    }
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_dpa_thread_start(t->thread));
    if (result == DOCA_SUCCESS)
        t->started = true;
    return result;
}

static doca_error_t fill_poll(struct dmesh_mux_probe *p, const struct dmesh_mux_probe_config *cfg)
{
    struct mux_probe_state *s = &p->state;

    for (int i = 0; i < cfg->nslots; i++) {
        struct dmesh_conn *conn = &p->objs->conns[cfg->slots[i]];
        doca_dpa_dev_buf_arr_t handle;
        doca_error_t result;

        if (conn->buf_arr == NULL) {
            DOCA_LOG_ERR("slot %d has no forward ring", cfg->slots[i]);
            return DOCA_ERROR_BAD_STATE;
        }
        result = DMESH_DPA_CALL(doca_buf_arr_get_dpa_handle(conn->buf_arr, &handle));
        if (result != DOCA_SUCCESS)
            return result;
        s->ring_buf_arr[i] = handle;
    }
    s->num_rings = (uint64_t)cfg->nslots;
    s->passes = cfg->passes;
    return DOCA_SUCCESS;
}

static doca_error_t fill_fault(struct dmesh_mux_probe *p, const struct dmesh_mux_probe_config *cfg)
{
    struct mux_probe_state *s = &p->state;
    doca_dpa_dev_mmap_t handle;
    doca_error_t result;

    for (int i = 0; i < 2; i++) {
        struct dmesh_conn *conn = &p->objs->conns[cfg->slots[i]];

        if (conn->sndbuf.mmap == NULL || conn->sndbuf.size < cfg->copy_len) {
            DOCA_LOG_ERR("slot %d has no host send buffer of %u bytes", cfg->slots[i], cfg->copy_len);
            return DOCA_ERROR_BAD_STATE;
        }
        result = DMESH_DPA_CALL(doca_mmap_dev_get_dpa_handle(conn->sndbuf.mmap, p->objs->dev, &handle));
        if (result != DOCA_SUCCESS)
            return result;
        s->src_mmap[i] = handle;
        s->src_addr[i] = (uint64_t)(uintptr_t)conn->sndbuf.buf;
    }

    /* Destination: a private DPU buffer, so the probe never writes into a
     * connection's staging while its own thread fills it. */
    result = alloc_buffer_and_set_mmap(&p->scratch_mmap, p->objs->dev, &p->scratch,
                                       2 * (size_t)cfg->copy_len, DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    if (result != DOCA_SUCCESS)
        return result;
    result = DMESH_DPA_CALL(doca_mmap_dev_get_dpa_handle(p->scratch_mmap, p->objs->dev, &handle));
    if (result != DOCA_SUCCESS)
        return result;
    s->dst_mmap = handle;
    s->dst_addr = (uint64_t)(uintptr_t)p->scratch;
    s->copy_len = cfg->copy_len;
    s->gap_us = cfg->gap_us;
    s->timeout_us = cfg->timeout_us;
    return DOCA_SUCCESS;
}

struct dmesh_mux_probe *dmesh_mux_probe_start(struct objects *objs, const struct dmesh_mux_probe_config *cfg)
{
    struct dmesh_mux_probe *p;
    doca_dpa_dev_completion_t producer_comp;
    doca_dpa_dev_comch_producer_t producer;
    doca_dpa_dev_comch_consumer_t consumer;
    struct comch_msg kick = {0};
    uint64_t rpc_ret = 0;
    doca_error_t result;

    if (objs == NULL || cfg == NULL || objs->dpa_pool == NULL || objs->dpa_pool->dpa == NULL ||
        cfg->nslots < 1 || cfg->nslots > (int)MUX_PROBE_MAX_RINGS ||
        (cfg->mode == MUX_PROBE_FAULT && (cfg->nslots != 2 || cfg->copy_len == 0)) ||
        (cfg->mode == MUX_PROBE_POLL && cfg->passes == 0) ||
        (cfg->mode != MUX_PROBE_POLL && cfg->mode != MUX_PROBE_FAULT))
        return NULL;
    p = calloc(1, sizeof(*p));
    if (p == NULL)
        return NULL;
    p->objs = objs;
    p->nslots = cfg->nslots;
    memcpy(p->slots, cfg->slots, sizeof(int) * (size_t)cfg->nslots);
    p->thread.dpa = objs->dpa_pool->dpa;
    p->thread.pin_eu = cfg->pin_eu;
    p->state.mode = cfg->mode;

    result = cfg->mode == MUX_PROBE_POLL ? fill_poll(p, cfg) : fill_fault(p, cfg);
    if (result == DOCA_SUCCESS)
        result = probe_thread_create(&p->thread);
    if (result == DOCA_SUCCESS) {
        /* Its own msgq pair on the worker's consumer PE, as each flow has. */
        p->ep.pe = objs->consumer_pe;
        p->ep.dpa_thread = &p->thread;
        result = dmesh_dpa_endpoint_init_comch(&p->ep, objs->dev);
        if (result == DOCA_SUCCESS) {
            p->ep.dpa_comch->owner = p;
            p->ep.dpa_comch->received = probe_received;
        }
    }
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_dpa_completion_get_dpa_handle(p->ep.dpa_comch->producer_comp, &producer_comp));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_comch_producer_get_dpa_handle(p->ep.dpa_comch->recv.producer, &producer));
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_comch_consumer_get_dpa_handle(p->ep.dpa_comch->send.consumer, &consumer));
    if (result == DOCA_SUCCESS) {
        p->state.dpa_producer = producer;
        p->state.dpa_producer_comp = producer_comp;
        result = DMESH_DPA_CALL(doca_dpa_rpc(p->thread.dpa, thread_init_rpc, &rpc_ret, consumer,
                                             (uint32_t)CC_DPA_MAX_MSG_NUM, (uint64_t)0));
    }
    if (result == DOCA_SUCCESS)
        result = DMESH_DPA_CALL(doca_dpa_h2d_memcpy(p->thread.dpa, p->thread.local_storage,
                                                    &p->state, sizeof(p->state)));
    if (result == DOCA_SUCCESS) {
        p->thread.running = true;
        result = DMESH_DPA_CALL(doca_dpa_thread_run(p->thread.thread));
    }
    /* A run thread starts on its first completion: this message is the kick
     * the per-flow path sends as DMA_REQ. */
    if (result == DOCA_SUCCESS)
        result = dmesh_doca_dpa_msgq_send(&p->ep.dpa_comch->send, &kick, sizeof(kick));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Probe start failed: %s", doca_error_get_name(result));
        return NULL; /* objects stay allocated until exit */
    }
    DOCA_LOG_INFO("Probe started: mode=%u slots=%d eu=%d", cfg->mode, cfg->nslots, (int)cfg->pin_eu - 1);
    return p;
}

int dmesh_mux_probe_read(struct dmesh_mux_probe *p)
{
    if (p == NULL)
        return -1;
    if (DMESH_DPA_CALL(doca_dpa_d2h_memcpy(p->thread.dpa, &p->state, p->thread.local_storage,
                                           sizeof(p->state))) != DOCA_SUCCESS)
        return -1;
    if (dmesh_doca_dpa_failed(p->thread.dpa))
        return -1;
    return p->state.done ? 1 : 0;
}

void dmesh_mux_probe_stop(struct dmesh_mux_probe *p)
{
    uint64_t one = 1;

    if (p == NULL)
        return;
    (void)DMESH_DPA_CALL(doca_dpa_h2d_memcpy(p->thread.dpa,
                                             p->thread.local_storage + offsetof(struct mux_probe_state, stop),
                                             &one, sizeof(one)));
}

static void print_poll(const struct dmesh_mux_probe *p, FILE *out)
{
    const struct mux_probe_state *s = &p->state;
    double cyc_per_us = s->cal_us ? (double)s->cal_cycles / (double)s->cal_us : 0.0;

    fprintf(out, "MUX_PROBE poll rings=%llu passes=%llu cycles_per_us=%.1f slots=",
            (unsigned long long)s->num_rings, (unsigned long long)s->passes, cyc_per_us);
    for (int i = 0; i < p->nslots; i++)
        fprintf(out, "%s%d", i ? "," : "", p->slots[i]);
    fprintf(out, "\nMUX_PROBE %-10s %3s %10s %10s %10s %9s %9s\n",
            "variant", "R", "min_cyc", "avg_cyc", "max_cyc", "avg_us", "us/ring");
    for (uint32_t v = 0; v < MUX_PROBE_VARIANTS; v++) {
        for (uint32_t k = 0; k < MUX_PROBE_R_STEPS; k++) {
            const struct mux_probe_stat *st = &s->stat[v][k];
            double avg;

            if (st->passes == 0)
                continue;
            avg = (double)st->sum / (double)st->passes;
            fprintf(out, "MUX_PROBE %-10s %3u %10llu %10.0f %10llu %9.3f %9.3f\n",
                    variant_names[v], r_steps[k], (unsigned long long)st->min, avg,
                    (unsigned long long)st->max, cyc_per_us ? avg / cyc_per_us : 0.0,
                    cyc_per_us ? avg / cyc_per_us / r_steps[k] : 0.0);
        }
    }
}

static void print_fault(const struct dmesh_mux_probe *p, FILE *out)
{
    const struct mux_probe_state *s = &p->state;

    fprintf(out, "MUX_PROBE fault iter=%llu", (unsigned long long)s->iter);
    for (int i = 0; i < 2; i++)
        fprintf(out, " | slot %d ok=%llu err=%llu first_err_iter=%lld last_err_type=%llu",
                p->slots[i], (unsigned long long)s->ok[i], (unsigned long long)s->err[i],
                s->err[i] ? (long long)s->first_err_iter[i] : -1LL,
                (unsigned long long)s->last_err_type[i]);
    fprintf(out, " | arm_msgs slot %d=%llu slot %d=%llu other=%llu | credit_waits=%llu",
            p->slots[0], (unsigned long long)p->arm_msgs[0], p->slots[1], (unsigned long long)p->arm_msgs[1],
            (unsigned long long)p->arm_other, (unsigned long long)s->credit_waits);
    if (s->stalled_src)
        fprintf(out, " | stalled_on=slot %d at_iter=%llu, then other source: ok=%llu err=%llu timeouts=%llu last_err_type=%llu\n",
                p->slots[s->stalled_src - 1], (unsigned long long)s->stall_iter,
                (unsigned long long)s->post_ok, (unsigned long long)s->post_err,
                (unsigned long long)s->post_timeouts, (unsigned long long)s->post_last_err_type);
    else
        fprintf(out, " | stalled_on=none\n");
}

void dmesh_mux_probe_print(const struct dmesh_mux_probe *p, FILE *out)
{
    if (p == NULL)
        return;
    if (p->state.mode == MUX_PROBE_POLL)
        print_poll(p, out);
    else
        print_fault(p, out);
}

#else /* !DOCA_ARCH_DPU */

struct dmesh_mux_probe *dmesh_mux_probe_start(struct objects *objs, const struct dmesh_mux_probe_config *cfg)
{
    (void)objs;
    (void)cfg;
    return NULL;
}
int dmesh_mux_probe_read(struct dmesh_mux_probe *p) { (void)p; return -1; }
void dmesh_mux_probe_stop(struct dmesh_mux_probe *p) { (void)p; }
void dmesh_mux_probe_print(const struct dmesh_mux_probe *p, FILE *out) { (void)p; (void)out; }

#endif
