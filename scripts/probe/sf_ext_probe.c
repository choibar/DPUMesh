/* Host DPA on an SF: the firmware refuses a DPA process created on the SF
 * itself (see sf_dpa_probe), so the process is created on the PF and extended
 * to the SF with doca_dpa_device_extend. Everything the host-dpa reverse path needs is
 * then created on the SF's extended context: a thread, its doca_dpa_completion
 * (attached before the comch consumer completion, as the channel layer does), a
 * DPA-reachable mmap and buf array, the two DPA <-> CPU msgqs and the
 * consumer completion with the channel layer's parameters. HOLD=<s> keeps it all alive
 * to run a second copy against another SF.
 *
 *   sf_ext_probe [pf ibdev] [sf ibdev] [second sf ibdev]     default mlx5_0 mlx5_2
 *   With a second SF the same process extends its one DPA process to both
 *   SFs and builds a thread + consumer completion on each.
 *   PROBE_SDK_LOG=debug                    SDK trace (shows the devx syndrome)
 *   PROBE_SKIP_CC1=1                       no consumer completion on the first SF
 *   PROBE_SKIP_MSGQ1=1                     no msgqs (and no consumer completion) on the first SF
 *   PROBE_SKIP_THREAD1=1                   first SF gets only the extension (no thread, completions, msgqs)
 *   PROBE_SKIP_COMP1=1                     first SF: thread but no doca_dpa_completion
 *   PROBE_NOSTART_THREAD1=1                first SF: thread created but not started
 *   PROBE_RELEASE_CC1=1                    destroy the first SF consumer completion before the second SF
 *   PROBE_EU1=<id> / PROBE_EU2=<id>        pin the first / second SF thread to that EU (doca_dpa_thread_set_affinity)
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <doca_dev.h>
#include <doca_dpa.h>
#include <doca_log.h>
#include <doca_mmap.h>
#include <doca_buf_array.h>
#include <doca_comch_msgq.h>
#include <doca_comch_consumer.h>
#include <doca_error.h>
#include "common.h"
#include "dpa_common.h"
extern struct doca_dpa_app *DPU_mesh_dpa_app;
extern doca_dpa_func_t run_dma_manager;
#define STEP(x) do { doca_error_t _r = (x); printf("%-62s %s\n", #x, doca_error_get_name(_r)); if (_r != DOCA_SUCCESS) { fails++; } } while (0)
int main(int argc, char **argv)
{
    const char *pf = argc > 1 ? argv[1] : "mlx5_0", *sf = argc > 2 ? argv[2] : "mlx5_2";
    const char *sf2 = argc > 3 ? argv[3] : NULL;
    struct doca_dev *sfdev2 = NULL; struct doca_dpa *ext2 = NULL; struct doca_dpa_thread *th2 = NULL;
    struct doca_dpa_completion *comp2 = NULL; struct doca_comch_consumer_completion *cc2 = NULL;
    struct doca_comch_msgq *q3 = NULL; doca_dpa_dev_uintptr_t mem2 = 0;
    struct doca_dev *pfdev = NULL, *sfdev = NULL;
    struct doca_dpa *base = NULL, *ext = NULL;
    struct doca_dpa_thread *th = NULL;
    struct doca_dpa_completion *comp = NULL;
    struct doca_comch_consumer_completion *cc = NULL;
    struct doca_mmap *mm = NULL; struct doca_buf_arr *ba = NULL;
    struct doca_comch_msgq *q = NULL, *q2 = NULL;
    doca_dpa_dev_uintptr_t mem = 0; doca_dpa_dev_mmap_t mh = 0; doca_dpa_dev_buf_arr_t bh = 0;
    void *buf = NULL; uint16_t vhca = 0; int fails = 0;

    doca_log_backend_create_standard();
    struct doca_log_backend *sdk = NULL;
    const char *lvl = getenv("PROBE_SDK_LOG");
    if (doca_log_backend_create_with_file_sdk(stderr, &sdk) == DOCA_SUCCESS)
        doca_log_backend_set_sdk_level(sdk, lvl && strcmp(lvl, "debug") == 0 ? DOCA_LOG_LEVEL_DEBUG : DOCA_LOG_LEVEL_WARNING);

    STEP(open_doca_device_with_ibdev_name((const uint8_t *)pf, strlen(pf), NULL, &pfdev));
    STEP(open_doca_device_with_ibdev_name((const uint8_t *)sf, strlen(sf), NULL, &sfdev));
    if (!pfdev || !sfdev) return 1;
    doca_devinfo_get_vhca_id(doca_dev_as_devinfo(sfdev), &vhca);
    printf("== DPA process on %s, extended to %s (vhca %u)\n", pf, sf, vhca);
    STEP(doca_dpa_create(pfdev, &base));
    STEP(doca_dpa_set_app(base, DPU_mesh_dpa_app));
    STEP(doca_dpa_start(base));
    STEP(doca_dpa_device_extend(base, sfdev, &ext));
    if (!ext) { printf("no extended context, stopping\n"); return 1; }
    { unsigned int nb = 0, ne = 0, pc = 0;
      doca_dpa_get_total_num_eus_available(base, &nb); doca_dpa_get_total_num_eus_available(ext, &ne);
      doca_dpa_get_num_eus_per_core(base, &pc);
      printf("  EUs available: base %u, extended %u (eus/core %u)\n", nb, ne, pc); }

    int skip_thread1 = getenv("PROBE_SKIP_THREAD1") != NULL;
    int skip_msgq1 = skip_thread1 || getenv("PROBE_SKIP_MSGQ1") != NULL;
    int skip_cc1 = skip_msgq1 || getenv("PROBE_SKIP_CC1") != NULL;
    printf("== thread and completions on the SF%s\n", skip_thread1 ? " (skipped)" : "");
    if (!skip_thread1) {
    STEP(doca_dpa_mem_alloc(ext, 4096, &mem));
    STEP(doca_dpa_thread_create(ext, &th));
    if (th) STEP(doca_dpa_thread_set_func_arg(th, run_dma_manager, 0));
    if (th) STEP(doca_dpa_thread_set_local_storage(th, mem));
    if (th && getenv("PROBE_EU1")) {
        struct doca_dpa_eu_affinity *af = NULL;
        printf("  pinning the first SF thread to EU %s\n", getenv("PROBE_EU1"));
        STEP(doca_dpa_eu_affinity_create(ext, &af));
        if (af) STEP(doca_dpa_eu_affinity_set(af, (unsigned)atoi(getenv("PROBE_EU1"))));
        if (af) STEP(doca_dpa_thread_set_affinity(th, af));
    }
    if (th && !getenv("PROBE_NOSTART_THREAD1")) STEP(doca_dpa_thread_start(th));
    else printf("  thread not started\n");
    if (getenv("PROBE_SKIP_COMP1")) printf("  doca_dpa_completion skipped\n");
    else {
    STEP(doca_dpa_completion_create(ext, 512, &comp));
    if (comp && th) STEP(doca_dpa_completion_set_thread(comp, th));
    if (comp) STEP(doca_dpa_completion_start(comp));
    }
    }

    printf("== SF-registered host memory reachable from the DPA\n");
    buf = aligned_alloc(4096, 1 << 20); memset(buf, 0, 1 << 20);
    STEP(doca_mmap_create(&mm));
    STEP(doca_mmap_add_dev(mm, sfdev));
    STEP(doca_mmap_set_permissions(mm, DOCA_ACCESS_FLAG_LOCAL_READ_WRITE | DOCA_ACCESS_FLAG_PCI_READ_WRITE));
    STEP(doca_mmap_set_memrange(mm, buf, 1 << 20));
    STEP(doca_mmap_start(mm));
    STEP(doca_mmap_dev_get_dpa_handle(mm, sfdev, &mh));
    STEP(doca_buf_arr_create(16, &ba));
    if (ba) STEP(doca_buf_arr_set_target_dpa(ba, ext));
    if (ba) STEP(doca_buf_arr_set_params(ba, mm, 64, 0));
    if (ba) STEP(doca_buf_arr_start(ba));
    if (ba) STEP(doca_buf_arr_get_dpa_handle(ba, &bh));

    printf("== DPA <-> CPU msgqs on the SF (one per direction, as dpa.c does)%s\n", skip_msgq1 ? " (skipped)" : "");
    if (!skip_msgq1) {
    STEP(doca_comch_msgq_create(sfdev, &q));
    if (q) STEP(doca_comch_msgq_set_max_num_consumers(q, 1));
    if (q) STEP(doca_comch_msgq_set_max_num_producers(q, 1));
    if (q) STEP(doca_comch_msgq_set_dpa_consumer(q, ext));
    if (q) STEP(doca_comch_msgq_start(q));
    STEP(doca_comch_msgq_create(sfdev, &q2));
    if (q2) STEP(doca_comch_msgq_set_max_num_consumers(q2, 1));
    if (q2) STEP(doca_comch_msgq_set_max_num_producers(q2, 1));
    if (q2) STEP(doca_comch_msgq_set_dpa_producer(q2, ext));
    if (q2) STEP(doca_comch_msgq_start(q2));
    }

    printf("== consumer completion on the SF thread (wire parameters: 512 entries, %zu B imm)\n", sizeof(struct comch_msg));
    if (skip_cc1) printf("  skipped\n");
    else STEP(doca_comch_consumer_completion_create(&cc));
    if (cc) STEP(doca_comch_consumer_completion_set_max_num_recv(cc, 64));
    if (cc) STEP(doca_comch_consumer_completion_set_imm_data_len(cc, sizeof(struct comch_msg)));
    if (cc && th) STEP(doca_comch_consumer_completion_set_dpa_thread(cc, th));
    if (cc) STEP(doca_comch_consumer_completion_start(cc));

    if (sf2) {
        if (cc && getenv("PROBE_RELEASE_CC1")) {
            printf("== releasing the first SF consumer completion first (PROBE_RELEASE_CC1)\n");
            doca_comch_consumer_completion_stop(cc); doca_comch_consumer_completion_destroy(cc); cc = NULL;
        }
        STEP(open_doca_device_with_ibdev_name((const uint8_t *)sf2, strlen(sf2), NULL, &sfdev2));
        if (sfdev2) {
            doca_devinfo_get_vhca_id(doca_dev_as_devinfo(sfdev2), &vhca);
            printf("== same process: extend to a second SF %s (vhca %u)\n", sf2, vhca);
            STEP(doca_dpa_device_extend(base, sfdev2, &ext2));
        }
        if (ext2) {
            STEP(doca_dpa_mem_alloc(ext2, 4096, &mem2));
            STEP(doca_dpa_thread_create(ext2, &th2));
            if (th2) STEP(doca_dpa_thread_set_func_arg(th2, run_dma_manager, 0));
            if (th2) STEP(doca_dpa_thread_set_local_storage(th2, mem2));
            if (th2 && getenv("PROBE_EU2")) {
                struct doca_dpa_eu_affinity *af2 = NULL;
                printf("  pinning the second SF thread to EU %s\n", getenv("PROBE_EU2"));
                STEP(doca_dpa_eu_affinity_create(ext2, &af2));
                if (af2) STEP(doca_dpa_eu_affinity_set(af2, (unsigned)atoi(getenv("PROBE_EU2"))));
                if (af2) STEP(doca_dpa_thread_set_affinity(th2, af2));
            }
            if (th2) STEP(doca_dpa_thread_start(th2));
            STEP(doca_dpa_completion_create(ext2, 512, &comp2));
            if (comp2 && th2) STEP(doca_dpa_completion_set_thread(comp2, th2));
            if (comp2) STEP(doca_dpa_completion_start(comp2));
            STEP(doca_comch_msgq_create(sfdev2, &q3));
            if (q3) STEP(doca_comch_msgq_set_max_num_consumers(q3, 1));
            if (q3) STEP(doca_comch_msgq_set_max_num_producers(q3, 1));
            if (q3) STEP(doca_comch_msgq_set_dpa_consumer(q3, ext2));
            if (q3) STEP(doca_comch_msgq_start(q3));
            STEP(doca_comch_consumer_completion_create(&cc2));
            if (cc2) STEP(doca_comch_consumer_completion_set_max_num_recv(cc2, 512));
            if (cc2) STEP(doca_comch_consumer_completion_set_imm_data_len(cc2, sizeof(struct comch_msg)));
            if (cc2 && th2) STEP(doca_comch_consumer_completion_set_dpa_thread(cc2, th2));
            if (cc2) STEP(doca_comch_consumer_completion_start(cc2));
        }
    }

    if (getenv("HOLD")) { printf("holding %s s\n", getenv("HOLD")); fflush(stdout); sleep(atoi(getenv("HOLD"))); }

    printf("== teardown\n");
    if (cc2) { doca_comch_consumer_completion_stop(cc2); doca_comch_consumer_completion_destroy(cc2); }
    if (q3) { doca_comch_msgq_stop(q3); doca_comch_msgq_destroy(q3); }
    if (comp2) { doca_dpa_completion_stop(comp2); doca_dpa_completion_destroy(comp2); }
    if (th2) doca_dpa_thread_destroy(th2);
    if (mem2) doca_dpa_mem_free(ext2, mem2);
    if (ext2) STEP(doca_dpa_destroy(ext2));
    if (cc) { doca_comch_consumer_completion_stop(cc); doca_comch_consumer_completion_destroy(cc); }
    if (q2) { doca_comch_msgq_stop(q2); doca_comch_msgq_destroy(q2); }
    if (q) { doca_comch_msgq_stop(q); doca_comch_msgq_destroy(q); }
    if (ba) doca_buf_arr_destroy(ba);
    if (mm) { doca_mmap_stop(mm); doca_mmap_destroy(mm); }
    if (comp) { doca_dpa_completion_stop(comp); doca_dpa_completion_destroy(comp); }
    if (th) doca_dpa_thread_destroy(th);
    if (mem) doca_dpa_mem_free(ext, mem);
    STEP(doca_dpa_destroy(ext));
    STEP(doca_dpa_destroy(base));
    free(buf);
    printf("RESULT: %d failing step(s)\n", fails);
    return fails != 0;
}
