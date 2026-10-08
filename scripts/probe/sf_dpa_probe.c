/* Can a host SF device host a DPA process + thread? (base context, EU partition) */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <doca_dev.h>
#include <doca_dpa.h>
#include <doca_log.h>
#include <doca_error.h>
#include "common.h"
#include "dpa_common.h"
extern struct doca_dpa_app *DPU_mesh_dpa_app;
extern doca_dpa_func_t run_dma_manager;
#define CK(x) do { doca_error_t _r = (x); printf("%-40s %s\n", #x, doca_error_get_name(_r)); if (_r) return 1; } while (0)
int main(int argc, char **argv)
{
    const char *ibdev = argc > 1 ? argv[1] : "mlx5_2";
    struct doca_dev *dev = NULL; struct doca_dpa *dpa = NULL; struct doca_dpa_thread *th = NULL;
    doca_dpa_dev_uintptr_t arg = 0; struct dpa_thread_ctx targ = {0};
    uint16_t vhca = 0; enum doca_pci_func_type ft;
    doca_log_backend_create_standard();
    CK(open_doca_device_with_ibdev_name((const uint8_t *)ibdev, strlen(ibdev), NULL, &dev));
    doca_devinfo_get_pci_func_type(doca_dev_as_devinfo(dev), &ft);
    doca_devinfo_get_vhca_id(doca_dev_as_devinfo(dev), &vhca);
    printf("device %s func_type=%d vhca_id=%u dpa_supported=%s\n", ibdev, ft, vhca,
           doca_error_get_name(doca_dpa_cap_is_supported(doca_dev_as_devinfo(dev))));
    CK(doca_dpa_create(dev, &dpa));
    CK(doca_dpa_set_app(dpa, DPU_mesh_dpa_app));
    CK(doca_dpa_start(dpa));
    CK(doca_dpa_mem_alloc(dpa, sizeof(targ), &arg));
    CK(doca_dpa_h2d_memcpy(dpa, arg, &targ, sizeof(targ)));
    CK(doca_dpa_thread_create(dpa, &th));
    CK(doca_dpa_thread_set_func_arg(th, run_dma_manager, 0));
    CK(doca_dpa_thread_set_local_storage(th, arg));
    CK(doca_dpa_thread_start(th));
    printf("DPA process + thread OK on %s (not run: kernel would spin on an empty ring)\n", ibdev);
    doca_dpa_thread_destroy(th); doca_dpa_mem_free(dpa, arg); doca_dpa_destroy(dpa); doca_dev_close(dev);
    return 0;
}
