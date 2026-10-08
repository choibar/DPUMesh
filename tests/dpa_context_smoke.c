/* Opt-in hardware lifecycle test. No thread_run, traffic or device configuration.
 * Build/run instructions: docs/2026-09-28_dpu-shared-dpa-context.md. */
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <doca_log.h>
#include "common.h"
#include "object.h"
#include "dpa.h"
#include "dpa_common.h"
static struct objects worker[2];
static doca_error_t status[2];
static void *initialize(void *arg)
{
    size_t i = (size_t)arg;
    status[i] = init_dpa_objects(&worker[i]);
    if (status[i] == DOCA_SUCCESS) status[i] = dmesh_dpa_thread_pool_init(&worker[i]);
    return NULL;
}
int main(int argc, char **argv)
{
    if (argc != 2) return 2;
    doca_log_backend_create_standard();
    for (unsigned i=0;i<2;i++) {
        doca_error_t e = open_doca_device_with_pci(argv[1], NULL, &worker[i].dev);
        if (e != DOCA_SUCCESS) { fprintf(stderr,"open: %s\n",doca_error_get_name(e)); return 1; }
    }
    pthread_t t[2];
    for (size_t i=0;i<2;i++) assert(pthread_create(&t[i],NULL,initialize,(void *)i)==0);
    for (unsigned i=0;i<2;i++) pthread_join(t[i],NULL);
    for (unsigned i=0;i<2;i++) {
        if (status[i]!=DOCA_SUCCESS) { fprintf(stderr,"worker %u: %s\n",i,doca_error_get_name(status[i])); return 1; }
    }
    assert(worker[0].dpa_pool->dpa == worker[1].dpa_pool->dpa);
    printf("PASS: two workers share context %p, 32 prepared threads each\n",(void *)worker[0].dpa_pool->dpa);
    assert(cleanup_objects(&worker[0])==DOCA_SUCCESS);
    doca_dpa_dev_uintptr_t ptr=0;
    struct doca_dpa *dpa=worker[1].dpa_pool->dpa;
    assert(DMESH_DPA_CALL(doca_dpa_mem_alloc(dpa, sizeof(uint64_t), &ptr))==DOCA_SUCCESS);
    uint64_t sent=0x1234,received=0;
    assert(DMESH_DPA_CALL(doca_dpa_h2d_memcpy(dpa,ptr,&sent,sizeof(sent)))==DOCA_SUCCESS);
    assert(DMESH_DPA_CALL(doca_dpa_d2h_memcpy(dpa,&received,ptr,sizeof(received)))==DOCA_SUCCESS);
    assert(received==sent);
    assert(DMESH_DPA_CALL(doca_dpa_mem_free(dpa,ptr))==DOCA_SUCCESS);
    puts("PASS: worker 1 can use DPA after worker 0 cleanup");
    assert(cleanup_objects(&worker[1])==DOCA_SUCCESS);
    puts("PASS: final worker destroys threads/context/device references");
    return 0;
}
