/* Execute the production shim; replace only its synchronous DPA copy. */
#include <assert.h>
#include "../linkerd2-proxy/linkerd/doca/src/shim.c"

pthread_mutex_t dmesh_dpa_sdk_mutex = PTHREAD_MUTEX_INITIALIZER;

static unsigned copies;
static uint32_t published;
static doca_error_t copy_error;

doca_error_t doca_dpa_h2d_memcpy(struct doca_dpa *dpa,
                               doca_dpa_dev_uintptr_t dst, void *src, size_t size)
{
    (void)dpa;
    assert(dst == 4096 + offsetof(struct dpa_thread_ctx, rx_consumed_pos));
    assert(size == sizeof(published));
    /* The copy is an SDK call: it runs under the process-wide DPA lock. */
    assert(pthread_mutex_trylock(&dmesh_dpa_sdk_mutex) != 0);
    ++copies;
    if (copy_error) return copy_error;
    memcpy(&published, src, size);
    return DOCA_SUCCESS;
}

int main(void)
{
    struct objects *objs = calloc(1, sizeof(*objs));
    assert(objs);
    struct dmesh_doca_dpa_thread thread = {.thread = (void *)1, .local_storage = 4096};
    struct dmesh_conn *conn = &objs->conns[0];
    assert(dmesh_doca_conn_update_dpu_rx_consumed_pos(objs, 0, 10) == DOCA_ERROR_BAD_STATE);
    conn->dpa_thread = &thread;
    assert(dmesh_doca_conn_update_dpu_rx_consumed_pos(objs, 0, 65535) == DOCA_SUCCESS);
    assert(copies == 0);
    assert(dmesh_doca_conn_update_dpu_rx_consumed_pos(objs, 0, 65536) == DOCA_SUCCESS);
    assert(copies == 1 && published == 65536);
    /* A failed update does not return staging space; the retry must copy
     * again instead of pretending the position reached the DPA. */
    copy_error = DOCA_ERROR_UNEXPECTED;
    assert(dmesh_doca_conn_update_dpu_rx_consumed_pos(objs, 0, 131072) == (int32_t)copy_error);
    assert(conn->rx_consumed_pos_published == 65536);
    copy_error = DOCA_SUCCESS;
    assert(dmesh_doca_conn_update_dpu_rx_consumed_pos(objs, 0, 131072) == DOCA_SUCCESS);
    assert(copies == 3 && published == 131072);
    /* Keep consuming small segments through several full staging wraps. The
     * withheld bytes never cost the gate its descriptor and wrap padding. */
    uint32_t position = 131072;
    for (unsigned i = 0; i < 10000; ++i) {
        position = (position + 1024) % BUFFER_SIZE;
        assert(dmesh_doca_conn_update_dpu_rx_consumed_pos(objs, 0, position) == DOCA_SUCCESS);
        uint32_t held = (position + BUFFER_SIZE - published) % BUFFER_SIZE;
        assert(held < DMESH_RX_CONSUMED_POS_BATCH);
        assert(held + 2u * 8064u < BUFFER_SIZE);
    }
    assert(copies == 3 + 10000 / 64);
    free(objs);
    puts("rx_consumed_pos_test: batching, SDK lock, failure retry and staging wraps: PASS");
}
