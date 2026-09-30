#include <assert.h>
#include "../linkerd2-proxy/linkerd/doca/src/shim.c"

static int accepted = 8192;
static unsigned submissions;
int dmesh_dma_push_staged(struct dmesh_conn *conn, uint32_t pos, uint32_t len)
{
    (void)pos;
    ++submissions;
    assert(conn->push_state == 0);
    if (accepted > 0) {
        assert((uint32_t)accepted <= len);
        conn->push_state = 1;
    }
    return accepted;
}
struct dma_desc *get_next_dma_desc(struct dma_ring *ring)
{ (void)ring; abort(); }
void commit_dma_desc(struct dma_ring *ring)
{ (void)ring; abort(); }

int main(void)
{
    struct objects *objs = calloc(1, sizeof(*objs));
    assert(objs);
    struct dmesh_conn *conn = &objs->conns[0];
    conn->flow.mode = DMESH_FLOW_MODE_INGRESS_PUSH;
    conn->reverse_exported = true;
    conn->tx_staging = (void *)1;
    conn->state = DMESH_CONN_RUNNING;
    assert(dmesh_doca_conn_send_staged(objs, 0, 0, 16384) == 0);
    assert(submissions == 1);
    assert(dmesh_doca_conn_send_staged(objs, 0, 0, 16384) == 0);
    assert(submissions == 1); /* retry must not submit the same bytes twice */
    conn->push_state = 2; /* data landed; descriptor is still in flight */
    assert(dmesh_doca_conn_send_staged(objs, 0, 0, 16384) == 0);
    conn->push_seq = 1; conn->push_state = 0;
    assert(dmesh_doca_conn_send_staged(objs, 0, 0, 16384) == 8192);
    assert(dmesh_doca_conn_send_staged(objs, 0, 8192, 8192) == 0);
    assert(submissions == 2);
    conn->dma_closing = true;
    assert(dmesh_doca_conn_send_staged(objs, 0, 8192, 8192) < 0);
    assert(conn->push_unreported_len == 8192); /* failure never releases custody */
    free(objs);
    puts("tx_staging_custody_test: source retained until DMA completion, retries and errors: PASS");
}
