/* SDK callback ownership and send-buffer lifetime, without opening a device. */
#include <assert.h>
#include <stdio.h>
#include "../src/transport/host/comch_client.c"

static struct dmesh_comch_client control;
static int owner, messages, disconnects, consumers, freed, stopped;
static struct { const void *bytes; size_t len; union doca_data user; } task;
static doca_error_t allocation_result, submit_result;

struct doca_comch_client *doca_comch_client_get_client_ctx(const struct doca_comch_connection *connection)
{ assert(connection == control.connection); return control.cc_client; }
struct doca_ctx *doca_comch_client_as_ctx(struct doca_comch_client *client)
{ assert(client == control.cc_client); return (void *)client; }
doca_error_t doca_ctx_get_user_data(const struct doca_ctx *ctx, union doca_data *data)
{ assert(ctx == (void *)control.cc_client); data->ptr = &control; return DOCA_SUCCESS; }
doca_error_t doca_ctx_stop(struct doca_ctx *ctx)
{ assert(ctx == (void *)control.cc_client); ++stopped; return DOCA_SUCCESS; }
struct doca_task *doca_comch_task_send_as_task(struct doca_comch_task_send *send)
{ assert(send == (void *)&task); return (void *)send; }
doca_error_t doca_comch_client_task_send_alloc_init(struct doca_comch_client *client,
    struct doca_comch_connection *connection, const void *data, uint32_t len,
    struct doca_comch_task_send **out)
{
    assert(client == control.cc_client && connection == control.connection);
    if (allocation_result != DOCA_SUCCESS) return allocation_result;
    task.bytes = data; task.len = len; *out = (void *)&task;
    return DOCA_SUCCESS;
}
void doca_task_set_user_data(struct doca_task *send, union doca_data data)
{ assert(send == (void *)&task); task.user = data; }
doca_error_t doca_task_submit(struct doca_task *send)
{ assert(send == (void *)&task); return submit_result; }
void doca_task_free(struct doca_task *send)
{ assert(send == (void *)&task); ++freed; }
doca_error_t doca_task_get_status(const struct doca_task *send)
{ assert(send == (void *)&task); return DOCA_ERROR_IO_FAILED; }

static void message(void *arg, const uint8_t *data, size_t len)
{ assert(arg == &owner && len == 4 && !memcmp(data, "test", 4)); ++messages; }
static void disconnected(void *arg) { assert(arg == &owner); ++disconnects; }
static void consumer(void *arg, uint32_t id) { assert(arg == &owner && id == 42); ++consumers; }

int main(void)
{
    control = (struct dmesh_comch_client){
        .cc_client = (void *)&control, .connection = (void *)&owner,
        .owner = &owner, .message = message, .disconnected = disconnected,
        .consumer_arrived = consumer,
    };
    client_message_recv_callback(NULL, (uint8_t *)"test", 4, control.connection);
    client_consumer_arrived(NULL, control.connection, 42);
    assert(messages == 1 && consumers == 1);
    client_state_changed_callback((union doca_data){.ptr = &control}, (void *)&control,
                                  DOCA_CTX_STATE_STARTING, DOCA_CTX_STATE_RUNNING);
    assert(!control.peer_gone && !disconnects);
    client_state_changed_callback((union doca_data){.ptr = &control}, (void *)&control,
                                  DOCA_CTX_STATE_RUNNING, DOCA_CTX_STATE_STOPPING);
    assert(control.peer_gone && disconnects == 1);

    char bytes[] = "test";
    assert(dmesh_comch_client_send(&control, bytes, 4) == DOCA_SUCCESS);
    bytes[0] = 'X';
    assert(task.bytes != bytes && task.len == 4 && !memcmp(task.bytes, "test", 4));
    client_send_task_completion_callback((void *)&task, task.user, (union doca_data){.ptr = &control});
    assert(freed == 1);
    assert(dmesh_comch_client_send(&control, bytes, 4) == DOCA_SUCCESS);
    client_send_task_completion_err_callback((void *)&task, task.user, (union doca_data){.ptr = &control});
    assert(freed == 2 && stopped == 1 && disconnects == 2);
    submit_result = DOCA_ERROR_AGAIN;
    assert(dmesh_comch_client_send(&control, bytes, 4) == DOCA_ERROR_AGAIN && freed == 3);
    allocation_result = DOCA_ERROR_NO_MEMORY;
    assert(dmesh_comch_client_send(&control, bytes, 4) == DOCA_ERROR_NO_MEMORY && freed == 3);
    puts("comch_client_test: PASS");
    return 0;
}
