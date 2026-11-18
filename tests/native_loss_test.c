/* A lost transport session over the memory carrier: by default the process
 * exits with DMESH_EXIT_LOST when the channel's poller first sees it; with
 * DPUMESH_EXIT_ON_LOSS=0 the channel's and the QPs' fds stay readable, accepts
 * and polls report the error after the events that landed before it, and dials
 * fail. */
#include <assert.h>
#include <poll.h>
#include <stdio.h>
#include <sys/wait.h>
#include "../src/core/dmesh_core.c"
#include "support/native_memory_transport.c"

static int readable(int fd)
{
    struct pollfd p = {.fd = fd, .events = POLLIN};
    return poll(&p, 1, 0) == 1;
}

/* A server whose channel sleeps with nothing to accept. */
static dmesh_channel_t *idle_server(void)
{
    setenv("DPUMESH_SERVICE", "echo:80", 1);
    dmesh_channel_t *ch = dmesh_create_channel();
    assert(ch);
    unsetenv("DPUMESH_SERVICE");
    assert(dmesh_channel_fd(ch) >= 0);
    assert(dmesh_accept(ch) == NULL && errno == EAGAIN);
    return ch;
}

int main(void)
{
    /* Default: the first accept after the loss ends the process. */
    pid_t child = fork();
    assert(child >= 0);
    if (child == 0) {
        dmesh_channel_t *ch = idle_server();
        test_native_lose(ECONNRESET);
        (void)dmesh_accept(ch);
        _exit(0);   /* not reached */
    }
    int status = 0;
    assert(waitpid(child, &status, 0) == child);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == DMESH_EXIT_LOST);

    /* Opt-out: the process lives and every surface reports the loss. */
    setenv("DPUMESH_EXIT_ON_LOSS", "0", 1);
    dmesh_channel_t *server = idle_server();
    dmesh_channel_t *client = dmesh_create_channel();
    assert(client);
    dmesh_qp_t *qp = dmesh_create_qp(client, "echo:80");
    assert(qp && dmesh_qp_fd(qp) >= 0);
    test_native_lose(ECONNRESET);
    dmesh_event_t ev[4];
    errno = 0;
    assert(dmesh_accept(server) == NULL && errno == ECONNRESET);
    assert(dpumesh_lost(server->ctx) == ECONNRESET);
    assert(readable(dmesh_channel_fd(server)));   /* a sleeper comes back for the error */
    errno = 0;
    assert(dmesh_accept(server) == NULL && errno == ECONNRESET);   /* sticky */
    /* The client's channel sees it, and its QPs report it. */
    errno = 0;
    assert(dmesh_accept(client) == NULL && errno == ECONNRESET);
    errno = 0;
    assert(dmesh_create_qp(client, "echo:80") == NULL && errno == ECONNRESET);
    errno = 0;
    assert(dmesh_poll_qp(qp, ev, 4) == -1 && errno == ECONNRESET);
    assert(readable(qp->eq->epfd));
    (void)dmesh_abort_qp(qp);
    puts("native_loss_test: exit by default, sticky errors and readable fds when kept: PASS");
    return 0;
}
