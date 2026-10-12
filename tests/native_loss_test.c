/* A lost transport session over the memory carrier: by default the process
 * exits with DMESH_EXIT_LOST at the first poll that sees it; with
 * DPUMESH_EXIT_ON_LOSS=0 the EQ fd stays readable, polls report the error
 * after the events that landed before it, and dials fail. */
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

/* A server that sleeps on its EQ with nothing to accept. */
static dmesh_eq_t *idle_server(void)
{
    setenv("DPUMESH_SERVICE", "echo:80", 1);
    dmesh_channel_t *ch = dmesh_create_channel();
    assert(ch);
    unsetenv("DPUMESH_SERVICE");
    dmesh_eq_t *eq = dmesh_create_eq(ch);
    assert(eq && dmesh_eq_fd(eq) >= 0);
    dmesh_event_t ev[4];
    assert(dmesh_poll_eq(eq, ev, 4) == 0);
    return eq;
}

int main(void)
{
    /* Default: the first poll after the loss ends the process. */
    pid_t child = fork();
    assert(child >= 0);
    if (child == 0) {
        dmesh_eq_t *eq = idle_server();
        test_native_lose(ECONNRESET);
        dmesh_event_t ev[4];
        (void)dmesh_poll_eq(eq, ev, 4);
        _exit(0);   /* not reached */
    }
    int status = 0;
    assert(waitpid(child, &status, 0) == child);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == DMESH_EXIT_LOST);

    /* Opt-out: the process lives and every surface reports the loss. */
    setenv("DPUMESH_EXIT_ON_LOSS", "0", 1);
    dmesh_eq_t *eq = idle_server();
    dmesh_channel_t *client = dmesh_create_channel();
    assert(client);
    dmesh_eq_t *ceq = dmesh_create_eq(client);
    assert(ceq && dmesh_eq_fd(ceq) >= 0);
    dmesh_qp_t *qp = dmesh_create_qp(ceq, "echo:80");
    assert(qp);
    test_native_lose(ECONNRESET);
    dmesh_event_t ev[4];
    errno = 0;
    assert(dmesh_poll_eq(eq, ev, 4) == -1 && errno == ECONNRESET);
    assert(dpumesh_lost(eq->ch->ctx) == ECONNRESET);
    assert(readable(dmesh_eq_fd(eq)));          /* a sleeper comes back for the error */
    errno = 0;
    assert(dmesh_poll_eq(eq, ev, 4) == -1 && errno == ECONNRESET);   /* sticky */
    errno = 0;
    assert(dmesh_create_qp(ceq, "echo:80") == NULL && errno == ECONNRESET);
    errno = 0;
    assert(dmesh_poll_eq(ceq, ev, 4) == -1 && errno == ECONNRESET);
    assert(readable(dmesh_eq_fd(ceq)));
    (void)dmesh_abort_qp(qp);
    puts("native_loss_test: exit by default, sticky errors and readable EQs when kept: PASS");
    return 0;
}
