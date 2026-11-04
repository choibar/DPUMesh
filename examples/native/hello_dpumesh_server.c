/* Minimal native DPUmesh byte-stream echo server. The channel is its
 * listening socket: dmesh_accept() returns each inbound stream as a QP with
 * its own fd. For clarity it serves one connection at a time and closes a
 * stream that arrives meanwhile. */
#define _GNU_SOURCE
#include <errno.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <dpumesh/dmesh.h>

#define EVENT_BATCH 16
#define PENDING_MAX 4096

/* Waits for the channel and, when serving one, the connection. */
static int wait_fds(int channel_fd, int qp_fd)
{
    struct pollfd pfd[2] = {
        { .fd = channel_fd, .events = POLLIN },
        { .fd = qp_fd, .events = POLLIN },
    };
    int rc;
    do {
        rc = poll(pfd, qp_fd >= 0 ? 2 : 1, -1);
    } while (rc < 0 && errno == EINTR);
    if (rc <= 0)
        return -1;
    return 0;
}

int main(void)
{
    if (!getenv("DPUMESH_SERVICE")) {
        fprintf(stderr, "DPUMESH_SERVICE must be this server's Service target (host:port)\n");
        return 2;
    }
    dmesh_channel_t *channel = dmesh_create_channel();
    int channel_fd = channel ? dmesh_channel_fd(channel) : -1;
    if (!channel || channel_fd < 0) {
        perror("DPUmesh setup");
        return 1;
    }

    dmesh_qp_t *active = NULL;
    int active_fd = -1;
    uint8_t pending[PENDING_MAX];
    size_t pending_len = 0;
    fprintf(stderr, "hello server ready as %s\n", getenv("DPUMESH_SERVICE"));

    for (;;) {
        if (wait_fds(channel_fd, active_fd) != 0)
            break;
        dmesh_qp_t *qp;
        while ((qp = dmesh_accept(channel)) != NULL) {
            if (active) {
                dmesh_destroy_qp(qp);
                continue;
            }
            active = qp;
            active_fd = dmesh_qp_fd(qp);
        }
        if (errno != EAGAIN)
            break;                       /* the session is gone */
        if (!active)
            continue;

        dmesh_event_t events[EVENT_BATCH];
        int n, close_active = 0;
        while ((n = dmesh_poll_qp(active, events, EVENT_BATCH)) > 0) {
            for (int i = 0; i < n; i++) {
                dmesh_event_t *event = &events[i];
                if (event->type == DMESH_EVENT_RECV) {
                    if (event->len > PENDING_MAX - pending_len) {
                        close_active = 1;
                    } else {
                        memcpy(pending + pending_len, event->buf, event->len);
                        pending_len += event->len;
                    }
                    dmesh_release_rx_buffer(channel, event);
                } else if (event->type == DMESH_EVENT_RECV_FIN ||
                           event->type == DMESH_EVENT_TX_ERROR) {
                    close_active = 1;
                }
            }
        }
        if (n < 0 || close_active) {
            dmesh_destroy_qp(active);
            active = NULL;
            active_fd = -1;
            pending_len = 0;
            continue;
        }
        if (!pending_len)
            continue;

        void *tx = dmesh_alloc(active, (uint32_t)pending_len);
        if (!tx) {
            if (errno == EAGAIN)
                continue;
            dmesh_destroy_qp(active);
            active = NULL;
            active_fd = -1;
            pending_len = 0;
            continue;
        }
        memcpy(tx, pending, pending_len);
        if (dmesh_post_send(active, tx, (uint32_t)pending_len) != 0 ||
            dmesh_flush(active) != 0) {
            dmesh_destroy_qp(active);
            active = NULL;
            active_fd = -1;
        }
        pending_len = 0;
    }

    dmesh_destroy_qp(active);
    dmesh_destroy_channel(channel);
    return 1;
}
