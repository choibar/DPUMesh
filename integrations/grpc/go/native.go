package dmeshgo

/*
#cgo CFLAGS: -D_GNU_SOURCE -I${SRCDIR}/../../../include
#cgo LDFLAGS: -L${SRCDIR}/../../../build/lib -ldpumesh -Wl,-rpath,${SRCDIR}/../../../build/lib
#include <errno.h>
#include <poll.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <time.h>
#include <unistd.h>
#include "dpumesh/dmesh.h"

#define DMESH_GO_EVENTS 64

static dmesh_event_t *dmesh_go_events_alloc(void) { return calloc(DMESH_GO_EVENTS, sizeof(dmesh_event_t)); }
static int dmesh_go_errno(void) { return errno ? errno : EIO; }

// Release by token so no Go pointer crosses into C on the receive path.
static void dmesh_go_release(dmesh_channel_t *s, int32_t token) {
    dmesh_event_t e = { ._rx_token = token };
    dmesh_release_rx_buffer(s, &e);
}

static int dmesh_go_wake_fd(void) { return eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC); }
static void dmesh_go_wake(int fd) { uint64_t one = 1; if (write(fd, &one, sizeof(one)) < 0) {} }

static int64_t dmesh_go_elapsed_ns(const struct timespec *start) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (int64_t)(now.tv_sec - start->tv_sec) * 1000000000LL + (now.tv_nsec - start->tv_nsec);
}

// Drains the EQ into events. While it is empty the loop stays in C: it sleeps
// on the EQ readiness fd (level-triggered, so the library's spin window and
// tick return at once) and on wake_fd, re-polling on each wake and at the next
// retained-tail deadline. A wake_fd signal or max_wait_ns (< 0: unbounded)
// returns 0 to Go. Returns the event count, or -errno.
static int dmesh_go_poll_wait(dmesh_eq_t *eq, int eq_fd, int wake_fd,
                              dmesh_event_t *events, int max, int64_t max_wait_ns) {
    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (;;) {
        errno = 0;
        int n = dmesh_poll_eq(eq, events, max);
        if (n < 0) return -dmesh_go_errno();
        if (n > 0) return n;
        int64_t wait = dmesh_eq_next_deadline_ns(eq);
        int bounded = 0;
        if (max_wait_ns >= 0) {
            int64_t left = max_wait_ns - dmesh_go_elapsed_ns(&start);
            if (left <= 0) return 0;
            if (wait < 0 || wait > left) { wait = left; bounded = 1; }
        }
        struct timespec timeout, *bound = NULL;
        if (wait >= 0) {
            timeout.tv_sec = (time_t)(wait / 1000000000LL);
            timeout.tv_nsec = (long)(wait % 1000000000LL);
            bound = &timeout;
        }
        struct pollfd fds[2] = { { eq_fd, POLLIN, 0 }, { wake_fd, POLLIN, 0 } };
        int ready = ppoll(fds, 2, bound, NULL);
        if (ready < 0) {
            if (errno == EINTR) continue;
            return -dmesh_go_errno();
        }
        if (fds[1].revents & POLLIN) {
            uint64_t value;
            if (read(wake_fd, &value, sizeof(value)) < 0) {}
            return 0;
        }
        if (ready == 0 && bounded) return 0;
    }
}

// One transmit call: reserve, copy and commit len bytes. Returns len or -errno.
static int dmesh_go_send(dmesh_qp_t *qp, const void *src, uint32_t len) {
    errno = 0;
    void *dst = dmesh_alloc(qp, len);
    if (!dst) return -dmesh_go_errno();
    memcpy(dst, src, len);
    errno = 0;
    if (dmesh_post_send(qp, dst, len) != 0) return -dmesh_go_errno();
    return (int)len;
}
*/
import "C"

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// cgoNative drives libdpumesh: one channel and one EQ for the process.
type cgoNative struct {
	serving bool // the channel serves DPUMESH_SERVICE
	ch      *C.dmesh_channel_t
	eq      *C.dmesh_eq_t
	eqFD    C.int
	wakeFD  C.int
	events  *C.dmesh_event_t // C-allocated poll buffer (no Go pointer crosses into C)
}

// openCgoNative opens the process channel and EQ. On failure a non-nil native
// holds resources whose cleanup failed; close retries it.
func openCgoNative() (*cgoNative, error) {
	// dmesh_create_channel serves DPUMESH_SERVICE when it is set.
	n := &cgoNative{eqFD: -1, wakeFD: -1, serving: os.Getenv("DPUMESH_SERVICE") != ""}
	n.events = C.dmesh_go_events_alloc()
	if n.events == nil {
		return nil, fmt.Errorf("dmesh: allocate EQ events: %w", syscall.ENOMEM)
	}
	fd, err := C.dmesh_go_wake_fd()
	if fd < 0 {
		C.free(unsafe.Pointer(n.events))
		return nil, fmt.Errorf("dmesh: wake eventfd: %w", err)
	}
	n.wakeFD = fd
	ch, err := C.dmesh_create_channel()
	if ch == nil {
		n.freeLocal()
		return nil, fmt.Errorf("dmesh: create channel: %w", err)
	}
	n.ch = ch
	eq, err := C.dmesh_create_eq(ch)
	if eq == nil {
		openErr := fmt.Errorf("dmesh: create EQ: %w", err)
		if closeErr := n.close(); closeErr != nil {
			// Shared Comch/DMA cleanup can fail without releasing its resources.
			// Keep the channel so CloseTransport can retry the cleanup.
			return n, fmt.Errorf("%w; dmesh: close channel: %w", openErr, closeErr)
		}
		return nil, openErr
	}
	n.eq = eq
	n.eqFD = C.dmesh_eq_fd(eq)
	if n.eqFD < 0 {
		openErr := fmt.Errorf("dmesh: EQ readiness fd: %w", syscall.EIO)
		if closeErr := n.close(); closeErr != nil {
			return n, fmt.Errorf("%w; dmesh: close channel: %w", openErr, closeErr)
		}
		return nil, openErr
	}
	return n, nil
}

func (n *cgoNative) postMax() int { return int(C.dmesh_post_max(n.ch)) }
func (n *cgoNative) serves() bool { return n.serving }

func (n *cgoNative) pollWait(out []nativeEvent, maxWaitNs int64) (int, error) {
	max := len(out)
	if max > C.DMESH_GO_EVENTS {
		max = C.DMESH_GO_EVENTS
	}
	r := C.dmesh_go_poll_wait(n.eq, n.eqFD, n.wakeFD, n.events, C.int(max), C.int64_t(maxWaitNs))
	if r < 0 {
		return 0, syscall.Errno(-r)
	}
	events := unsafe.Slice(n.events, int(r))
	for i := range events {
		ev := &events[i]
		o := &out[i]
		*o = nativeEvent{qp: unsafe.Pointer(ev.qp), token: int32(ev._rx_token)}
		switch ev._type {
		case C.DMESH_EVENT_RECV:
			o.kind = evRecv
			o.buf = unsafe.Slice((*byte)(unsafe.Pointer(ev.buf)), int(ev.len))
		case C.DMESH_EVENT_RECV_FIN:
			o.kind = evFin
		case C.DMESH_EVENT_CONN_REQ:
			o.kind = evConnReq
			o.localPort = int(ev.qp.local_port)
			o.remotePort = int(ev.qp.remote_port)
		case C.DMESH_EVENT_TX_READY:
			o.kind = evTxReady
		case C.DMESH_EVENT_TX_ERROR:
			o.kind = evTxError
		}
	}
	return int(r), nil
}

func (n *cgoNative) wake() { C.dmesh_go_wake(n.wakeFD) }

func (n *cgoNative) dial(target string) (qpID, int, error) {
	name := C.CString(target)
	defer C.free(unsafe.Pointer(name))
	qp, err := C.dmesh_create_qp(n.eq, name)
	if qp == nil {
		return nil, 0, err
	}
	return unsafe.Pointer(qp), int(qp.local_port), nil
}

func (n *cgoNative) send(qp qpID, p []byte) (int, error) {
	r := C.dmesh_go_send((*C.dmesh_qp_t)(qp), unsafe.Pointer(&p[0]), C.uint32_t(len(p)))
	if r < 0 {
		if syscall.Errno(-r) == syscall.EAGAIN {
			return 0, errWouldBlock
		}
		return 0, syscall.Errno(-r)
	}
	return int(r), nil
}

func (n *cgoNative) release(token int32) { C.dmesh_go_release(n.ch, C.int32_t(token)) }

func (n *cgoNative) destroy(qp qpID, abort bool) error {
	var rc C.int
	var err error
	if abort {
		rc, err = C.dmesh_abort_qp((*C.dmesh_qp_t)(qp))
	} else {
		rc, err = C.dmesh_destroy_qp((*C.dmesh_qp_t)(qp))
	}
	// ABI5 consumes the QP even on error; only channel teardown is retryable.
	if rc != 0 {
		return err
	}
	return nil
}

// close destroys the EQ, then the channel. A failed step keeps what remains
// for a retry.
func (n *cgoNative) close() error {
	if n.eq != nil {
		if rc, err := C.dmesh_destroy_eq(n.eq); rc != 0 {
			return err
		}
		n.eq = nil
	}
	if n.ch != nil {
		if rc, err := C.dmesh_destroy_channel(n.ch); rc != 0 {
			return err
		}
		n.ch = nil
	}
	n.freeLocal()
	return nil
}

// freeLocal releases the wake fd and the poll buffer.
func (n *cgoNative) freeLocal() {
	if n.wakeFD >= 0 {
		C.close(n.wakeFD)
		n.wakeFD = -1
	}
	if n.events != nil {
		C.free(unsafe.Pointer(n.events))
		n.events = nil
	}
}
