package dmeshgo

/*
#cgo CFLAGS: -D_GNU_SOURCE -I${SRCDIR}/../../../include
#cgo LDFLAGS: -L${SRCDIR}/../../../build/lib -ldpumesh -Wl,-rpath,${SRCDIR}/../../../build/lib
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "dpumesh/dmesh.h"

#define DMESH_GO_EVENTS 64

static dmesh_event_t *dmesh_go_events_alloc(void) { return calloc(DMESH_GO_EVENTS, sizeof(dmesh_event_t)); }
static int dmesh_go_errno(void) { return errno ? errno : EIO; }

// Release by token so no Go pointer crosses into C on the receive path.
static void dmesh_go_release(dmesh_channel_t *s, int32_t token) {
    dmesh_event_t e = { ._rx_token = token };
    dmesh_release_rx_buffer(s, &e);
}

// One non-blocking EQ poll. Returns the event count, or -errno. An empty poll
// arms the EQ fd, which the Go netpoller then waits on (eq_wait_linux.go).
static int dmesh_go_poll(dmesh_eq_t *eq, dmesh_event_t *events, int max) {
    errno = 0;
    int n = dmesh_poll_eq(eq, events, max);
    return n < 0 ? -dmesh_go_errno() : n;
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
	"time"
	"unsafe"
)

// cgoNative drives libdpumesh: one channel and one EQ for the process.
type cgoNative struct {
	serving bool // the channel serves DPUMESH_SERVICE
	ch      *C.dmesh_channel_t
	eq      *C.dmesh_eq_t
	eqFD    C.int
	waiter  *eqWaiter        // the EQ fd on Go's netpoller
	events  *C.dmesh_event_t // C-allocated poll buffer (no Go pointer crosses into C)
}

// openCgoNative opens the process channel and EQ. On failure a non-nil native
// holds resources whose cleanup failed; close retries it.
func openCgoNative() (*cgoNative, error) {
	// dmesh_create_channel serves DPUMESH_SERVICE when it is set.
	n := &cgoNative{eqFD: -1, serving: os.Getenv("DPUMESH_SERVICE") != ""}
	n.events = C.dmesh_go_events_alloc()
	if n.events == nil {
		return nil, fmt.Errorf("dmesh: allocate EQ events: %w", syscall.ENOMEM)
	}
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
	if n.eqFD >= 0 {
		n.waiter, err = newEQWaiter(int(n.eqFD))
	} else {
		err = syscall.EIO
	}
	if err != nil {
		openErr := fmt.Errorf("dmesh: EQ readiness fd: %w", err)
		if closeErr := n.close(); closeErr != nil {
			return n, fmt.Errorf("%w; dmesh: close channel: %w", openErr, closeErr)
		}
		return nil, openErr
	}
	return n, nil
}

func (n *cgoNative) postMax() int { return int(C.dmesh_post_max(n.ch)) }
func (n *cgoNative) serves() bool { return n.serving }

// pollWait drains the EQ; while it is empty the goroutine parks on the Go
// netpoller until the EQ fd is readable, wake is called or maxWaitNs passes
// (< 0: unbounded). A wake or an expiry returns 0.
func (n *cgoNative) pollWait(out []nativeEvent, maxWaitNs int64) (int, error) {
	max := len(out)
	if max > C.DMESH_GO_EVENTS {
		max = C.DMESH_GO_EVENTS
	}
	var deadline time.Time
	if maxWaitNs >= 0 {
		deadline = time.Now().Add(time.Duration(maxWaitNs))
	}
	var r C.int
	for {
		r = C.dmesh_go_poll(n.eq, n.events, C.int(max))
		if r < 0 {
			return 0, syscall.Errno(-r)
		}
		if r > 0 {
			break
		}
		woken, err := n.waiter.waitUntil(deadline)
		if err != nil {
			return 0, err
		}
		if woken || (!deadline.IsZero() && !time.Now().Before(deadline)) {
			return 0, nil
		}
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

func (n *cgoNative) wake() {
	if n.waiter != nil {
		n.waiter.interrupt()
	}
}

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
// for a retry. The netpoller's duplicate of the EQ fd goes first.
func (n *cgoNative) close() error {
	if n.waiter != nil {
		n.waiter.close()
		n.waiter = nil
	}
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

// freeLocal releases the poll buffer.
func (n *cgoNative) freeLocal() {
	if n.events != nil {
		C.free(unsafe.Pointer(n.events))
		n.events = nil
	}
}
