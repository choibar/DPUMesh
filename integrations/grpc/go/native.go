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

// cgoNative drives libdpumesh: one channel for the process, polled as its
// listening socket, and EQs on demand.
type cgoNative struct {
	serving  bool // the channel serves DPUMESH_SERVICE
	ch       *C.dmesh_channel_t
	listener *eqWaiter // the channel's fd on Go's netpoller
}

// cgoEQ is one native EQ with its fd on Go's netpoller and a C-allocated
// poll buffer (no Go pointer crosses into C).
type cgoEQ struct {
	eq     *C.dmesh_eq_t
	waiter *eqWaiter
	events *C.dmesh_event_t
}

// openCgoNative opens the process channel and takes its fd, before any EQ,
// so the channel is the listening socket. Connections each poll an EQ of
// their own, so each flow asks for its own wake consumer (DPUMESH_FLOW_WAKE;
// an explicit setting wins). On failure a non-nil native holds resources
// whose cleanup failed; close retries it.
func openCgoNative() (*cgoNative, error) {
	if os.Getenv("DPUMESH_FLOW_WAKE") == "" {
		_ = os.Setenv("DPUMESH_FLOW_WAKE", "1")
	}
	// dmesh_create_channel serves DPUMESH_SERVICE when it is set.
	n := &cgoNative{serving: os.Getenv("DPUMESH_SERVICE") != ""}
	ch, err := C.dmesh_create_channel()
	if ch == nil {
		return nil, fmt.Errorf("dmesh: create channel: %w", err)
	}
	n.ch = ch
	if fd, ferr := C.dmesh_channel_fd(ch); fd < 0 {
		err = ferr
	} else {
		n.listener, err = newEQWaiter(int(fd))
	}
	if err != nil {
		err = fmt.Errorf("dmesh: channel fd: %w", err)
		if cerr := n.close(); cerr != nil {
			return n, err
		}
		return nil, err
	}
	return n, nil
}

func (n *cgoNative) postMax() int { return int(C.dmesh_post_max(n.ch)) }
func (n *cgoNative) serves() bool { return n.serving }

func (n *cgoNative) newEQ() (eqID, error) {
	e := &cgoEQ{events: C.dmesh_go_events_alloc()}
	if e.events == nil {
		return nil, fmt.Errorf("dmesh: allocate EQ events: %w", syscall.ENOMEM)
	}
	eq, err := C.dmesh_create_eq(n.ch)
	if eq == nil {
		C.free(unsafe.Pointer(e.events))
		return nil, fmt.Errorf("dmesh: create EQ: %w", err)
	}
	e.eq = eq
	if fd := C.dmesh_eq_fd(eq); fd >= 0 {
		e.waiter, err = newEQWaiter(int(fd))
	} else {
		err = syscall.EIO
	}
	if err != nil {
		C.dmesh_destroy_eq(eq)
		C.free(unsafe.Pointer(e.events))
		return nil, fmt.Errorf("dmesh: EQ readiness fd: %w", err)
	}
	return unsafe.Pointer(e), nil
}

// destroyEQ closes the netpoller's duplicate of the EQ fd first.
func (n *cgoNative) destroyEQ(id eqID) error {
	e := (*cgoEQ)(id)
	if e.waiter != nil {
		e.waiter.close()
		e.waiter = nil
	}
	if rc, err := C.dmesh_destroy_eq(e.eq); rc != 0 {
		return err
	}
	C.free(unsafe.Pointer(e.events))
	e.events = nil
	return nil
}

// pollWait drains the EQ; while it is empty the goroutine parks on the Go
// netpoller until the EQ fd is readable, wake is called or maxWaitNs passes
// (< 0: unbounded). A wake or an expiry returns 0.
func (n *cgoNative) pollWait(id eqID, out []nativeEvent, maxWaitNs int64) (int, error) {
	e := (*cgoEQ)(id)
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
		r = C.dmesh_go_poll(e.eq, e.events, C.int(max))
		if r < 0 {
			return 0, syscall.Errno(-r)
		}
		if r > 0 {
			break
		}
		woken, err := e.waiter.waitUntil(deadline)
		if err != nil {
			return 0, err
		}
		if woken || (!deadline.IsZero() && !time.Now().Before(deadline)) {
			return 0, nil
		}
	}
	events := unsafe.Slice(e.events, int(r))
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
		case C.DMESH_EVENT_TX_READY:
			o.kind = evTxReady
		case C.DMESH_EVENT_TX_ERROR:
			o.kind = evTxError
		}
	}
	return int(r), nil
}

func (n *cgoNative) wake(id eqID) {
	if e := (*cgoEQ)(id); e.waiter != nil {
		e.waiter.interrupt()
	}
}

func (n *cgoNative) dial(id eqID, target string) (qpID, int, error) {
	name := C.CString(target)
	defer C.free(unsafe.Pointer(name))
	qp, err := C.dmesh_create_qp((*cgoEQ)(id).eq, name)
	if qp == nil {
		return nil, 0, err
	}
	return unsafe.Pointer(qp), int(qp.local_port), nil
}

func (n *cgoNative) accept(id eqID) (qpID, int, error) {
	var eq *C.dmesh_eq_t
	if id != nil {
		eq = (*cgoEQ)(id).eq
	}
	qp, err := C.dmesh_accept(n.ch, eq)
	if qp == nil {
		return nil, 0, err
	}
	return unsafe.Pointer(qp), int(qp.remote_port), nil
}

func (n *cgoNative) waitChannel() error {
	_, err := n.listener.waitUntil(time.Time{})
	return err
}

func (n *cgoNative) wakeChannel() { n.listener.interrupt() }

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

// close destroys the channel; its EQs are gone with their owners. A failed
// step keeps the channel for a retry.
func (n *cgoNative) close() error {
	if n.listener != nil {
		n.listener.close()
		n.listener = nil
	}
	if n.ch != nil {
		if rc, err := C.dmesh_destroy_channel(n.ch); rc != 0 {
			return err
		}
		n.ch = nil
	}
	return nil
}
