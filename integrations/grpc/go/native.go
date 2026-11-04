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

// One non-blocking QP poll. Returns the event count, or -errno. An empty
// poll arms the QP's fd, which the Go netpoller then waits on (fd_wait_linux.go).
static int dmesh_go_poll(dmesh_qp_t *qp, dmesh_event_t *events, int max) {
    errno = 0;
    int n = dmesh_poll_qp(qp, events, max);
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

// cgoNative drives libdpumesh: one channel for the process, its listening
// socket, and its QPs.
type cgoNative struct {
	serving  bool // the channel serves DPUMESH_SERVICE
	ch       *C.dmesh_channel_t
	listener *fdWaiter        // the channel's fd on Go's netpoller
	requests *C.dmesh_event_t // the channel poll's buffer
}

// cgoQP is one native QP with its fd on Go's netpoller and a C-allocated poll
// buffer (no Go pointer crosses into C). Its address is the qpID.
type cgoQP struct {
	qp     *C.dmesh_qp_t
	waiter *fdWaiter
	events *C.dmesh_event_t
	max    C.int // events the next poll may return
	r      C.int // the last poll's result: events, or -errno
}

// poll is the waiter's try: one native poll, which arms the QP's fd when it
// comes back empty.
func (q *cgoQP) poll() bool {
	q.r = C.dmesh_go_poll(q.qp, q.events, q.max)
	return q.r != 0
}

// openCgoNative opens the process channel and puts its fd on the netpoller.
// On failure a non-nil native holds resources whose cleanup failed; close
// retries it.
func openCgoNative() (*cgoNative, error) {
	// dmesh_create_channel serves DPUMESH_SERVICE when it is set.
	n := &cgoNative{serving: os.Getenv("DPUMESH_SERVICE") != ""}
	ch, err := C.dmesh_create_channel()
	if ch == nil {
		return nil, fmt.Errorf("dmesh: create channel: %w", err)
	}
	n.ch = ch
	n.requests = (*C.dmesh_event_t)(C.calloc(requestBatch, C.sizeof_dmesh_event_t))
	if n.requests == nil {
		err = syscall.ENOMEM
	} else if fd := C.dmesh_channel_fd(ch); fd < 0 {
		err = syscall.EIO
	} else {
		n.listener, err = newFDWaiter(int(fd), nil)
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

// wrap gives a new native QP its waiter and poll buffer, or aborts it.
func wrap(qp *C.dmesh_qp_t) (*cgoQP, error) {
	q := &cgoQP{qp: qp, events: C.dmesh_go_events_alloc()}
	var err error = syscall.ENOMEM
	if q.events != nil {
		if fd := C.dmesh_qp_fd(qp); fd < 0 {
			err = syscall.EIO
		} else {
			q.waiter, err = newFDWaiter(int(fd), q.poll)
		}
	}
	if err != nil {
		C.dmesh_abort_qp(qp)
		C.free(unsafe.Pointer(q.events))
		return nil, fmt.Errorf("dmesh: QP readiness fd: %w", err)
	}
	return q, nil
}

// pollWait drains the QP; while it is empty the goroutine parks on the Go
// netpoller until the QP's fd is readable, wake is called or maxWaitNs passes
// (< 0: unbounded). A wake or an expiry returns 0. The waiter runs the polls
// (cgoQP.poll), each after the netpoller's readiness is reset.
func (n *cgoNative) pollWait(id qpID, out []nativeEvent, maxWaitNs int64) (int, error) {
	q := (*cgoQP)(id)
	q.max = C.int(min(len(out), C.DMESH_GO_EVENTS))
	var deadline time.Time
	if maxWaitNs >= 0 {
		deadline = time.Now().Add(time.Duration(maxWaitNs))
	}
	if _, err := q.waiter.waitUntil(deadline); err != nil {
		return 0, err
	}
	r := q.r
	if r < 0 {
		return 0, syscall.Errno(-r)
	}
	events := unsafe.Slice(q.events, int(r))
	for i := range events {
		ev := &events[i]
		o := &out[i]
		*o = nativeEvent{token: int32(ev._rx_token)}
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

func (n *cgoNative) wake(id qpID) { (*cgoQP)(id).waiter.interrupt() }

func (n *cgoNative) dial(target string) (qpID, int, error) {
	name := C.CString(target)
	defer C.free(unsafe.Pointer(name))
	qp, err := C.dmesh_create_qp(n.ch, name)
	if qp == nil {
		return nil, 0, err
	}
	port := int(qp.local_port)
	q, err := wrap(qp)
	if err != nil {
		return nil, 0, err
	}
	return unsafe.Pointer(q), port, nil
}

func (n *cgoNative) accept() (qpID, int, error) {
	qp, err := C.dmesh_accept(n.ch)
	if qp == nil {
		return nil, 0, err
	}
	port := int(qp.remote_port)
	q, err := wrap(qp)
	if err != nil {
		return nil, 0, err
	}
	return unsafe.Pointer(q), port, nil
}

// pollChannel copies the DPU's requests out of the channel's storage, which
// the next poll reuses.
func (n *cgoNative) pollChannel(out []nativeEvent) (int, error) {
	r, err := C.dmesh_poll_channel(n.ch, n.requests, C.int(min(len(out), requestBatch)))
	if r < 0 {
		return 0, err
	}
	for i, ev := range unsafe.Slice(n.requests, int(r)) {
		o := &out[i]
		*o = nativeEvent{token: -1}
		switch ev._type {
		case C.DMESH_EVENT_PREPARE:
			o.kind = evPrepare
			o.target = C.GoStringN((*C.char)(unsafe.Pointer(ev.buf)), C.int(ev.len))
		case C.DMESH_EVENT_SCALE:
			sc := (*C.dmesh_scale_t)(unsafe.Pointer(ev.buf))
			o.kind = evScale
			o.target = C.GoString(&sc.target[0])
			o.count = int(sc.count)
			o.scaleToken = uint32(sc.token)
		}
	}
	return int(r), nil
}

func (n *cgoNative) waitChannel() error {
	_, err := n.listener.waitUntil(time.Time{})
	return err
}

func (n *cgoNative) wakeChannel() { n.listener.interrupt() }

func (n *cgoNative) send(id qpID, p []byte) (int, error) {
	r := C.dmesh_go_send((*cgoQP)(id).qp, unsafe.Pointer(&p[0]), C.uint32_t(len(p)))
	if r < 0 {
		if syscall.Errno(-r) == syscall.EAGAIN {
			return 0, errWouldBlock
		}
		return 0, syscall.Errno(-r)
	}
	return int(r), nil
}

func (n *cgoNative) spareReady(qp qpID) error {
	if rc, err := C.dmesh_spare_ready((*cgoQP)(qp).qp); rc != 0 {
		return err
	}
	return nil
}

func (n *cgoNative) scaleAck(token uint32, applied int) error {
	if rc, err := C.dmesh_scale_ack(n.ch, C.uint32_t(token), C.uint32_t(applied)); rc != 0 {
		return err
	}
	return nil
}

func (n *cgoNative) release(token int32) { C.dmesh_go_release(n.ch, C.int32_t(token)) }

// destroy closes the netpoller's duplicate of the QP's fd first: the native
// close takes the fd with the QP, even when it fails.
func (n *cgoNative) destroy(id qpID, abort bool) error {
	q := (*cgoQP)(id)
	q.waiter.close()
	var rc C.int
	var err error
	if abort {
		rc, err = C.dmesh_abort_qp(q.qp)
	} else {
		rc, err = C.dmesh_destroy_qp(q.qp)
	}
	C.free(unsafe.Pointer(q.events))
	if rc != 0 {
		return err
	}
	return nil
}

// close destroys the channel, after its QPs. A failed step keeps the channel
// for a retry.
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
	C.free(unsafe.Pointer(n.requests))
	n.requests = nil
	return nil
}
