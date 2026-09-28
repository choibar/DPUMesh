// Package dmeshgo adapts the native channel/EQ/QP API to net.Conn and
// net.Listener. One process owns one channel and one polling goroutine.
// Configure DPUMESH_PCI_ADDR, DPUMESH_SERVER, DPUMESH_POD_IP and, for a
// listener (ListenService), the DPUMESH_SERVICE "<host>:<port>" target before
// starting Go.
package dmeshgo

/*
#cgo CFLAGS: -D_GNU_SOURCE -I${SRCDIR}/../../../include
#cgo LDFLAGS: -L${SRCDIR}/../../../build/lib -ldpumesh -Wl,-rpath,${SRCDIR}/../../../build/lib
#include <stdlib.h>
#include <string.h>
#include "dpumesh/dmesh.h"
#include <poll.h>
#include <time.h>
#include <unistd.h>
// The EQ fd is a level-triggered epoll set: dmesh_poll_eq running to empty
// settles it, so the wake needs no read.
#define DMESH_GO_EVENTS 64
static dmesh_event_t *dmesh_go_events_alloc(void) { return calloc(DMESH_GO_EVENTS, sizeof(dmesh_event_t)); }
// Release by token so no Go pointer crosses into C on the receive path.
static void dmesh_go_release(dmesh_channel_t *s, int32_t token) {
    dmesh_event_t e = { ._rx_token = token };
    dmesh_release_rx_buffer(s, &e);
}
static void dmesh_go_wait_fd(int fd, int64_t timeout_ns) {
    if (timeout_ns < 0 || timeout_ns > 1000000) timeout_ns = 1000000;
    struct timespec timeout = {0, timeout_ns};
    struct pollfd event = {.fd = fd, .events = POLLIN};
    (void)ppoll(&event, 1, &timeout, NULL);
}
*/
import "C"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

const (
	ModeBackend     = 1
	ModeIngressPush = 2
)

// PCIAddr must agree with the native process configuration.
var PCIAddr = envOr("DPUMESH_PCI_ADDR", "")

func envOr(key, fallback string) string {
	if s := os.Getenv(key); s != "" {
		return s
	}
	return fallback
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "dmesh: i/o deadline exceeded" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }
func (timeoutError) Unwrap() error   { return os.ErrDeadlineExceeded }

// All native calls and EQ event dispatch share this mutex. Read and Write
// hold their own direction locks across waits, as required by net.Conn.
// RX events retain native leases; they are copied directly into the caller's
// slice and released only after the last byte has been read.
type transport struct {
	mu       sync.Mutex
	ch       *C.dmesh_channel_t
	eq       *C.dmesh_eq_t
	events   *C.dmesh_event_t // C-allocated poll buffer (no Go pointer crosses into C)
	conns    map[*C.dmesh_qp_t]*Conn
	listener *Listener
	changed  chan struct{} // transport-wide edges: accepts, errors, close
	stop     chan struct{}
	done     chan struct{}
	err      error
	parked   int           // goroutines waiting on an edge (under mu)
	parkedCh chan struct{} // wakes an idle poller for a connection or waiter
}

// park registers the caller as a waiter so the sleeper polls on its behalf,
// then waits for ch or the deadline. Called with t.mu held; returns with it
// released.
func (t *transport) park(ch <-chan struct{}, deadline time.Time) {
	t.parked++
	if t.parked == 1 {
		t.wakePoller()
	}
	t.mu.Unlock()
	_ = waitChange(ch, deadline)
	t.mu.Lock()
	t.parked--
	t.mu.Unlock()
}

// rxEvent is the Go copy of one RECV lease.
type rxEvent struct {
	buf   []byte
	token C.int32_t
}

var process struct {
	sync.Mutex
	t *transport
}

func (t *transport) notify() { close(t.changed); t.changed = make(chan struct{}) }

func newTransport() *transport {
	return &transport{conns: make(map[*C.dmesh_qp_t]*Conn), changed: make(chan struct{}),
		stop: make(chan struct{}), done: make(chan struct{}), parkedCh: make(chan struct{}, 1)}
}

func (t *transport) wakePoller() {
	select {
	case t.parkedCh <- struct{}{}:
	default:
	}
}
func openTransport() (*transport, error) {
	process.Lock()
	defer process.Unlock()
	if process.t != nil {
		return process.t, nil
	}
	t := newTransport()
	t.events = C.dmesh_go_events_alloc()
	if t.events == nil {
		return nil, fmt.Errorf("dmesh: allocate EQ events: %w", syscall.ENOMEM)
	}
	ch, err := C.dmesh_create_channel()
	if ch == nil {
		C.free(unsafe.Pointer(t.events))
		return nil, fmt.Errorf("dmesh: create channel: %w", err)
	}
	t.ch = ch
	eq, err := C.dmesh_create_eq(ch)
	if eq == nil {
		createErr := fmt.Errorf("dmesh: create EQ: %w", err)
		if rc, closeErr := C.dmesh_destroy_channel(ch); rc != 0 {
			// Shared Comch/DMA cleanup can fail without releasing its resources.
			// Preserve the channel so CloseTransport can retry the cleanup.
			t.err = errors.Join(createErr, fmt.Errorf("dmesh: close channel: %w", closeErr))
			close(t.stop)
			close(t.done)
			process.t = t
			return nil, t.err
		}
		C.free(unsafe.Pointer(t.events))
		return nil, createErr
	}
	t.eq = eq
	process.t = t
	go t.poll()
	return t, nil
}

// pollLocked drains one EQ batch into the connections' inboxes and wakes
// only the connections (and the listener) that received something. Called
// under t.mu by whichever goroutine needs progress: a reader with an empty
// inbox, a writer out of TX credit, or the background sleeper. Returns the
// number of events, or -1 once the transport has failed.
func (t *transport) pollLocked() int {
	if t.eq == nil || t.err != nil {
		return -1
	}
	count, err := C.dmesh_poll_eq(t.eq, t.events, C.DMESH_GO_EVENTS)
	if count < 0 {
		t.err = err
		t.notify()
		for _, c := range t.conns {
			c.notify()
		}
		return -1
	}
	events := unsafe.Slice(t.events, int(count))
	var reject map[*C.dmesh_qp_t]bool
	accepted := false
	for i := range events {
		ev := &events[i]
		c := t.conns[ev.qp]
		if ev._type == C.DMESH_EVENT_CONN_REQ && c == nil {
			if t.listener == nil || t.listener.closed {
				if reject == nil {
					reject = make(map[*C.dmesh_qp_t]bool)
				}
				reject[ev.qp] = true
				continue
			}
			c = newConn(t, ev.qp, t.listener.addr, &net.TCPAddr{Port: int(ev.qp.remote_port)})
			t.conns[ev.qp] = c
			t.listener.pending = append(t.listener.pending, c)
			accepted = true
		}
		if c == nil {
			if ev._rx_token >= 0 {
				C.dmesh_go_release(t.ch, ev._rx_token)
			}
			continue
		}
		switch ev._type {
		case C.DMESH_EVENT_RECV:
			c.rx = append(c.rx, rxEvent{buf: unsafe.Slice((*byte)(unsafe.Pointer(ev.buf)), int(ev.len)), token: ev._rx_token})
			c.wake = true
		case C.DMESH_EVENT_RECV_FIN:
			c.eof = true
			c.wake = true
		case C.DMESH_EVENT_TX_ERROR:
			c.err = syscall.EIO
			c.wake = true
		case C.DMESH_EVENT_TX_READY:
			c.wake = true
		}
	}
	for qp := range reject {
		C.dmesh_abort_qp(qp)
	}
	for _, c := range t.conns {
		if c.wake {
			c.wake = false
			c.notify()
		}
	}
	if accepted {
		t.notify()
	}
	return int(count)
}

// poll advances the shared Comch session and buffered TX while any connection
// exists, even when no Read/Write is parked. An idle channel sleeps until a
// connection or waiter appears; active channels wait at most 1 ms between polls.
func (t *transport) poll() {
	fd := int(C.dmesh_eq_fd(t.eq))
	t.runPoll(fd, func() (int, int64) {
		count := t.pollLocked()
		if count < 0 {
			return count, -1
		}
		return count, int64(C.dmesh_eq_next_deadline_ns(t.eq))
	})
}

// progress is called with t.mu held, like inline Read/Write polling. Keeping the
// scheduler separate also lets tests exercise idle and active progress without
// registering a hardware channel.
func (t *transport) runPoll(fd int, progress func() (int, int64)) {
	defer close(t.done)
	for {
		select {
		case <-t.stop:
			return
		default:
		}
		t.mu.Lock()
		if t.parked == 0 && len(t.conns) == 0 {
			t.mu.Unlock()
			select {
			case <-t.parkedCh:
			case <-t.stop:
				return
			}
			continue
		}
		count, deadline := progress()
		t.mu.Unlock()
		if count < 0 {
			return
		}
		if count == C.DMESH_GO_EVENTS {
			continue
		}
		C.dmesh_go_wait_fd(C.int(fd), C.int64_t(deadline))
	}
}

// CloseTransport closes the idle process channel. Call after closing all
// listeners and connections; live objects cause EBUSY without invalidation.
func CloseTransport() error {
	process.Lock()
	defer process.Unlock()
	t := process.t
	if t == nil {
		return nil
	}
	t.mu.Lock()
	if len(t.conns) != 0 || t.listener != nil {
		t.mu.Unlock()
		return syscall.EBUSY
	}
	// An opener can already hold t after dropping process.Mutex. Mark the
	// transport closed under the same lock that guards QP/listener creation
	// before stopping the poller or destroying its EQ. A failed cleanup keeps
	// this state until CloseTransport can retry successfully.
	t.err = net.ErrClosed
	t.notify()
	for _, c := range t.conns {
		c.notify()
	}
	t.mu.Unlock()
	select {
	case <-t.stop:
	default:
		close(t.stop)
	}
	<-t.done
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.eq != nil {
		if n, err := C.dmesh_destroy_eq(t.eq); n != 0 {
			return err
		}
		t.eq = nil
	}
	if n, err := C.dmesh_destroy_channel(t.ch); n != 0 {
		return err
	}
	t.ch = nil
	C.free(unsafe.Pointer(t.events))
	t.events = nil
	process.t = nil
	return nil
}

type Conn struct {
	t               *transport
	qp              *C.dmesh_qp_t
	local, remote   net.Addr
	readMu, writeMu sync.Mutex
	rx              []rxEvent
	pos             int
	rd, wd          time.Time
	closed, eof     bool
	err             error
	changed         chan struct{} // this connection's edges (under t.mu)
	wake            bool          // set by pollLocked, consumed before it returns
}

func newConn(t *transport, qp *C.dmesh_qp_t, local, remote net.Addr) *Conn {
	return &Conn{t: t, qp: qp, local: local, remote: remote, changed: make(chan struct{})}
}
func (c *Conn) edge() chan struct{} {
	if c.changed == nil {
		c.changed = make(chan struct{})
	}
	return c.changed
}
func (c *Conn) notify() {
	if c.changed != nil {
		close(c.changed)
	}
	c.changed = make(chan struct{})
}
func waitChange(ch <-chan struct{}, deadline time.Time) error {
	if deadline.IsZero() {
		<-ch
		return nil
	}
	left := time.Until(deadline)
	if left <= 0 {
		return timeoutError{}
	}
	timer := time.NewTimer(left)
	defer timer.Stop()
	select {
	case <-ch:
		return nil
	case <-timer.C:
		return timeoutError{}
	}
}
func (c *Conn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	t := c.t
	for {
		t.mu.Lock()
		if c.closed {
			t.mu.Unlock()
			return 0, net.ErrClosed
		}
		if len(p) == 0 {
			t.mu.Unlock()
			return 0, nil
		}
		if !c.rd.IsZero() && !time.Now().Before(c.rd) {
			t.mu.Unlock()
			return 0, timeoutError{}
		}
		if len(c.rx) == 0 && c.err == nil && !c.eof {
			t.pollLocked() // in line: no hand-off from a poller goroutine
		}
		if len(c.rx) != 0 {
			ev := &c.rx[0]
			n := copy(p, ev.buf[c.pos:])
			c.pos += n
			if c.pos == len(ev.buf) {
				C.dmesh_go_release(t.ch, ev.token)
				c.rx[0] = rxEvent{}
				c.rx = c.rx[1:]
				c.pos = 0
			}
			t.mu.Unlock()
			return n, nil
		}
		err := c.err
		if err == nil {
			err = t.err
		}
		if err == nil && c.eof {
			err = io.EOF
		}
		if err != nil {
			t.mu.Unlock()
			return 0, err
		}
		// Re-read state under the lock: a deadline extension can race the
		// previous timer firing, and close/error takes precedence on wake.
		t.park(c.edge(), c.rd)
	}
}
func (c *Conn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	t := c.t
	written := 0
	for written < len(p) {
		t.mu.Lock()
		err := c.err
		if err == nil {
			err = t.err
		}
		if c.closed {
			err = net.ErrClosed
		}
		if err == nil && !c.wd.IsZero() && !time.Now().Before(c.wd) {
			err = timeoutError{}
		}
		if err != nil {
			t.mu.Unlock()
			return written, err
		}
		n := len(p) - written
		if max := int(C.dmesh_post_max(t.ch)); n > max {
			n = max
		}
		dst, err := C.dmesh_alloc(c.qp, C.uint32_t(n))
		if dst == nil {
			if !errors.Is(err, syscall.EAGAIN) {
				t.mu.Unlock()
				return written, err
			}
			// Out of TX credit: reclaim custody ACKs in line before parking.
			if t.pollLocked() > 0 {
				t.mu.Unlock()
				continue
			}
			t.park(c.edge(), c.wd)
			continue
		}
		copy(unsafe.Slice((*byte)(dst), n), p[written:written+n])
		rc, err := C.dmesh_post_send(c.qp, dst, C.uint32_t(n))
		if rc != 0 {
			t.mu.Unlock()
			return written, err
		}
		written += n
		t.mu.Unlock()
	}
	return written, nil
}
func (c *Conn) closeLocked(abort bool) error {
	if c.closed {
		return nil
	}
	c.closed = true
	for i := range c.rx {
		C.dmesh_go_release(c.t.ch, c.rx[i].token)
	}
	c.rx = nil
	c.notify()
	delete(c.t.conns, c.qp)
	var rc C.int
	var err error
	if abort {
		rc, err = C.dmesh_abort_qp(c.qp)
	} else {
		rc, err = C.dmesh_destroy_qp(c.qp)
	}
	// ABI5 consumes the QP even on error; only channel teardown is retryable.
	c.qp = nil
	c.t.notify()
	if rc != 0 {
		return err
	}
	return nil
}
func (c *Conn) Close() error         { c.t.mu.Lock(); defer c.t.mu.Unlock(); return c.closeLocked(false) }
func (c *Conn) LocalAddr() net.Addr  { return c.local }
func (c *Conn) RemoteAddr() net.Addr { return c.remote }
func (c *Conn) SetDeadline(d time.Time) error {
	c.t.mu.Lock()
	defer c.t.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	c.rd, c.wd = d, d
	c.notify()
	return nil
}
func (c *Conn) SetReadDeadline(d time.Time) error {
	c.t.mu.Lock()
	defer c.t.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	c.rd = d
	c.notify()
	return nil
}
func (c *Conn) SetWriteDeadline(d time.Time) error {
	c.t.mu.Lock()
	defer c.t.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	c.wd = d
	c.notify()
	return nil
}

// servesAt checks that the process's DPUMESH_SERVICE target resolves to
// ip:port, the address the native library serves once the channel is open.
func servesAt(ip string, port int) error {
	target := os.Getenv("DPUMESH_SERVICE")
	host, p, err := net.SplitHostPort(target)
	if err != nil || p != strconv.Itoa(port) {
		return fmt.Errorf("dmesh: DPUMESH_SERVICE %q is not a target on port %d", target, port)
	}
	addrs, err := net.LookupHost(host)
	if err != nil {
		return fmt.Errorf("dmesh: DPUMESH_SERVICE %q: %w", target, err)
	}
	want := net.ParseIP(ip)
	for _, a := range addrs {
		if net.ParseIP(a).Equal(want) {
			return nil
		}
	}
	return fmt.Errorf("dmesh: DPUMESH_SERVICE %q does not resolve to %s", target, net.JoinHostPort(ip, p))
}
func checkConfig(server, pod, workload string) error {
	for _, value := range [][3]string{{"DPUMESH_SERVER", server, "DPUMesh0"}, {"DPUMESH_POD_IP", pod, ""},
		{"DPUMESH_WORKLOAD", workload, ""}, {"DPUMESH_PCI_ADDR", PCIAddr, ""}} {
		if value[1] != "" && value[1] != envOr(value[0], value[2]) {
			return fmt.Errorf("dmesh: argument disagrees with process %s; configure it before opening the channel", value[0])
		}
	}
	return nil
}
func Dial(server, srcIP string, srcPort int, dstIP string, dstPort int, workload string) (net.Conn, error) {
	if err := checkConfig(server, srcIP, workload); err != nil {
		return nil, err
	}
	t, err := openTransport()
	if err != nil {
		return nil, err
	}
	name := C.CString(net.JoinHostPort(dstIP, strconv.Itoa(dstPort)))
	defer C.free(unsafe.Pointer(name))
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.err != nil {
		return nil, t.err
	}
	qp, err := C.dmesh_create_qp(t.eq, name)
	if qp == nil {
		return nil, err
	}
	// Native port allocation owns the stream identifier; srcPort is a caller
	// label only and never overrides the QP allocator.
	c := newConn(t, qp, &net.TCPAddr{IP: net.ParseIP(envOr("DPUMESH_POD_IP", srcIP)), Port: int(qp.local_port)},
		&net.TCPAddr{IP: net.ParseIP(dstIP), Port: dstPort})
	t.conns[qp] = c
	t.wakePoller()
	return c, nil
}

type Listener struct {
	t       *transport
	addr    net.Addr
	pending []*Conn
	closed  bool
}

func Listen(server, svcIP string, svcPort int, workload string) (*Listener, error) {
	if err := checkConfig(server, "", workload); err != nil {
		return nil, err
	}
	if err := servesAt(svcIP, svcPort); err != nil {
		return nil, err
	}
	return listen(&net.TCPAddr{IP: net.ParseIP(svcIP), Port: svcPort})
}

// serviceAddr is the listener address of the DPUMESH_SERVICE target: the
// Service port, and the IP only when the target is an IPv4 literal.
func serviceAddr() (*net.TCPAddr, error) {
	target := os.Getenv("DPUMESH_SERVICE")
	host, p, err := net.SplitHostPort(target)
	port, perr := strconv.Atoi(p)
	if err != nil || host == "" || perr != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf("dmesh: DPUMESH_SERVICE %q is not a \"<host>:<port>\" target", target)
	}
	return &net.TCPAddr{IP: net.ParseIP(host).To4(), Port: port}, nil
}

// ListenService serves the process's DPUMESH_SERVICE target. The native
// library resolves the target when it opens the channel, so the caller names
// no address, as a server behind a sidecar binds only its own port.
func ListenService() (*Listener, error) {
	addr, err := serviceAddr()
	if err != nil {
		return nil, err
	}
	return listen(addr)
}

func listen(addr net.Addr) (*Listener, error) {
	t, err := openTransport()
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.err != nil {
		return nil, t.err
	}
	if t.listener != nil {
		return nil, syscall.EADDRINUSE
	}
	l := &Listener{t: t, addr: addr}
	t.listener = l
	return l, nil
}
func (l *Listener) Accept() (net.Conn, error) {
	for {
		l.t.mu.Lock()
		if l.closed {
			l.t.mu.Unlock()
			return nil, net.ErrClosed
		}
		if len(l.pending) != 0 {
			c := l.pending[0]
			l.pending[0] = nil
			l.pending = l.pending[1:]
			l.t.mu.Unlock()
			return c, nil
		}
		if l.t.err != nil {
			err := l.t.err
			l.t.mu.Unlock()
			return nil, err
		}
		l.t.park(l.t.changed, time.Time{})
	}
}
func (l *Listener) Close() error {
	l.t.mu.Lock()
	defer l.t.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	l.t.listener = nil
	var closeErr error
	for _, c := range l.pending {
		closeErr = errors.Join(closeErr, c.closeLocked(true))
	}
	l.pending = nil
	l.t.notify()
	return closeErr
}
func (l *Listener) Addr() net.Addr { return l.addr }

// DialAddress opens a logical stream using the process registration. The DPU
// chooses a live backend for the service at the configured virtual address.
func DialAddress(ip string, port int) (net.Conn, error) {
	return Dial("", "", 0, ip, port, "")
}

// DialContext is the gRPC ContextDialer entry point. Native registration has
// its own bounded setup timeout; a cancelled caller never receives a live QP.
func DialContext(ctx context.Context, ip string, port int) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c, err := DialAddress(ip, port)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// ListenAddress serves the process's DPUMESH_SERVICE target after checking
// that it resolves to ip:port.
//
// Deprecated: use ListenService, which needs no ClusterIP.
func ListenAddress(ip string, port int) (*Listener, error) {
	return Listen("", ip, port, "")
}
