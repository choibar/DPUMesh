// Package dmeshgo adapts the native channel/QP socket API to net.Conn and
// net.Listener. One process owns one channel, its listening socket. Each
// connection owns a QP: a Read or Write that has to wait polls that QP itself
// and parks on its fd through Go's netpoller, so the goroutine waiting for
// the bytes is the one the DPU's completion wakes, as with a TCP socket. A
// channel goroutine polls the channel: it does the channel's control work
// and, on a serving channel, accepts each inbound stream and hands it to the
// listener. A connection's state has its own lock; the transport lock guards
// only the connection table, so no connection waits for another's I/O.
//
// Configure DPUMESH_PCI_ADDR, DPUMESH_SERVER, DPUMESH_POD_IP and, for a
// listener (ListenService), the DPUMESH_SERVICE "<host>:<port>" target before
// starting Go.
package dmeshgo

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

// errWouldBlock reports a QP without transmit capacity; a TX_READY event
// follows once capacity returns.
var errWouldBlock = errors.New("dmesh: transmit would block")

type eventKind uint8

const (
	evUnknown eventKind = iota
	evRecv
	evFin
	evTxReady
	evTxError
	evPrepare
	evScale
)

// qpID is a native QP handle, used as an opaque key.
type qpID = unsafe.Pointer

type nativeEvent struct {
	kind  eventKind
	buf   []byte // evRecv: native memory, valid until release(token)
	token int32  // receive lease; -1 when the event holds none

	target     string // evPrepare, evScale: "ip:port"
	count      int    // evScale: streams each client keeps to target
	scaleToken uint32 // evScale: answered by scaleAck
}

// native is the libdpumesh surface. A QP has one poller at a time: pollWait
// and destroy run on whichever goroutine currently owns it. The channel
// goroutine alone calls accept and waitChannel. send is serialized per QP;
// the others may run on any goroutine.
type native interface {
	postMax() int
	// serves reports a channel that receives inbound streams for its
	// DPUMESH_SERVICE target.
	serves() bool
	// pollWait returns qp's events, or 0 after wake(qp) or maxWaitNs
	// (< 0: unbounded).
	pollWait(qp qpID, events []nativeEvent, maxWaitNs int64) (int, error)
	wake(qp qpID)
	// dial connects a QP; what the peer sends before it returns waits in it.
	dial(target string) (qp qpID, localPort int, err error)
	// accept does the channel's control work and returns the next inbound
	// stream, whose first bytes its own poll returns. syscall.EAGAIN when
	// none waits.
	accept() (qp qpID, remotePort int, err error)
	// waitChannel returns once the channel has work for accept, or after
	// wakeChannel.
	waitChannel() error
	wakeChannel()
	send(qp qpID, p []byte) (int, error) // all of p, or errWouldBlock
	release(token int32)
	destroy(qp qpID, abort bool) error // consumes qp and its fd even on error
	close() error                      // retryable
	spareReady(qp qpID) error          // migration: qp is a ready spare
	scaleAck(qp qpID, token uint32, applied int) error
}

const (
	// Events one connection's poll returns.
	connBatch = 16
	// Inbound streams held for a listener that has not been created yet.
	maxPrelisten = 64
)

type transport struct {
	n       native
	postMax int
	serves  bool

	mu       sync.Mutex
	conns    map[qpID]*Conn
	listener *Listener
	// Inbound streams that arrived before the first listener; handed to it.
	// Once a listener has existed, a stream with none is refused.
	prelisten []*Conn
	listened  bool
	dials     int               // native dials in flight
	spares    map[string]*spare // migration spares by "ip:port"
	err       error             // sticky: transport failure or closed
	failed    chan struct{}     // closed when err is first set

	// The channel goroutine. QPs never do the channel's work, so it polls
	// the channel for the transport's whole life: control messages, the
	// session's liveness and, serving, inbound streams.
	stop     chan struct{} // closed to stop the channel goroutine
	stopOnce sync.Once
	done     chan struct{} // closed when the channel goroutine exits (at once without one)
}

var process struct {
	sync.Mutex
	t *transport
}

func newTransport(n native) *transport {
	t := &transport{n: n, postMax: n.postMax(), serves: n.serves(), conns: make(map[qpID]*Conn),
		spares: make(map[string]*spare),
		failed: make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{})}
	if t.postMax <= 0 {
		t.failLocked(fmt.Errorf("dmesh: native post size %d", t.postMax))
		close(t.done)
		return t
	}
	go t.channelLoop()
	return t
}

// failedTransport keeps a native whose cleanup failed, for CloseTransport to
// retry; it opens nothing new.
func failedTransport(n native, err error) *transport {
	t := &transport{n: n, conns: make(map[qpID]*Conn), err: err,
		failed: make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{})}
	close(t.failed)
	close(t.done)
	return t
}

func openTransport() (*transport, error) {
	process.Lock()
	defer process.Unlock()
	if process.t != nil {
		return process.t, nil
	}
	n, err := openCgoNative()
	if err != nil {
		if n != nil {
			process.t = failedTransport(n, err)
		}
		return nil, err
	}
	process.t = newTransport(n)
	return process.t, nil
}

// failLocked latches the first transport error. Requires t.mu.
func (t *transport) failLocked(err error) {
	if t.err == nil {
		t.err = err
		close(t.failed)
	}
}

func (t *transport) fail(err error) {
	t.mu.Lock()
	t.failLocked(err)
	conns := make([]*Conn, 0, len(t.conns))
	for _, c := range t.conns {
		conns = append(conns, c)
	}
	l := t.listener
	t.mu.Unlock()
	for _, c := range conns {
		c.fail(err)
	}
	if l != nil {
		l.signal()
	}
}

// channelLoop is the channel's only poller: each inbound stream is accepted
// or promptly refused. After a transport failure it waits for close, so
// Close still returns.
func (t *transport) channelLoop() {
	defer close(t.done)
	for {
		select {
		case <-t.stop:
			return
		default:
		}
		qp, port, err := t.n.accept()
		if err == nil {
			t.admit(qp, port)
			continue
		}
		if errors.Is(err, syscall.EAGAIN) {
			err = t.n.waitChannel()
		}
		if err != nil {
			t.fail(fmt.Errorf("dmesh: accept: %w", err))
			<-t.stop
			return
		}
	}
}

// admit opens the connection of an accepted stream and hands it on, or
// aborts the stream when no listener can take it.
func (t *transport) admit(qp qpID, remotePort int) {
	t.mu.Lock()
	take := t.err == nil && (t.listener != nil || (!t.listened && len(t.prelisten) < maxPrelisten))
	t.mu.Unlock()
	if !take {
		_ = t.n.destroy(qp, true)
		return
	}
	t.hand(newConn(t, qp, nil, &net.TCPAddr{Port: remotePort}))
}

// hand gives an inbound connection to the listener, holds it for the first
// one, or aborts it when neither is possible any more.
func (t *transport) hand(c *Conn) {
	t.mu.Lock()
	switch l := t.listener; {
	case t.err != nil:
	case l != nil:
		c.local = l.addr
		t.conns[c.qp] = c
		l.pending = append(l.pending, c)
		t.mu.Unlock()
		l.signal()
		return
	case !t.listened && len(t.prelisten) < maxPrelisten:
		t.conns[c.qp] = c
		t.prelisten = append(t.prelisten, c)
		t.mu.Unlock()
		return
	}
	t.mu.Unlock()
	_ = c.shutdown(true)
}

// retire forgets a closed connection.
func (t *transport) retire(c *Conn) {
	t.mu.Lock()
	if t.conns[c.qp] == c {
		delete(t.conns, c.qp)
	}
	t.mu.Unlock()
}

// dial opens a stream. The native dial waits for the DPU's answer and runs
// outside every lock; a cancelable ctx returns at once and the late QP, if
// any, is aborted. Events of a dialed QP wait in it until its connection
// first polls, so the peer may speak before the dial returns.
func (t *transport) dial(ctx context.Context, target string, local, remote *net.TCPAddr) (*Conn, error) {
	t.mu.Lock()
	if t.err != nil {
		err := t.err
		t.mu.Unlock()
		return nil, err
	}
	t.dials++
	t.mu.Unlock()
	if ctx.Done() == nil {
		qp, port, err := t.n.dial(target)
		return t.adopt(ctx, qp, port, err, local, remote)
	}
	type result struct {
		qp   qpID
		port int
		err  error
	}
	res := make(chan result, 1)
	go func() {
		qp, port, err := t.n.dial(target)
		res <- result{qp, port, err}
	}()
	select {
	case r := <-res:
		return t.adopt(ctx, r.qp, r.port, r.err, local, remote)
	case <-ctx.Done():
		go func() {
			r := <-res
			t.abandon(r.qp, r.err)
		}()
		return nil, ctx.Err()
	}
}

func (t *transport) adopt(ctx context.Context, qp qpID, port int, err error, local, remote *net.TCPAddr) (*Conn, error) {
	t.mu.Lock()
	t.dials--
	late := false
	if err == nil {
		if err = t.err; err == nil {
			err = ctx.Err()
		}
		late = err != nil
	}
	if err != nil {
		t.mu.Unlock()
		if late {
			_ = t.n.destroy(qp, true)
		}
		return nil, err
	}
	local.Port = port
	c := newConn(t, qp, local, remote)
	t.conns[qp] = c
	t.mu.Unlock()
	return c, nil
}

func (t *transport) abandon(qp qpID, err error) {
	t.mu.Lock()
	t.dials--
	t.mu.Unlock()
	if err == nil {
		_ = t.n.destroy(qp, true)
	}
}

func (t *transport) listen(addr net.Addr) (*Listener, error) {
	t.mu.Lock()
	if t.err != nil {
		err := t.err
		t.mu.Unlock()
		return nil, err
	}
	if t.listener != nil {
		t.mu.Unlock()
		return nil, syscall.EADDRINUSE
	}
	l := &Listener{t: t, addr: addr, sig: make(chan struct{}, 1), closedCh: make(chan struct{})}
	for _, c := range t.prelisten {
		c.local = addr
	}
	l.pending, t.prelisten = t.prelisten, nil
	t.listener, t.listened = l, true
	t.mu.Unlock()
	if len(l.pending) != 0 {
		l.signal()
	}
	return l, nil
}

// CloseTransport closes the idle process channel. Call after closing all
// listeners and connections; live objects cause EBUSY without invalidation.
// A failed native cleanup keeps the channel, rejects new connections and can
// be retried.
func CloseTransport() error {
	process.Lock()
	defer process.Unlock()
	t := process.t
	if t == nil {
		return nil
	}
	if err := t.close(); err != nil {
		return err
	}
	process.t = nil
	return nil
}

// close stops the channel goroutine of an idle transport, then closes the native. A
// failed native close keeps the transport closed to new work and retryable.
// Inbound streams no listener ever took, and migration spares no dial took,
// belong to the transport and are closed here.
func (t *transport) close() error {
	t.mu.Lock()
	unclaimed := t.prelisten
	t.prelisten = nil
	spares := t.spares
	t.spares = make(map[string]*spare)
	t.mu.Unlock()
	for _, c := range unclaimed {
		_ = c.shutdown(true)
	}
	for _, s := range spares {
		s.expiry.Stop()
		_ = s.conn.Close()
	}
	t.mu.Lock()
	if len(t.conns) != 0 || t.listener != nil || t.dials != 0 {
		t.mu.Unlock()
		return syscall.EBUSY
	}
	t.failLocked(net.ErrClosed)
	t.mu.Unlock()
	t.stopOnce.Do(func() { close(t.stop) })
	t.n.wakeChannel()
	<-t.done
	return t.n.close()
}

// rxLease is one native receive fragment, released once fully read.
type rxLease struct {
	buf   []byte
	token int32
}

// Conn is a DPUMesh byte stream. Read and Write may run concurrently with each
// other and with Close, as net.Conn requires.
type Conn struct {
	t             *transport
	qp            qpID
	local, remote net.Addr

	readMu, writeMu sync.Mutex // serialize readers; serialize writers
	done            []int32    // reader-owned: leases consumed by one Read

	// The QP's single poller: a waiting Read or Write that takes pollMu
	// polls the QP and parks on its fd; the other waits for its signal.
	pollMu sync.Mutex
	events []nativeEvent // under pollMu

	mu     sync.Mutex
	rx     []rxLease
	pos    int // bytes of rx[0] already read
	eof    bool
	err    error // sticky transport error
	closed bool
	rd, wd time.Time

	rdSig, wrSig chan struct{} // capacity 1: something changed for the reader / writer
	closedCh     chan struct{}

	closeOnce sync.Once
	closeErr  error
}

func newConn(t *transport, qp qpID, local, remote net.Addr) *Conn {
	return &Conn{t: t, qp: qp, local: local, remote: remote, events: make([]nativeEvent, connBatch),
		rdSig: make(chan struct{}, 1), wrSig: make(chan struct{}, 1), closedCh: make(chan struct{})}
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (c *Conn) signalReader() { signal(c.rdSig) }
func (c *Conn) signalWriter() { signal(c.wrSig) }

// apply takes one event of this connection's QP.
func (c *Conn) apply(ev *nativeEvent) {
	switch ev.kind {
	case evRecv:
		if !c.deliver(ev.buf, ev.token) {
			c.t.n.release(ev.token)
		}
	case evFin:
		c.peerClosed()
	case evTxReady:
		c.signalWriter()
	case evTxError:
		c.fail(syscall.EIO)
	case evPrepare:
		// The DPU moves this stream: a spare to its target takes over. Each
		// request runs on its own goroutine, since it opens streams.
		go c.t.prepareSpare(ev.target)
	case evScale:
		go c.t.scale(c.qp, ev.target, ev.count, ev.scaleToken)
	default:
		if ev.token >= 0 {
			c.t.n.release(ev.token)
		}
	}
}

// deliver queues one receive lease. It reports false when the connection no
// longer takes data; the caller then returns the lease.
func (c *Conn) deliver(buf []byte, token int32) bool {
	if len(buf) == 0 {
		return false
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false
	}
	c.rx = append(c.rx, rxLease{buf: buf, token: token})
	c.mu.Unlock()
	c.signalReader()
	return true
}

// peerClosed records the peer's FIN. Queued data stays readable. A writer
// waiting for capacity is woken: a departed peer returns no more credit.
func (c *Conn) peerClosed() {
	c.mu.Lock()
	c.eof = true
	c.mu.Unlock()
	c.signalReader()
	c.signalWriter()
}

func (c *Conn) fail(err error) {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
	}
	c.mu.Unlock()
	c.signalReader()
	c.signalWriter()
}

// await waits until something may have changed for its caller. A caller
// that finds the QP free polls it, parking on its fd until events, the
// deadline, Close or a wake; then it signals the other side, which may have
// waited for this poll and must now poll for itself. A caller that finds it
// taken waits for that poller's signal on own.
func (c *Conn) await(own, other chan struct{}, deadline time.Time) error {
	if !c.pollMu.TryLock() {
		return c.wait(own, deadline)
	}
	err := c.poll(deadline)
	c.pollMu.Unlock()
	signal(other)
	return err
}

// poll takes one batch of the QP, parking while it is empty. Requires pollMu.
// A Close that ran before pollMu was taken is seen here; one that runs after
// wakes the parked poll.
func (c *Conn) poll(deadline time.Time) error {
	maxWait := int64(-1)
	if !deadline.IsZero() {
		left := time.Until(deadline)
		if left <= 0 {
			return timeoutError{}
		}
		maxWait = int64(left)
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil
	}
	n, err := c.t.n.pollWait(c.qp, c.events, maxWait)
	if err != nil {
		c.t.fail(fmt.Errorf("dmesh: poll QP: %w", err))
		return nil
	}
	for i := range c.events[:n] {
		c.apply(&c.events[i])
		c.events[i] = nativeEvent{}
	}
	if n == 0 && expired(deadline) {
		return timeoutError{}
	}
	return nil
}

// wait sleeps until sig, Close, a transport failure or the deadline.
func (c *Conn) wait(sig chan struct{}, deadline time.Time) error {
	if deadline.IsZero() {
		select {
		case <-sig:
		case <-c.closedCh:
		case <-c.t.failed:
		}
		return nil
	}
	left := time.Until(deadline)
	if left <= 0 {
		return timeoutError{}
	}
	timer := time.NewTimer(left)
	defer timer.Stop()
	select {
	case <-sig:
	case <-c.closedCh:
	case <-c.t.failed:
	case <-timer.C:
		return timeoutError{}
	}
	return nil
}

func expired(deadline time.Time) bool { return !deadline.IsZero() && !time.Now().Before(deadline) }

func (c *Conn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return 0, net.ErrClosed
		}
		if len(p) == 0 {
			c.mu.Unlock()
			return 0, nil
		}
		if expired(c.rd) {
			c.mu.Unlock()
			return 0, timeoutError{}
		}
		if len(c.rx) != 0 {
			n := c.takeLocked(p)
			c.mu.Unlock()
			for _, token := range c.done {
				c.t.n.release(token)
			}
			c.done = c.done[:0]
			return n, nil
		}
		err := c.err
		if err == nil && c.eof {
			err = io.EOF
		}
		deadline := c.rd
		c.mu.Unlock()
		if err != nil {
			return 0, err
		}
		if err := c.await(c.rdSig, c.wrSig, deadline); err != nil {
			return 0, err
		}
	}
}

// takeLocked copies queued leases into p and moves the fully read ones to
// c.done for release after c.mu is dropped. The rest move to the front of the
// queue's array, which deliver keeps appending to. Requires c.mu and c.readMu.
func (c *Conn) takeLocked(p []byte) int {
	n, read := 0, 0
	for n < len(p) && read < len(c.rx) {
		lease := &c.rx[read]
		k := copy(p[n:], lease.buf[c.pos:])
		n += k
		c.pos += k
		if c.pos == len(lease.buf) {
			c.done = append(c.done, lease.token)
			read++
			c.pos = 0
		}
	}
	if read != 0 {
		left := copy(c.rx, c.rx[read:])
		clear(c.rx[left:])
		c.rx = c.rx[:left]
	}
	return n
}

func (c *Conn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	written := 0
	for written < len(p) {
		c.mu.Lock()
		err := c.err
		switch {
		case c.closed:
			err = net.ErrClosed
		case expired(c.wd):
			err = timeoutError{}
		}
		deadline, eof := c.wd, c.eof
		c.mu.Unlock()
		if err != nil {
			return written, err
		}
		chunk := p[written:]
		if len(chunk) > c.t.postMax {
			chunk = chunk[:c.t.postMax]
		}
		n, err := c.t.n.send(c.qp, chunk)
		if err == nil {
			written += n
			continue
		}
		if !errors.Is(err, errWouldBlock) {
			c.fail(err)
			return written, err
		}
		if eof {
			return written, syscall.EPIPE
		}
		// The space comes back with custody ACKs, which only a poll of
		// the QP retires: wait as its poller when nobody else is.
		if err := c.await(c.wrSig, c.rdSig, deadline); err != nil {
			return written, err
		}
	}
	return written, nil
}

// Close ends the stream: it wakes blocked Read and Write calls, returns queued
// leases and destroys the QP, reporting the native close result once. The
// native close consumes the QP even when that fails; later calls return nil.
func (c *Conn) Close() error { return c.shutdown(false) }

func (c *Conn) shutdown(abort bool) error {
	first := false
	c.closeOnce.Do(func() {
		first = true
		c.mu.Lock()
		c.closed = true
		leases := c.rx
		c.rx, c.pos = nil, 0
		c.mu.Unlock()
		close(c.closedCh)
		c.t.n.wake(c.qp)
		// A Read copying out of a lease, a Write inside a native send and
		// a poll of the QP finish before the QP goes away; all return
		// promptly once closed.
		c.readMu.Lock()
		c.writeMu.Lock()
		c.pollMu.Lock()
		for _, lease := range leases {
			c.t.n.release(lease.token)
		}
		c.closeErr = c.t.n.destroy(c.qp, abort)
		c.pollMu.Unlock()
		c.writeMu.Unlock()
		c.readMu.Unlock()
		c.t.retire(c)
	})
	if !first {
		return nil
	}
	return c.closeErr
}

func (c *Conn) LocalAddr() net.Addr  { return c.local }
func (c *Conn) RemoteAddr() net.Addr { return c.remote }

func (c *Conn) setDeadlines(read, write bool, d time.Time) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return net.ErrClosed
	}
	if read {
		c.rd = d
	}
	if write {
		c.wd = d
	}
	c.mu.Unlock()
	// A waiter recomputes its timer; a poller parks again with the new one.
	if read {
		c.signalReader()
	}
	if write {
		c.signalWriter()
	}
	c.t.n.wake(c.qp)
	return nil
}

func (c *Conn) SetDeadline(d time.Time) error      { return c.setDeadlines(true, true, d) }
func (c *Conn) SetReadDeadline(d time.Time) error  { return c.setDeadlines(true, false, d) }
func (c *Conn) SetWriteDeadline(d time.Time) error { return c.setDeadlines(false, true, d) }

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

func dialContext(ctx context.Context, server, srcIP string, dstIP string, dstPort int, workload string) (net.Conn, error) {
	if err := checkConfig(server, srcIP, workload); err != nil {
		return nil, err
	}
	t, err := openTransport()
	if err != nil {
		return nil, err
	}
	// Native port allocation owns the stream identifier; a source port is a
	// caller label only and never overrides the QP allocator.
	local := &net.TCPAddr{IP: net.ParseIP(envOr("DPUMESH_POD_IP", srcIP))}
	remote := &net.TCPAddr{IP: net.ParseIP(dstIP), Port: dstPort}
	target := net.JoinHostPort(dstIP, strconv.Itoa(dstPort))
	if c := t.takeSpare(target); c != nil {
		return c, nil
	}
	c, err := t.dial(ctx, target, local, remote)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func Dial(server, srcIP string, srcPort int, dstIP string, dstPort int, workload string) (net.Conn, error) {
	return dialContext(context.Background(), server, srcIP, dstIP, dstPort, workload)
}

// Listener accepts the DPUMesh streams the DPU routes to this process.
type Listener struct {
	t        *transport
	addr     net.Addr
	pending  []*Conn // under t.mu
	closed   bool    // under t.mu
	sig      chan struct{}
	closedCh chan struct{}
}

func (l *Listener) signal() { signal(l.sig) }

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
	return t.listen(addr)
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
		if err := l.t.err; err != nil {
			l.t.mu.Unlock()
			return nil, err
		}
		l.t.mu.Unlock()
		select {
		case <-l.sig:
		case <-l.closedCh:
		case <-l.t.failed:
		}
	}
}

// Close stops accepting and aborts streams accepted natively but not yet
// returned by Accept.
func (l *Listener) Close() error {
	l.t.mu.Lock()
	if l.closed {
		l.t.mu.Unlock()
		return nil
	}
	l.closed = true
	if l.t.listener == l {
		l.t.listener = nil
	}
	pending := l.pending
	l.pending = nil
	l.t.mu.Unlock()
	close(l.closedCh)
	var closeErr error
	for _, c := range pending {
		closeErr = errors.Join(closeErr, c.shutdown(true))
	}
	return closeErr
}
func (l *Listener) Addr() net.Addr { return l.addr }

// DialAddress opens a logical stream using the process registration. The DPU
// chooses a live backend for the service at the configured virtual address.
func DialAddress(ip string, port int) (net.Conn, error) {
	return Dial("", "", 0, ip, port, "")
}

// DialContext is the gRPC ContextDialer entry point. The native dial waits for
// the DPU's answer; a ctx that ends first returns its error at once and the
// late stream is aborted, so a cancelled caller never receives a live QP.
func DialContext(ctx context.Context, ip string, port int) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return dialContext(ctx, "", "", ip, port, "")
}

// ListenAddress serves the process's DPUMESH_SERVICE target after checking
// that it resolves to ip:port.
//
// Deprecated: use ListenService, which needs no ClusterIP.
func ListenAddress(ip string, port int) (*Listener, error) {
	return Listen("", ip, port, "")
}
