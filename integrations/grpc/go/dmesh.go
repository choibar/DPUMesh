// Package dmeshgo adapts the native channel/EQ/QP API to net.Conn and
// net.Listener. One process owns one channel. Each connection owns a
// connection EQ that only its QP is bound to: a Read or Write that has to
// wait polls that EQ itself and parks on its fd through Go's netpoller, so the
// goroutine waiting for the bytes is the one the DPU's completion wakes, as
// with a TCP socket. The channel's session EQ is polled by a session goroutine:
// it takes the session's control messages and, on a serving channel, moves
// each inbound stream to a connection EQ of its own and hands it to the
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
	evConnReq
	evTxReady
	evTxError
)

// qpID and eqID are native handles, used as opaque keys.
type (
	qpID = unsafe.Pointer
	eqID = unsafe.Pointer
)

type nativeEvent struct {
	kind  eventKind
	qp    qpID
	buf   []byte // evRecv: native memory, valid until release(token)
	token int32  // receive lease; -1 when the event holds none

	localPort, remotePort int // evConnReq
}

// native is the libdpumesh surface. An EQ has one consumer at a time:
// pollWait, moveQP of a QP bound to it, destroy of such a QP and destroyEQ
// run on whichever goroutine currently owns it. send is serialized per QP;
// the others may run on any goroutine.
type native interface {
	postMax() int
	// serves reports a channel that receives inbound streams for its
	// DPUMESH_SERVICE target.
	serves() bool
	// newEQ creates the session EQ, or with conn a connection EQ: one that
	// is woken by and drains only the QPs bound to it.
	newEQ(conn bool) (eqID, error)
	destroyEQ(eq eqID) error // EBUSY while a QP is bound to it
	// pollWait returns eq's events, or 0 after wake(eq) or maxWaitNs
	// (< 0: unbounded).
	pollWait(eq eqID, events []nativeEvent, maxWaitNs int64) (int, error)
	wake(eq eqID)
	dial(eq eqID, target string) (qp qpID, localPort int, err error)
	// moveQP binds an accepted QP to eq: its later events, including those
	// not yet returned by its old EQ, arrive there.
	moveQP(qp qpID, eq eqID) error
	send(qp qpID, p []byte) (int, error) // all of p, or errWouldBlock
	release(token int32)
	destroy(qp qpID, abort bool) error // consumes qp even on error
	close() error                      // retryable
}

const (
	// Events one poll returns: the session EQ's batch, or a connection's.
	sessionBatch = 64
	connBatch    = 16
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
	dials     int           // native dials in flight
	err       error         // sticky: transport failure or closed
	failed    chan struct{} // closed when err is first set

	// The session EQ and its goroutine. Connection EQs never progress the
	// session, so it is polled for the transport's whole life: control
	// messages, the session's liveness and, serving, inbound streams.
	sessionEQ eqID
	stop      chan struct{} // closed to stop the session goroutine
	stopOnce  sync.Once
	done      chan struct{} // closed when the session goroutine exits (at once without one)
}

var process struct {
	sync.Mutex
	t *transport
}

func newTransport(n native) *transport {
	t := &transport{n: n, postMax: n.postMax(), serves: n.serves(), conns: make(map[qpID]*Conn),
		failed: make(chan struct{}), stop: make(chan struct{}), done: make(chan struct{})}
	if t.postMax <= 0 {
		t.failLocked(fmt.Errorf("dmesh: native post size %d", t.postMax))
	} else if eq, err := n.newEQ(false); err == nil {
		t.sessionEQ = eq
		go t.sessionLoop()
		return t
	} else {
		t.failLocked(fmt.Errorf("dmesh: session EQ: %w", err))
	}
	close(t.done)
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

// sessionLoop is the session EQ's only consumer. An inbound stream is
// accepted or promptly refused. After a transport failure it waits for close,
// so Close still returns.
func (t *transport) sessionLoop() {
	defer close(t.done)
	events := make([]nativeEvent, sessionBatch)
	for {
		select {
		case <-t.stop:
			return
		default:
		}
		n, err := t.n.pollWait(t.sessionEQ, events, -1)
		if err != nil {
			t.fail(fmt.Errorf("dmesh: poll EQ: %w", err))
			<-t.stop
			return
		}
		t.admit(events[:n])
	}
}

// admit takes one batch of the session EQ. Each inbound stream a listener can
// take gets a connection EQ of its own; the events that came with it in the
// batch are applied to its connection, and only then is its QP moved, as the
// native API requires. Streams nobody takes are aborted on this goroutine,
// which owns their EQ.
func (t *transport) admit(events []nativeEvent) {
	var fresh []*Conn
	var orphans []int32
	var rejects []qpID
	for i := range events {
		ev := &events[i]
		if ev.kind == evConnReq {
			if c := t.inbound(ev); c != nil {
				fresh = append(fresh, c)
			} else {
				rejects = append(rejects, ev.qp)
			}
			continue
		}
		var c *Conn
		for _, f := range fresh {
			if f.qp == ev.qp {
				c = f
				break
			}
		}
		if c == nil {
			if ev.token >= 0 {
				orphans = append(orphans, ev.token)
			}
			continue
		}
		c.apply(ev)
	}
	for _, token := range orphans {
		t.n.release(token)
	}
	for _, qp := range rejects {
		_ = t.n.destroy(qp, true)
	}
	for _, c := range fresh {
		if err := t.n.moveQP(c.qp, c.eq); err != nil {
			_ = t.n.destroy(c.qp, true)
			_ = t.n.destroyEQ(c.eq)
			continue
		}
		t.hand(c)
	}
}

// inbound opens the connection of a new stream, or nil when no listener can
// take it.
func (t *transport) inbound(ev *nativeEvent) *Conn {
	t.mu.Lock()
	take := t.err == nil && (t.listener != nil || (!t.listened && len(t.prelisten) < maxPrelisten))
	t.mu.Unlock()
	if !take {
		return nil
	}
	eq, err := t.n.newEQ(true)
	if err != nil {
		return nil
	}
	return newConn(t, ev.qp, eq, nil, &net.TCPAddr{Port: ev.remotePort})
}

// hand gives a moved inbound connection to the listener, holds it for the
// first one, or aborts it when neither is possible any more.
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

// dial opens a stream on a connection EQ of its own. The native dial waits for the
// DPU's answer and runs outside every lock; a cancelable ctx returns at once
// and the late QP, if any, is aborted. Events of a dialed QP wait in its EQ
// until its connection first polls, so the peer may speak before the dial
// returns.
func (t *transport) dial(ctx context.Context, target string, local, remote *net.TCPAddr) (*Conn, error) {
	t.mu.Lock()
	if t.err != nil {
		err := t.err
		t.mu.Unlock()
		return nil, err
	}
	t.dials++
	t.mu.Unlock()
	eq, err := t.n.newEQ(true)
	if err != nil {
		t.mu.Lock()
		t.dials--
		t.mu.Unlock()
		return nil, err
	}
	if ctx.Done() == nil {
		qp, port, err := t.n.dial(eq, target)
		return t.adopt(ctx, eq, qp, port, err, local, remote)
	}
	type result struct {
		qp   qpID
		port int
		err  error
	}
	res := make(chan result, 1)
	go func() {
		qp, port, err := t.n.dial(eq, target)
		res <- result{qp, port, err}
	}()
	select {
	case r := <-res:
		return t.adopt(ctx, eq, r.qp, r.port, r.err, local, remote)
	case <-ctx.Done():
		go func() {
			r := <-res
			t.abandon(eq, r.qp, r.err)
		}()
		return nil, ctx.Err()
	}
}

func (t *transport) adopt(ctx context.Context, eq eqID, qp qpID, port int, err error, local, remote *net.TCPAddr) (*Conn, error) {
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
		_ = t.n.destroyEQ(eq)
		return nil, err
	}
	local.Port = port
	c := newConn(t, qp, eq, local, remote)
	t.conns[qp] = c
	t.mu.Unlock()
	return c, nil
}

func (t *transport) abandon(eq eqID, qp qpID, err error) {
	t.mu.Lock()
	t.dials--
	t.mu.Unlock()
	if err == nil {
		_ = t.n.destroy(qp, true)
	}
	_ = t.n.destroyEQ(eq)
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

// close stops the session goroutine of an idle transport, then closes the native. A
// failed native close keeps the transport closed to new work and retryable.
// Inbound streams no listener ever took belong to the transport and are
// aborted here.
func (t *transport) close() error {
	t.mu.Lock()
	unclaimed := t.prelisten
	t.prelisten = nil
	t.mu.Unlock()
	for _, c := range unclaimed {
		_ = c.shutdown(true)
	}
	t.mu.Lock()
	if len(t.conns) != 0 || t.listener != nil || t.dials != 0 {
		t.mu.Unlock()
		return syscall.EBUSY
	}
	t.failLocked(net.ErrClosed)
	t.mu.Unlock()
	t.stopOnce.Do(func() { close(t.stop) })
	if t.sessionEQ != nil {
		t.n.wake(t.sessionEQ)
	}
	<-t.done
	if t.sessionEQ != nil {
		if err := t.n.destroyEQ(t.sessionEQ); err != nil {
			return err
		}
		t.sessionEQ = nil
	}
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
	eq            eqID // this connection's connection EQ
	local, remote net.Addr

	readMu, writeMu sync.Mutex // serialize readers; serialize writers
	done            []int32    // reader-owned: leases consumed by one Read

	// The EQ's single consumer: a waiting Read or Write that takes pollMu
	// polls the EQ and parks on its fd; the other waits for its signal.
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

func newConn(t *transport, qp qpID, eq eqID, local, remote net.Addr) *Conn {
	return &Conn{t: t, qp: qp, eq: eq, local: local, remote: remote, events: make([]nativeEvent, connBatch),
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
// that finds the EQ free polls it, parking on its fd until events, the
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

// poll takes one batch of the EQ, parking while it is empty. Requires pollMu.
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
	n, err := c.t.n.pollWait(c.eq, c.events, maxWait)
	if err != nil {
		c.t.fail(fmt.Errorf("dmesh: poll EQ: %w", err))
		return nil
	}
	for i := range c.events[:n] {
		ev := &c.events[i]
		if ev.qp == c.qp {
			c.apply(ev)
		} else if ev.token >= 0 {
			c.t.n.release(ev.token)
		}
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
// c.done for release after c.mu is dropped. Requires c.mu and c.readMu.
func (c *Conn) takeLocked(p []byte) int {
	n := 0
	for n < len(p) && len(c.rx) != 0 {
		lease := &c.rx[0]
		k := copy(p[n:], lease.buf[c.pos:])
		n += k
		c.pos += k
		if c.pos == len(lease.buf) {
			c.done = append(c.done, lease.token)
			c.rx[0] = rxLease{}
			c.rx = c.rx[1:]
			c.pos = 0
		}
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
		// the EQ retires: wait as its poller when nobody else is.
		if err := c.await(c.wrSig, c.rdSig, deadline); err != nil {
			return written, err
		}
	}
	return written, nil
}

// Close ends the stream: it wakes blocked Read and Write calls, returns queued
// leases and destroys the QP and its EQ, reporting the native close result
// once. ABI5 consumes the QP even when that fails; later calls return nil.
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
		c.t.n.wake(c.eq)
		// A Read copying out of a lease, a Write inside a native send and
		// a poll of the EQ finish before the QP goes away; all return
		// promptly once closed.
		c.readMu.Lock()
		c.writeMu.Lock()
		c.pollMu.Lock()
		for _, lease := range leases {
			c.t.n.release(lease.token)
		}
		c.closeErr = c.t.n.destroy(c.qp, abort)
		if err := c.t.n.destroyEQ(c.eq); err != nil && c.closeErr == nil {
			c.closeErr = err
		}
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
	c.t.n.wake(c.eq)
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
	c, err := t.dial(ctx, net.JoinHostPort(dstIP, strconv.Itoa(dstPort)), local, remote)
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
