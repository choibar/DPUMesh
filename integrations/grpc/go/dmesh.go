// Package dmeshgo adapts the native channel/EQ/QP API to net.Conn and
// net.Listener. One process owns one channel, one EQ and one poller
// goroutine, the EQ's single consumer: it drains events, hands receive leases
// to their connections and runs every QP destruction, which the native API
// serializes with polling. Reads, writes and dials run on the caller's
// goroutine. A connection's state has its own lock; the transport lock guards
// only the connection table, so no connection waits for another's I/O.
//
// Configure DPUMESH_PCI_ADDR, DPUMESH_SERVER and, for a listener
// (ListenService), DPUMESH_PORT, which the native library serves on the Pod IP,
// or a DPUMESH_SERVICE "<host>:<port>" target before starting Go. Outside a Pod
// also set DPUMESH_POD_IP.
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

// qpID is a native QP handle, used as an opaque key.
type qpID = unsafe.Pointer

type nativeEvent struct {
	kind  eventKind
	qp    qpID
	buf   []byte // evRecv: native memory, valid until release(token)
	token int32  // receive lease; -1 when the event holds none

	localPort, remotePort int // evConnReq
}

// native is the libdpumesh surface. pollWait, destroy and close run only on
// the poller goroutine (close after it exits); the others may run on any
// goroutine, send serialized per QP.
type native interface {
	postMax() int
	// serves reports a channel that receives inbound streams for its
	// DPUMESH_SERVICE target.
	serves() bool
	// pollWait returns events, or 0 after wake or maxWaitNs (< 0: unbounded).
	pollWait(events []nativeEvent, maxWaitNs int64) (int, error)
	wake()
	dial(target string) (qp qpID, localPort int, err error)
	send(qp qpID, p []byte) (int, error) // all of p, or errWouldBlock
	release(token int32)
	destroy(qp qpID, abort bool) error // consumes qp even on error
	close() error                      // retryable
}

const (
	eventBatch = 64
	// Inbound streams held for a listener that has not been created yet.
	maxPrelisten = 64
	// A live transport returns to Go at least this often even when the EQ
	// stays empty. The EQ fd carries the library's naps, tail deadlines and
	// doorbells, and commands raise the wake fd, so this is only a backstop;
	// a short bound would wake an idle process on every expiry.
	activeWaitNs = int64(time.Second)
)

type earlyEvents struct {
	rx  []rxLease
	eof bool
	err error
}

type command struct {
	qp    qpID
	abort bool
	done  chan error // nil: nobody waits for the result
}

type transport struct {
	n       native
	postMax int
	serves  bool
	events  []nativeEvent // poller only
	orphans []int32       // poller only
	rejects []qpID        // poller only
	kick    chan struct{} // wakes a poller parked in Go

	mu       sync.Mutex
	conns    map[qpID]*Conn
	listener *Listener
	// Inbound streams that arrived before the first listener; handed to it.
	// Once a listener has existed, a stream with none is refused.
	prelisten []*Conn
	listened  bool
	dials     int // native dials in flight; they use the EQ
	// A dialed QP can deliver events before its dial returns and registers
	// it: the peer may speak first (an HTTP/2 server sends SETTINGS at once).
	// While a dial is in flight such events are held here and adopted with
	// the connection. QPs being destroyed are listed in closing, so their
	// late events are returned instead of held.
	early    map[qpID]*earlyEvents
	closing  map[qpID]struct{}
	cmds     []command
	stopping bool
	err      error         // sticky: transport failure or closed
	failed   chan struct{} // closed when err is first set

	done chan struct{} // closed when the poller exits
}

var process struct {
	sync.Mutex
	t *transport
}

func newTransport(n native) *transport {
	t := &transport{n: n, postMax: n.postMax(), serves: n.serves(), events: make([]nativeEvent, eventBatch),
		kick: make(chan struct{}, 1), conns: make(map[qpID]*Conn),
		early: make(map[qpID]*earlyEvents), closing: make(map[qpID]struct{}),
		failed: make(chan struct{}), done: make(chan struct{})}
	if t.postMax <= 0 {
		t.err = fmt.Errorf("dmesh: native post size %d", t.postMax)
		close(t.failed)
	}
	go t.run()
	return t
}

// failedTransport keeps a native whose cleanup failed, for CloseTransport to
// retry; it opens nothing new.
func failedTransport(n native, err error) *transport {
	t := &transport{n: n, conns: make(map[qpID]*Conn), early: make(map[qpID]*earlyEvents),
		closing: make(map[qpID]struct{}), err: err,
		failed: make(chan struct{}), done: make(chan struct{}), stopping: true}
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

func (t *transport) wakePoller() {
	select {
	case t.kick <- struct{}{}:
	default:
	}
	t.n.wake()
}

// failLocked latches the first transport error. Requires t.mu.
func (t *transport) failLocked(err error) {
	if t.err == nil {
		t.err = err
		close(t.failed)
	}
}

// run is the poller: the EQ's only consumer and the only goroutine that
// destroys QPs. It destroys a QP only after dispatching the batch that named
// it, as the native API requires, and keeps running commands after a
// transport failure so Close still returns. A serving channel is polled for
// its whole life, so an inbound stream is accepted or promptly refused;
// otherwise the poller sleeps while no connection exists.
func (t *transport) run() {
	defer close(t.done)
	for {
		// Commands and the stop flag are read together: a command is queued
		// in the same critical section that removes its connection or dial,
		// so close() cannot stop the poller while one is waiting.
		t.mu.Lock()
		cmds := t.cmds
		t.cmds = nil
		stop := t.stopping
		failed := t.err != nil
		active := len(t.conns) != 0 || t.listener != nil || t.serves
		t.mu.Unlock()
		if len(cmds) != 0 {
			for _, cmd := range cmds {
				err := t.n.destroy(cmd.qp, cmd.abort)
				t.retire(cmd.qp)
				if cmd.done != nil {
					cmd.done <- err
				}
			}
			continue // recheck before sleeping: a command may have queued more
		}
		if stop {
			return
		}
		if failed || !active {
			<-t.kick
			continue
		}
		n, err := t.n.pollWait(t.events, activeWaitNs)
		if err != nil {
			t.fail(fmt.Errorf("dmesh: poll EQ: %w", err))
			continue
		}
		t.dispatch(t.events[:n])
	}
}

// retire forgets a destroyed QP. The native produces no further events for
// it, and events held for a dial that never adopted it are returned.
func (t *transport) retire(qp qpID) {
	t.mu.Lock()
	delete(t.closing, qp)
	held := t.early[qp]
	delete(t.early, qp)
	t.mu.Unlock()
	if held != nil {
		for _, lease := range held.rx {
			t.n.release(lease.token)
		}
	}
}

// holdLocked keeps an event for a QP whose dial has not returned yet. It
// reports false when no dial can claim it. Requires t.mu.
func (t *transport) holdLocked(ev *nativeEvent) bool {
	if t.dials == 0 {
		return false
	}
	if _, gone := t.closing[ev.qp]; gone {
		return false
	}
	held := t.early[ev.qp]
	if held == nil {
		held = &earlyEvents{}
		t.early[ev.qp] = held
	}
	switch ev.kind {
	case evRecv:
		if len(ev.buf) == 0 {
			return false
		}
		held.rx = append(held.rx, rxLease{buf: ev.buf, token: ev.token})
	case evFin:
		held.eof = true
	case evTxError:
		if held.err == nil {
			held.err = syscall.EIO
		}
	}
	return true
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

// dispatch hands one batch to its connections. Leases that no connection
// takes, and inbound QPs that no listener accepts, are returned after the
// whole batch has been seen.
func (t *transport) dispatch(events []nativeEvent) {
	orphans, rejects := t.orphans[:0], t.rejects[:0]
	var accepted *Listener
	t.mu.Lock()
	for i := range events {
		ev := &events[i]
		c := t.conns[ev.qp]
		if c == nil && ev.kind != evConnReq && t.holdLocked(ev) {
			continue
		}
		switch ev.kind {
		case evConnReq:
			if c != nil {
				break
			}
			l := t.listener
			switch {
			case l != nil:
				c = newConn(t, ev.qp, l.addr, &net.TCPAddr{Port: ev.remotePort})
				t.conns[ev.qp] = c
				l.pending = append(l.pending, c)
				accepted = l
			case !t.listened && len(t.prelisten) < maxPrelisten:
				c = newConn(t, ev.qp, nil, &net.TCPAddr{Port: ev.remotePort})
				t.conns[ev.qp] = c
				t.prelisten = append(t.prelisten, c)
			default:
				rejects = append(rejects, ev.qp)
				t.closing[ev.qp] = struct{}{}
			}
		case evRecv:
			if c == nil || !c.deliver(ev.buf, ev.token) {
				orphans = append(orphans, ev.token)
			}
		case evFin:
			if c != nil {
				c.peerClosed()
			}
		case evTxReady:
			if c != nil {
				c.signalWriter()
			}
		case evTxError:
			if c != nil {
				c.fail(syscall.EIO)
			}
		default:
			if ev.token >= 0 {
				orphans = append(orphans, ev.token)
			}
		}
	}
	t.mu.Unlock()
	for _, token := range orphans {
		if token >= 0 {
			t.n.release(token)
		}
	}
	for _, qp := range rejects {
		_ = t.n.destroy(qp, true)
		t.retire(qp)
	}
	if accepted != nil {
		accepted.signal()
	}
	t.orphans, t.rejects = orphans[:0], rejects[:0]
}

// destroy runs the native close on the poller and waits for its result. The
// connection leaves the table first, so events still naming the QP are
// returned as orphans.
func (t *transport) destroy(qp qpID, abort bool) error {
	done := make(chan error, 1)
	t.mu.Lock()
	delete(t.conns, qp)
	t.closing[qp] = struct{}{}
	t.cmds = append(t.cmds, command{qp: qp, abort: abort, done: done})
	t.mu.Unlock()
	t.wakePoller()
	return <-done
}

// dial opens a client QP. The native dial blocks until the DPU answers, so it
// runs outside every lock; a cancelable ctx returns at once and the late QP,
// if any, is aborted.
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
	if err != nil {
		t.mu.Unlock()
		return nil, err
	}
	if err := t.err; err != nil || ctx.Err() != nil {
		if err == nil {
			err = ctx.Err()
		}
		t.closing[qp] = struct{}{}
		t.cmds = append(t.cmds, command{qp: qp, abort: true})
		t.mu.Unlock()
		t.wakePoller()
		return nil, err
	}
	local.Port = port
	c := newConn(t, qp, local, remote)
	t.conns[qp] = c
	held := t.early[qp]
	delete(t.early, qp)
	if held != nil {
		c.rx, c.eof, c.err = held.rx, held.eof, held.err
	}
	t.mu.Unlock()
	if held != nil {
		c.signalReader()
		c.signalWriter()
	}
	t.wakePoller() // the new QP's readiness is armed by the next poll
	return c, nil
}

func (t *transport) abandon(qp qpID, err error) {
	t.mu.Lock()
	t.dials--
	if err == nil {
		t.closing[qp] = struct{}{}
		t.cmds = append(t.cmds, command{qp: qp, abort: true})
	}
	t.mu.Unlock()
	t.wakePoller()
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
	t.wakePoller() // spare backend flows need progress from now on
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

// close stops the poller of an idle transport, then closes the native. A
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
	t.stopping = true
	t.mu.Unlock()
	t.wakePoller()
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
	return &Conn{t: t, qp: qp, local: local, remote: remote,
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
		if err := c.wait(c.rdSig, deadline); err != nil {
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
		if err := c.wait(c.wrSig, deadline); err != nil {
			return written, err
		}
	}
	return written, nil
}

// Close ends the stream: it wakes blocked Read and Write calls, returns queued
// leases and destroys the QP, reporting the native close result once. ABI5
// consumes the QP even when that fails; later calls return nil.
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
		// A Read copying out of a lease and a Write inside a native send
		// finish before the QP goes away; both return promptly once closed.
		c.readMu.Lock()
		c.writeMu.Lock()
		for _, lease := range leases {
			c.t.n.release(lease.token)
		}
		c.closeErr = c.t.destroy(c.qp, abort)
		c.writeMu.Unlock()
		c.readMu.Unlock()
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
	// A waiter recomputes its timer.
	if read {
		c.signalReader()
	}
	if write {
		c.signalWriter()
	}
	return nil
}

func (c *Conn) SetDeadline(d time.Time) error      { return c.setDeadlines(true, true, d) }
func (c *Conn) SetReadDeadline(d time.Time) error  { return c.setDeadlines(true, false, d) }
func (c *Conn) SetWriteDeadline(d time.Time) error { return c.setDeadlines(false, true, d) }

// servesAt checks that the process's DPUMESH_SERVICE target resolves to
// ip:port, the address the native library serves once the channel is open.
// Without a target only DPUMESH_PORT is checked: the library finds the Pod IP.
func servesAt(ip string, port int) error {
	target := os.Getenv("DPUMESH_SERVICE")
	if target == "" {
		if addr, err := serviceAddr(); err != nil || addr.Port != port {
			return fmt.Errorf("dmesh: DPUMESH_PORT %q is not port %d", os.Getenv("DPUMESH_PORT"), port)
		}
		return nil
	}
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

// serviceAddr is the listener address of the target the process serves: the
// DPUMESH_SERVICE port, and the IP only when the target is an IPv4 literal, or
// without a target DPUMESH_PORT, which the native library serves on the Pod IP.
func serviceAddr() (*net.TCPAddr, error) {
	target := os.Getenv("DPUMESH_SERVICE")
	if target == "" {
		port, err := strconv.Atoi(os.Getenv("DPUMESH_PORT"))
		if err != nil || port < 1 || port > 65535 {
			return nil, errors.New("dmesh: a listener needs DPUMESH_PORT or a DPUMESH_SERVICE \"<host>:<port>\" target")
		}
		return &net.TCPAddr{Port: port}, nil
	}
	host, p, err := net.SplitHostPort(target)
	port, perr := strconv.Atoi(p)
	if err != nil || host == "" || perr != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf("dmesh: DPUMESH_SERVICE %q is not a \"<host>:<port>\" target", target)
	}
	return &net.TCPAddr{IP: net.ParseIP(host).To4(), Port: port}, nil
}

// ListenService serves the process's target: DPUMESH_SERVICE, or DPUMESH_PORT
// on the Pod IP. The native library resolves it when it opens the channel, so
// the caller names no address, as a server behind a sidecar binds only its own
// port.
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
