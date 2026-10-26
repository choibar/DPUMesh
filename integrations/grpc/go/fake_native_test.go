package dmeshgo

import (
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

// fakeNet is an in-memory stand-in for the DPU path between fake natives. It
// keeps the native contract the adapter relies on: a receive lease holds
// transmit credit until released, a sender past its window gets
// errWouldBlock and later one TX_READY, a destroyed QP sends FIN, and a
// departed peer returns no credit. Events go to the EQ a QP is bound to; an
// inbound stream holds its events until accept binds it to an EQ. Released
// buffers are poisoned so a read after release corrupts the data the tests
// verify.
type fakeNet struct {
	mu      sync.Mutex
	server  *fakeNative
	window  int // bytes a sender may have unreleased at its peer
	frag    int // largest receive fragment
	maxPost int
	token   int32
	leases  map[int32]*fakeLease
	faults  []string // contract violations seen by the fake
	nextUP  int
}

type fakeLease struct {
	sender *fakeQP
	buf    []byte
}

type fakeQP struct {
	owner     *fakeNative
	eq        *fakeEQ       // nil: an inbound stream not accepted yet
	held      []nativeEvent // under net.mu: its events until then
	peer      *fakeQP
	localPort int
	inflight  int
	blocked   bool
	destroyed bool
}

type fakeEQ struct {
	owner     *fakeNative
	qps       int           // under net.mu: live QPs bound here
	queue     []nativeEvent // under net.mu
	notify    chan struct{}
	wakeCh    chan struct{}
	destroyed bool
}

type fakeNative struct {
	net       *fakeNet
	server    bool
	eqs       []*fakeEQ // under net.mu
	pending   []*fakeQP // under net.mu: inbound streams no accept took yet
	pollErr   error     // under net.mu
	chNotify  chan struct{}
	chWake    chan struct{}
	dialDelay time.Duration // before the DPU sees the stream
	// After the stream is live but before dial returns: the window in which
	// the peer's first bytes can reach the EQ ahead of the dialer.
	dialReturnDelay time.Duration
	closeErrs       []error // under net.mu: results of successive close calls

	polls, accepts, destroys, aborts atomic.Int64
}

func newFakeNet(window, frag, maxPost int) *fakeNet {
	return &fakeNet{window: window, frag: frag, maxPost: maxPost, leases: make(map[int32]*fakeLease), nextUP: 40000}
}

func (fn *fakeNet) native(server bool) *fakeNative {
	f := &fakeNative{net: fn, server: server, chNotify: make(chan struct{}, 1), chWake: make(chan struct{}, 1)}
	if server {
		fn.mu.Lock()
		fn.server = f
		fn.mu.Unlock()
	}
	return f
}

func (fn *fakeNet) fault(format string, args ...any) {
	fn.faults = append(fn.faults, fmt.Sprintf(format, args...))
}

// check reports contract violations and leases still held.
func (fn *fakeNet) check() error {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	if len(fn.faults) != 0 {
		return fmt.Errorf("fake native faults: %v", fn.faults)
	}
	if len(fn.leases) != 0 {
		return fmt.Errorf("%d receive leases never released", len(fn.leases))
	}
	return nil
}

func (fn *fakeNet) outstanding() int {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	return len(fn.leases)
}

// enqueueLocked queues one event for q's EQ, or holds it with q while q is
// an inbound stream not accepted yet. Requires net.mu.
func (q *fakeQP) enqueueLocked(ev nativeEvent) {
	if q.eq == nil {
		q.held = append(q.held, ev)
		return
	}
	q.eq.queue = append(q.eq.queue, ev)
	signal(q.eq.notify)
}

func (f *fakeNative) setPollErr(err error) {
	f.net.mu.Lock()
	f.pollErr = err
	eqs := append([]*fakeEQ(nil), f.eqs...)
	f.net.mu.Unlock()
	for _, e := range eqs {
		signal(e.notify)
	}
	signal(f.chNotify)
}

func (f *fakeNative) liveEQs() int {
	f.net.mu.Lock()
	defer f.net.mu.Unlock()
	return len(f.eqs)
}

func (f *fakeNative) postMax() int { return f.net.maxPost }
func (f *fakeNative) serves() bool { return f.server }

func (f *fakeNative) newEQ() (eqID, error) {
	e := &fakeEQ{owner: f, notify: make(chan struct{}, 1), wakeCh: make(chan struct{}, 1)}
	f.net.mu.Lock()
	f.eqs = append(f.eqs, e)
	f.net.mu.Unlock()
	return unsafe.Pointer(e), nil
}

func (f *fakeNative) destroyEQ(id eqID) error {
	e := (*fakeEQ)(id)
	fn := f.net
	fn.mu.Lock()
	defer fn.mu.Unlock()
	if e.destroyed {
		fn.fault("EQ destroyed twice")
		return syscall.EBADF
	}
	if e.qps != 0 {
		fn.fault("EQ destroyed with %d live QPs", e.qps)
		return syscall.EBUSY
	}
	e.destroyed = true
	for i, x := range f.eqs {
		if x == e {
			f.eqs = append(f.eqs[:i], f.eqs[i+1:]...)
			break
		}
	}
	return nil
}

func (f *fakeNative) pollWait(id eqID, out []nativeEvent, maxWaitNs int64) (int, error) {
	f.polls.Add(1)
	e := (*fakeEQ)(id)
	var timeout <-chan time.Time
	if maxWaitNs >= 0 {
		timer := time.NewTimer(time.Duration(maxWaitNs))
		defer timer.Stop()
		timeout = timer.C
	}
	for {
		f.net.mu.Lock()
		if e.destroyed {
			f.net.fault("poll of a destroyed EQ")
		}
		if f.pollErr != nil {
			err := f.pollErr
			f.net.mu.Unlock()
			return 0, err
		}
		if len(e.queue) != 0 {
			n := copy(out, e.queue)
			e.queue = append(e.queue[:0], e.queue[n:]...)
			f.net.mu.Unlock()
			return n, nil
		}
		f.net.mu.Unlock()
		select {
		case <-e.notify:
		case <-e.wakeCh:
			return 0, nil
		case <-timeout:
			return 0, nil
		}
	}
}

func (f *fakeNative) wake(id eqID) { signal((*fakeEQ)(id).wakeCh) }

func (f *fakeNative) dial(id eqID, target string) (qpID, int, error) {
	if f.dialDelay > 0 {
		time.Sleep(f.dialDelay)
	}
	fn := f.net
	fn.mu.Lock()
	if fn.server == nil {
		fn.mu.Unlock()
		return nil, 0, syscall.ECONNREFUSED
	}
	fn.nextUP++
	client := &fakeQP{owner: f, eq: (*fakeEQ)(id), localPort: fn.nextUP}
	client.eq.qps++
	fn.nextUP++
	server := &fakeQP{owner: fn.server, localPort: fn.nextUP, peer: client}
	client.peer = server
	fn.server.pending = append(fn.server.pending, server)
	signal(fn.server.chNotify)
	fn.mu.Unlock()
	if f.dialReturnDelay > 0 {
		time.Sleep(f.dialReturnDelay)
	}
	return unsafe.Pointer(client), client.localPort, nil
}

// accept binds the oldest inbound stream to eq, with the events it held.
func (f *fakeNative) accept(id eqID) (qpID, int, error) {
	f.accepts.Add(1)
	fn := f.net
	fn.mu.Lock()
	defer fn.mu.Unlock()
	if f.pollErr != nil {
		return nil, 0, f.pollErr
	}
	if id == nil || len(f.pending) == 0 {
		return nil, 0, syscall.EAGAIN
	}
	e := (*fakeEQ)(id)
	if e.destroyed || e.owner != f {
		fn.fault("accept onto a destroyed or foreign EQ")
		return nil, 0, syscall.EINVAL
	}
	q := f.pending[0]
	f.pending = f.pending[1:]
	q.eq = e
	e.qps++
	e.queue, q.held = append(e.queue, q.held...), nil
	signal(e.notify)
	return unsafe.Pointer(q), q.peer.localPort, nil
}

// waitChannel returns at once while a stream waits or the channel failed:
// the native fd is level-triggered.
func (f *fakeNative) waitChannel() error {
	for {
		f.net.mu.Lock()
		ready := len(f.pending) != 0 || f.pollErr != nil
		f.net.mu.Unlock()
		if ready {
			return nil
		}
		select {
		case <-f.chNotify:
		case <-f.chWake:
			return nil
		}
	}
}

func (f *fakeNative) wakeChannel() { signal(f.chWake) }

func (f *fakeNative) send(qp qpID, p []byte) (int, error) {
	fn := f.net
	q := (*fakeQP)(qp)
	fn.mu.Lock()
	defer fn.mu.Unlock()
	if q.destroyed {
		fn.fault("send on destroyed QP %d", q.localPort)
		return 0, syscall.EBADF
	}
	if len(p) == 0 || len(p) > fn.maxPost {
		fn.fault("send of %d bytes (post max %d)", len(p), fn.maxPost)
		return 0, syscall.EINVAL
	}
	if q.inflight > 0 && q.inflight+len(p) > fn.window {
		q.blocked = true
		return 0, errWouldBlock
	}
	q.inflight += len(p)
	if q.peer.destroyed {
		return len(p), nil // lands nowhere; the credit never returns
	}
	for off := 0; off < len(p); off += fn.frag {
		end := min(off+fn.frag, len(p))
		buf := append([]byte(nil), p[off:end]...)
		fn.token++
		fn.leases[fn.token] = &fakeLease{sender: q, buf: buf}
		q.peer.enqueueLocked(nativeEvent{kind: evRecv, qp: unsafe.Pointer(q.peer), buf: buf, token: fn.token})
	}
	return len(p), nil
}

func (f *fakeNative) release(token int32) {
	f.net.mu.Lock()
	defer f.net.mu.Unlock()
	f.net.releaseLocked(token)
}

// releaseLocked returns one lease's credit to its sender. Requires fn.mu.
func (fn *fakeNet) releaseLocked(token int32) {
	lease := fn.leases[token]
	if lease == nil {
		fn.fault("release of unknown or released lease %d", token)
		return
	}
	delete(fn.leases, token)
	for i := range lease.buf {
		lease.buf[i] = 0xDD
	}
	s := lease.sender
	s.inflight -= len(lease.buf)
	if s.blocked && !s.destroyed {
		s.blocked = false
		s.enqueueLocked(nativeEvent{kind: evTxReady, qp: unsafe.Pointer(s), token: -1})
	}
}

func (f *fakeNative) destroy(qp qpID, abort bool) error {
	f.destroys.Add(1)
	if abort {
		f.aborts.Add(1)
	}
	fn := f.net
	q := (*fakeQP)(qp)
	fn.mu.Lock()
	defer fn.mu.Unlock()
	if q.destroyed {
		fn.fault("QP %d destroyed twice", q.localPort)
		return syscall.EBADF
	}
	if q.eq == nil {
		fn.fault("destroy of QP %d before its accept", q.localPort)
		return syscall.EINVAL
	}
	q.destroyed = true
	// Like dpumesh_free_port, reclaim what landed for q but was never polled.
	queue := &q.eq.queue
	q.eq.qps--
	kept := (*queue)[:0]
	var reclaimed []int32
	for _, ev := range *queue {
		if ev.qp == qp {
			if ev.kind == evRecv {
				reclaimed = append(reclaimed, ev.token)
			}
			continue
		}
		kept = append(kept, ev)
	}
	*queue = kept
	for _, token := range reclaimed {
		fn.releaseLocked(token)
	}
	if p := q.peer; p != nil && !p.destroyed {
		p.enqueueLocked(nativeEvent{kind: evFin, qp: unsafe.Pointer(p), token: -1})
	}
	return nil
}

// close fails with the next injected error, or tears the channel down: like
// the native teardown it reclaims the receive leases of streams never
// accepted.
func (f *fakeNative) close() error {
	f.net.mu.Lock()
	if len(f.closeErrs) != 0 {
		err := f.closeErrs[0]
		f.closeErrs = f.closeErrs[1:]
		f.net.mu.Unlock()
		return err
	}
	if len(f.eqs) != 0 {
		f.net.fault("channel closed with %d EQs", len(f.eqs))
	}
	var queued []int32
	for _, q := range f.pending {
		for _, ev := range q.held {
			if ev.kind == evRecv {
				queued = append(queued, ev.token)
			}
		}
		q.held = nil
	}
	f.pending = nil
	f.net.mu.Unlock()
	for _, token := range queued {
		f.release(token)
	}
	return nil
}
