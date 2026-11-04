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
// departed peer returns no credit. Events wait in the QP they belong to, an
// inbound stream's from before its accept too. Released buffers are poisoned
// so a read after release corrupts the data the tests verify.
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
	peer      *fakeQP
	localPort int
	inflight  int
	blocked   bool
	destroyed bool
	queue     []nativeEvent // under net.mu
	notify    chan struct{}
	wakeCh    chan struct{}
}

type fakeNative struct {
	net       *fakeNet
	server    bool
	live      map[*fakeQP]struct{} // under net.mu: QPs handed out, not destroyed
	pending   []*fakeQP            // under net.mu: inbound streams no accept took yet
	pollErr   error                // under net.mu
	chNotify  chan struct{}
	chWake    chan struct{}
	dialDelay time.Duration // before the DPU sees the stream
	// After the stream is live but before dial returns: the window in which
	// the peer's first bytes can reach the QP ahead of the dialer.
	dialReturnDelay time.Duration
	closeErrs       []error // under net.mu: results of successive close calls

	polls, accepts, destroys, aborts atomic.Int64

	requests  []nativeEvent // under net.mu: DPU requests the channel's next poll returns
	mu        sync.Mutex
	scaleAcks [][2]int // token, applied
}

func newFakeNet(window, frag, maxPost int) *fakeNet {
	return &fakeNet{window: window, frag: frag, maxPost: maxPost, leases: make(map[int32]*fakeLease), nextUP: 40000}
}

func (fn *fakeNet) native(server bool) *fakeNative {
	f := &fakeNative{net: fn, server: server, live: make(map[*fakeQP]struct{}),
		chNotify: make(chan struct{}, 1), chWake: make(chan struct{}, 1)}
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

// newQPLocked makes a QP of f with the next port. Requires net.mu.
func (f *fakeNative) newQPLocked() *fakeQP {
	f.net.nextUP++
	return &fakeQP{owner: f, localPort: f.net.nextUP, notify: make(chan struct{}, 1), wakeCh: make(chan struct{}, 1)}
}

// enqueueLocked queues one event in q. Requires net.mu.
func (q *fakeQP) enqueueLocked(ev nativeEvent) {
	q.queue = append(q.queue, ev)
	signal(q.notify)
}

// reclaimLocked returns the leases of q's unpolled receives, as the native
// teardown does. Requires net.mu.
func (q *fakeQP) reclaimLocked() {
	for _, ev := range q.queue {
		if ev.kind == evRecv {
			q.owner.net.releaseLocked(ev.token)
		}
	}
	q.queue = nil
}

func (f *fakeNative) setPollErr(err error) {
	f.net.mu.Lock()
	f.pollErr = err
	for q := range f.live {
		signal(q.notify)
	}
	f.net.mu.Unlock()
	signal(f.chNotify)
}

func (f *fakeNative) liveQPs() int {
	f.net.mu.Lock()
	defer f.net.mu.Unlock()
	return len(f.live)
}

func (f *fakeNative) postMax() int { return f.net.maxPost }
func (f *fakeNative) serves() bool { return f.server }

func (f *fakeNative) pollWait(id qpID, out []nativeEvent, maxWaitNs int64) (int, error) {
	f.polls.Add(1)
	q := (*fakeQP)(id)
	var timeout <-chan time.Time
	if maxWaitNs >= 0 {
		timer := time.NewTimer(time.Duration(maxWaitNs))
		defer timer.Stop()
		timeout = timer.C
	}
	for {
		f.net.mu.Lock()
		if q.destroyed {
			f.net.fault("poll of destroyed QP %d", q.localPort)
		}
		if f.pollErr != nil {
			err := f.pollErr
			f.net.mu.Unlock()
			return 0, err
		}
		if len(q.queue) != 0 {
			n := copy(out, q.queue)
			q.queue = append(q.queue[:0], q.queue[n:]...)
			f.net.mu.Unlock()
			return n, nil
		}
		f.net.mu.Unlock()
		select {
		case <-q.notify:
		case <-q.wakeCh:
			return 0, nil
		case <-timeout:
			return 0, nil
		}
	}
}

func (f *fakeNative) wake(id qpID) { signal((*fakeQP)(id).wakeCh) }

func (f *fakeNative) spareReady(qp qpID) error { return nil }
func (f *fakeNative) scaleAck(token uint32, applied int) error {
	f.mu.Lock()
	f.scaleAcks = append(f.scaleAcks, [2]int{int(token), applied})
	f.mu.Unlock()
	return nil
}

func (f *fakeNative) dial(target string) (qpID, int, error) {
	if f.dialDelay > 0 {
		time.Sleep(f.dialDelay)
	}
	fn := f.net
	fn.mu.Lock()
	if fn.server == nil {
		fn.mu.Unlock()
		return nil, 0, syscall.ECONNREFUSED
	}
	client := f.newQPLocked()
	f.live[client] = struct{}{}
	server := fn.server.newQPLocked()
	client.peer, server.peer = server, client
	fn.server.pending = append(fn.server.pending, server)
	signal(fn.server.chNotify)
	fn.mu.Unlock()
	if f.dialReturnDelay > 0 {
		time.Sleep(f.dialReturnDelay)
	}
	return unsafe.Pointer(client), client.localPort, nil
}

// accept returns the oldest inbound stream, with the events it holds.
func (f *fakeNative) accept() (qpID, int, error) {
	f.accepts.Add(1)
	fn := f.net
	fn.mu.Lock()
	defer fn.mu.Unlock()
	if f.pollErr != nil {
		return nil, 0, f.pollErr
	}
	if len(f.pending) == 0 {
		return nil, 0, syscall.EAGAIN
	}
	q := f.pending[0]
	f.pending = f.pending[1:]
	f.live[q] = struct{}{}
	return unsafe.Pointer(q), q.peer.localPort, nil
}

// waitChannel returns at once while a stream waits or the channel failed:
// the native fd is level-triggered.
func (f *fakeNative) waitChannel() error {
	for {
		f.net.mu.Lock()
		ready := len(f.pending) != 0 || len(f.requests) != 0 || f.pollErr != nil
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

// request queues a DPU request (PREPARE, SCALE) for the channel.
func (f *fakeNative) request(ev nativeEvent) {
	f.net.mu.Lock()
	f.requests = append(f.requests, ev)
	f.net.mu.Unlock()
	signal(f.chNotify)
}

func (f *fakeNative) pollChannel(out []nativeEvent) (int, error) {
	f.net.mu.Lock()
	defer f.net.mu.Unlock()
	n := copy(out, f.requests)
	f.requests = f.requests[n:]
	return n, nil
}

func (f *fakeNative) send(id qpID, p []byte) (int, error) {
	fn := f.net
	q := (*fakeQP)(id)
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
		q.peer.enqueueLocked(nativeEvent{kind: evRecv, buf: buf, token: fn.token})
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
		s.enqueueLocked(nativeEvent{kind: evTxReady, token: -1})
	}
}

func (f *fakeNative) destroy(id qpID, abort bool) error {
	f.destroys.Add(1)
	if abort {
		f.aborts.Add(1)
	}
	fn := f.net
	q := (*fakeQP)(id)
	fn.mu.Lock()
	defer fn.mu.Unlock()
	if _, ok := f.live[q]; !ok {
		fn.fault("destroy of QP %d not handed out or already destroyed", q.localPort)
		return syscall.EBADF
	}
	delete(f.live, q)
	q.destroyed = true
	q.reclaimLocked()
	if p := q.peer; p != nil && !p.destroyed {
		p.enqueueLocked(nativeEvent{kind: evFin, token: -1})
	}
	return nil
}

// close fails with the next injected error, or tears the channel down: like
// the native teardown it reclaims the receive leases of streams never
// accepted.
func (f *fakeNative) close() error {
	f.net.mu.Lock()
	defer f.net.mu.Unlock()
	if len(f.closeErrs) != 0 {
		err := f.closeErrs[0]
		f.closeErrs = f.closeErrs[1:]
		return err
	}
	if len(f.live) != 0 {
		f.net.fault("channel closed with %d QPs", len(f.live))
	}
	for _, q := range f.pending {
		q.reclaimLocked()
	}
	f.pending = nil
	return nil
}
