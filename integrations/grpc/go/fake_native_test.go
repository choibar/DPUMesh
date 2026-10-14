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
// departed peer returns no credit. Released buffers are poisoned so a read
// after release corrupts the data the tests verify.
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
}

type fakeNative struct {
	net       *fakeNet
	server    bool
	queue     []nativeEvent // under net.mu
	notify    chan struct{}
	wakeCh    chan struct{}
	pollErr   error         // under net.mu
	dialDelay time.Duration // before the DPU sees the stream
	// After the stream is live but before dial returns: the window in which
	// the peer's first bytes can reach the EQ ahead of the dialer.
	dialReturnDelay time.Duration
	closeErrs       []error // under net.mu: results of successive close calls

	polls, destroys, aborts atomic.Int64
}

func newFakeNet(window, frag, maxPost int) *fakeNet {
	return &fakeNet{window: window, frag: frag, maxPost: maxPost, leases: make(map[int32]*fakeLease), nextUP: 40000}
}

func (fn *fakeNet) native(server bool) *fakeNative {
	f := &fakeNative{net: fn, server: server, notify: make(chan struct{}, 1), wakeCh: make(chan struct{}, 1)}
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

// enqueueLocked queues one event for f. Requires net.mu.
func (f *fakeNative) enqueueLocked(ev nativeEvent) {
	f.queue = append(f.queue, ev)
	signal(f.notify)
}

func (f *fakeNative) setPollErr(err error) {
	f.net.mu.Lock()
	f.pollErr = err
	f.net.mu.Unlock()
	signal(f.notify)
}

func (f *fakeNative) postMax() int { return f.net.maxPost }
func (f *fakeNative) serves() bool { return f.server }

func (f *fakeNative) pollWait(out []nativeEvent, maxWaitNs int64) (int, error) {
	f.polls.Add(1)
	var timeout <-chan time.Time
	if maxWaitNs >= 0 {
		timer := time.NewTimer(time.Duration(maxWaitNs))
		defer timer.Stop()
		timeout = timer.C
	}
	for {
		f.net.mu.Lock()
		if f.pollErr != nil {
			err := f.pollErr
			f.net.mu.Unlock()
			return 0, err
		}
		if len(f.queue) != 0 {
			n := copy(out, f.queue)
			f.queue = append(f.queue[:0], f.queue[n:]...)
			f.net.mu.Unlock()
			return n, nil
		}
		f.net.mu.Unlock()
		select {
		case <-f.notify:
		case <-f.wakeCh:
			return 0, nil
		case <-timeout:
			return 0, nil
		}
	}
}

func (f *fakeNative) wake() { signal(f.wakeCh) }

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
	fn.nextUP++
	client := &fakeQP{owner: f, localPort: fn.nextUP}
	fn.nextUP++
	server := &fakeQP{owner: fn.server, localPort: fn.nextUP, peer: client}
	client.peer = server
	fn.server.enqueueLocked(nativeEvent{kind: evConnReq, qp: unsafe.Pointer(server), token: -1,
		localPort: server.localPort, remotePort: client.localPort})
	fn.mu.Unlock()
	if f.dialReturnDelay > 0 {
		time.Sleep(f.dialReturnDelay)
	}
	return unsafe.Pointer(client), client.localPort, nil
}

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
		q.peer.owner.enqueueLocked(nativeEvent{kind: evRecv, qp: unsafe.Pointer(q.peer), buf: buf, token: fn.token})
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
		s.owner.enqueueLocked(nativeEvent{kind: evTxReady, qp: unsafe.Pointer(s), token: -1})
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
	q.destroyed = true
	// Like dpumesh_free_port, reclaim what landed for q but was never polled.
	kept := f.queue[:0]
	var reclaimed []int32
	for _, ev := range f.queue {
		if ev.qp == qp {
			if ev.kind == evRecv {
				reclaimed = append(reclaimed, ev.token)
			}
			continue
		}
		kept = append(kept, ev)
	}
	f.queue = kept
	for _, token := range reclaimed {
		fn.releaseLocked(token)
	}
	if p := q.peer; p != nil && !p.destroyed {
		p.owner.enqueueLocked(nativeEvent{kind: evFin, qp: unsafe.Pointer(p), token: -1})
	}
	return nil
}

// close fails with the next injected error, or tears the channel down: like
// the native teardown it reclaims receive leases never polled out of the EQ.
func (f *fakeNative) close() error {
	f.net.mu.Lock()
	if len(f.closeErrs) != 0 {
		err := f.closeErrs[0]
		f.closeErrs = f.closeErrs[1:]
		f.net.mu.Unlock()
		return err
	}
	var queued []int32
	for _, ev := range f.queue {
		if ev.kind == evRecv {
			queued = append(queued, ev.token)
		}
	}
	f.queue = nil
	f.net.mu.Unlock()
	for _, token := range queued {
		f.release(token)
	}
	return nil
}
