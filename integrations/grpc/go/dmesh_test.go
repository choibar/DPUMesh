package dmeshgo

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
)

// rig is a server and a client transport over one fake network.
type rig struct {
	net        *fakeNet
	srvN, cliN *fakeNative
	srv, cli   *transport
	lis        *Listener
}

func newRig(t *testing.T, window, frag, maxPost int, listen bool) *rig {
	t.Helper()
	fn := newFakeNet(window, frag, maxPost)
	r := &rig{net: fn, srvN: fn.native(true), cliN: fn.native(false)}
	r.srv, r.cli = newTransport(r.srvN), newTransport(r.cliN)
	if listen {
		l, err := r.srv.listen(&net.TCPAddr{Port: 9095})
		if err != nil {
			t.Fatal(err)
		}
		r.lis = l
	}
	t.Cleanup(func() { r.shutdown(t) })
	return r
}

func (r *rig) dial(ctx context.Context) (*Conn, error) {
	return r.cli.dial(ctx, "svc:9095", &net.TCPAddr{}, &net.TCPAddr{Port: 9095})
}

// pair opens one stream and returns both of its ends.
func (r *rig) pair(t *testing.T) (client, server *Conn) {
	t.Helper()
	c, err := r.dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s, err := r.lis.Accept()
	if err != nil {
		t.Fatal(err)
	}
	return c, s.(*Conn)
}

// shutdown closes whatever the test left, stops both pollers and verifies the
// fake saw no contract violation and no leaked lease.
func (r *rig) shutdown(t *testing.T) {
	t.Helper()
	if r.lis != nil {
		_ = r.lis.Close()
	}
	for _, tr := range []*transport{r.cli, r.srv} {
		tr.mu.Lock()
		conns := make([]*Conn, 0, len(tr.conns))
		for _, c := range tr.conns {
			conns = append(conns, c)
		}
		tr.mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
		deadline := time.Now().Add(2 * time.Second)
		for {
			err := tr.close()
			if err == nil {
				break
			}
			if !errors.Is(err, syscall.EBUSY) || time.Now().After(deadline) {
				t.Errorf("transport close: %v", err)
				break
			}
			time.Sleep(time.Millisecond)
		}
	}
	if err := r.net.check(); err != nil {
		t.Error(err)
	}
}

func awaitError(t *testing.T, done <-chan error, expected error) {
	t.Helper()
	select {
	case err := <-done:
		if !errors.Is(err, expected) {
			t.Fatalf("got %v, want %v", err, expected)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked operation was not woken")
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestReadDeadlineAndCloseWakeWaiters(t *testing.T) {
	r := newRig(t, 1<<20, 8064, 64<<10, true)
	for _, closeIt := range []bool{false, true} {
		c, s := r.pair(t)
		done := make(chan error, 1)
		go func() { _, err := c.Read(make([]byte, 1)); done <- err }()
		if closeIt {
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			awaitError(t, done, net.ErrClosed)
			if err := c.SetDeadline(time.Now()); !errors.Is(err, net.ErrClosed) {
				t.Fatal(err)
			}
			if _, err := c.Write([]byte("x")); !errors.Is(err, net.ErrClosed) {
				t.Fatalf("write after close: %v", err)
			}
		} else {
			if err := c.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
				t.Fatal(err)
			}
			awaitError(t, done, os.ErrDeadlineExceeded)
			// A deadline set while a Read waits wakes it; clearing it lets a
			// later Read wait for data.
			go func() { _, err := c.Read(make([]byte, 1)); done <- err }()
			time.Sleep(10 * time.Millisecond)
			if err := c.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			awaitError(t, done, os.ErrDeadlineExceeded)
			if err := c.SetReadDeadline(time.Time{}); err != nil {
				t.Fatal(err)
			}
			go func() {
				buf := make([]byte, 1)
				_, err := c.Read(buf)
				if err == nil && buf[0] != 'z' {
					err = errors.New("wrong byte")
				}
				done <- err
			}()
			if _, err := s.Write([]byte("z")); err != nil {
				t.Fatal(err)
			}
			awaitError(t, done, nil)
			_ = c.Close()
		}
		_ = s.Close()
	}
}

func TestPeerCloseDrainsThenEOF(t *testing.T) {
	r := newRig(t, 1<<20, 8064, 64<<10, true)
	c, s := r.pair(t)
	payload := bytes.Repeat([]byte("abc"), 10000)
	if _, err := s.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("read %d bytes, want %d intact", len(got), len(payload))
	}
	if n, err := c.Read(make([]byte, 8)); n != 0 || err != io.EOF {
		t.Fatalf("after EOF: %d, %v", n, err)
	}
}

// A writer past its window waits for the reader to release receive leases,
// resumes on TX_READY, and the stream arrives intact.
func TestWriteBackpressureResumesOnRelease(t *testing.T) {
	r := newRig(t, 16<<10, 4096, 8<<10, true)
	c, s := r.pair(t)
	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	errc := make(chan error, 1)
	go func() {
		_, err := c.Write(payload)
		errc <- err
	}()
	got := make([]byte, 0, len(payload))
	buf := make([]byte, 3000)
	for len(got) < len(payload) {
		n, err := s.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, buf[:n]...)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("stream corrupted under backpressure")
	}
}

func TestBlockedWriterWokenByCloseAndDeadline(t *testing.T) {
	r := newRig(t, 8<<10, 4096, 8<<10, true)
	for _, viaClose := range []bool{false, true} {
		c, s := r.pair(t)
		done := make(chan error, 1)
		go func() {
			_, err := c.Write(make([]byte, 64<<10)) // far past the window; nobody reads
			done <- err
		}()
		time.Sleep(20 * time.Millisecond)
		if viaClose {
			go c.Close()
			awaitError(t, done, net.ErrClosed)
		} else {
			if err := c.SetWriteDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			awaitError(t, done, os.ErrDeadlineExceeded)
			_ = c.Close()
		}
		_ = s.Close()
	}
}

// A departed peer returns no credit, so a writer blocked on it fails instead
// of waiting forever.
func TestBlockedWriteFailsAfterPeerClose(t *testing.T) {
	r := newRig(t, 8<<10, 4096, 8<<10, true)
	c, s := r.pair(t)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(make([]byte, 64<<10)); !errors.Is(err, syscall.EPIPE) {
		t.Fatalf("write to departed peer: %v, want EPIPE", err)
	}
}

// Unread leases return with Close, the QP is destroyed exactly once and later
// Close calls report nothing.
func TestCloseReturnsLeasesAndDestroysOnce(t *testing.T) {
	r := newRig(t, 1<<20, 1000, 64<<10, true)
	c, s := r.pair(t)
	if _, err := s.Write(make([]byte, 50000)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "leases to reach the client", func() bool { return r.net.outstanding() == 50 })
	before := r.cliN.destroys.Load()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if got := r.cliN.destroys.Load() - before; got != 1 {
		t.Fatalf("%d native destroys, want 1", got)
	}
	eventually(t, "leases to be released", func() bool { return r.net.outstanding() == 0 })
	_ = s.Close()
}

func TestListenerCloseAbortsPendingAndWakesAccept(t *testing.T) {
	r := newRig(t, 1<<20, 8064, 64<<10, true)
	c, err := r.dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "the inbound stream to queue", func() bool {
		r.srv.mu.Lock()
		defer r.srv.mu.Unlock()
		return len(r.lis.pending) == 1
	})
	aborts := r.srvN.aborts.Load()
	if err := r.lis.Close(); err != nil {
		t.Fatal(err)
	}
	if r.srvN.aborts.Load() != aborts+1 {
		t.Fatal("pending stream was not aborted")
	}
	if _, err := r.lis.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("accept after close: %v", err)
	}
	if n, err := c.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("client of aborted stream: %d, %v", n, err)
	}
	_ = c.Close()

	// Accept blocked before Close returns net.ErrClosed.
	l, err := r.srv.listen(&net.TCPAddr{Port: 9095})
	if err != nil {
		t.Fatal(err)
	}
	r.lis = l
	done := make(chan error, 1)
	go func() { _, err := l.Accept(); done <- err }()
	time.Sleep(10 * time.Millisecond)
	_ = l.Close()
	awaitError(t, done, net.ErrClosed)
}

// With no listener, an inbound stream is aborted and its client sees EOF.
func TestInboundWithoutListenerIsAborted(t *testing.T) {
	r := newRig(t, 1<<20, 8064, 64<<10, false)
	// A listener keeps the server polling; closing it leaves the poller
	// active only while connections exist, so poll through a live stream.
	l, err := r.srv.listen(&net.TCPAddr{Port: 9095})
	if err != nil {
		t.Fatal(err)
	}
	c0, err := r.dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s0, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	c, err := r.dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n, err := c.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("client of a stream nobody accepts: %d, %v", n, err)
	}
	if r.srvN.aborts.Load() != 1 {
		t.Fatalf("%d server aborts, want 1", r.srvN.aborts.Load())
	}
	for _, conn := range []net.Conn{c, c0, s0} {
		_ = conn.Close()
	}
}

// A serving channel holds streams that arrive before its first listener and
// hands them over; streams nobody ever accepts are aborted at close.
func TestInboundBeforeListenIsHandedToListener(t *testing.T) {
	r := newRig(t, 1<<20, 8064, 64<<10, false)
	c, err := r.dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte("early")); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the early stream to be held", func() bool {
		r.srv.mu.Lock()
		defer r.srv.mu.Unlock()
		return len(r.srv.prelisten) == 1
	})
	l, err := r.srv.listen(&net.TCPAddr{Port: 9095})
	if err != nil {
		t.Fatal(err)
	}
	r.lis = l
	s, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if s.LocalAddr() != l.Addr() {
		t.Fatalf("held stream local address %v, want %v", s.LocalAddr(), l.Addr())
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(s, buf); err != nil || string(buf) != "early" {
		t.Fatalf("held stream data: %q, %v", buf, err)
	}
	_ = s.Close()
	_ = c.Close()

	// Nobody listens on a second rig: close aborts the held stream.
	r2 := newRig(t, 1<<20, 8064, 64<<10, false)
	c2, err := r2.dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "the stream to be held", func() bool {
		r2.srv.mu.Lock()
		defer r2.srv.mu.Unlock()
		return len(r2.srv.prelisten) == 1
	})
	if err := r2.srv.close(); err != nil {
		t.Fatalf("close with an unclaimed inbound stream: %v", err)
	}
	if r2.srvN.aborts.Load() != 1 {
		t.Fatal("unclaimed inbound stream was not aborted")
	}
	if n, err := c2.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("client of the unclaimed stream: %d, %v", n, err)
	}
	_ = c2.Close()
}

// The peer can speak and even close before the dial returns (an HTTP/2
// server sends SETTINGS at once). Those events wait for the dialer instead
// of being dropped, and events held for an abandoned dial are returned.
func TestPeerSpeaksBeforeDialReturns(t *testing.T) {
	r := newRig(t, 1<<20, 3, 64<<10, true)
	// An open stream keeps the client polling through the dial, as in a
	// process that already has connections.
	keep, keepPeer := r.pair(t)
	defer keep.Close()
	defer keepPeer.Close()
	r.cliN.dialReturnDelay = 50 * time.Millisecond
	go func() {
		s, err := r.lis.Accept()
		if err != nil {
			return
		}
		_, _ = s.Write([]byte("settings"))
		_ = s.Close()
	}()
	c, err := r.dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// A dropped event would otherwise leave this read waiting forever.
	if err := c.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(c)
	if err != nil || string(got) != "settings" {
		t.Fatalf("early data: %q, %v", got, err)
	}
	_ = c.Close()

	// Abandoned while its events are held: they are returned, not leaked.
	r.cliN.dialReturnDelay = 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	go func() {
		s, err := r.lis.Accept()
		if err != nil {
			return
		}
		_, _ = s.Write([]byte("unwanted"))
	}()
	if _, err := r.dial(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("abandoned dial: %v", err)
	}
	eventually(t, "the abandoned stream's leases to return", func() bool {
		r.cli.mu.Lock()
		defer r.cli.mu.Unlock()
		return r.cliN.aborts.Load() == 1 && len(r.cli.early) == 0 && r.net.outstanding() == 0
	})
}

// A context that ends while the native dial waits returns at once; the late
// stream is aborted rather than leaked.
func TestDialContextReturnsAtDeadlineAndAbortsLateQP(t *testing.T) {
	r := newRig(t, 1<<20, 8064, 64<<10, true)
	r.cliN.dialDelay = 200 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	conn, err := r.dial(ctx)
	if conn != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("%v, %v", conn, err)
	}
	if took := time.Since(start); took > 150*time.Millisecond {
		t.Fatalf("dial returned after %v, not at the context deadline", took)
	}
	eventually(t, "the late QP to be aborted", func() bool { return r.cliN.aborts.Load() == 1 })
	s, err := r.lis.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if n, err := s.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("server end of an abandoned dial: %d, %v", n, err)
	}
	_ = s.Close()
	r.cli.mu.Lock()
	live, dials := len(r.cli.conns), r.cli.dials
	r.cli.mu.Unlock()
	if live != 0 || dials != 0 {
		t.Fatalf("abandoned dial left %d conns, %d dials", live, dials)
	}
}

// A poll failure fails blocked reads, writes and accepts with that error, and
// Close still completes.
func TestTransportFailureWakesWaitersAndCloseCompletes(t *testing.T) {
	r := newRig(t, 8<<10, 4096, 8<<10, true)
	c, s := r.pair(t)
	injected := syscall.EIO
	reads, writes, accepts := make(chan error, 1), make(chan error, 1), make(chan error, 1)
	go func() { _, err := c.Read(make([]byte, 1)); reads <- err }()
	go func() { _, err := c.Write(make([]byte, 64<<10)); writes <- err }()
	time.Sleep(20 * time.Millisecond)
	r.cliN.setPollErr(injected)
	awaitError(t, reads, injected)
	awaitError(t, writes, injected)
	if _, err := r.cli.dial(context.Background(), "svc:9095", &net.TCPAddr{}, &net.TCPAddr{}); !errors.Is(err, injected) {
		t.Fatalf("dial on failed transport: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close after failure: %v", err)
	}
	r.srvN.setPollErr(injected)
	go func() { _, err := r.lis.Accept(); accepts <- err }()
	awaitError(t, accepts, injected)
	_ = s.Close()
}

func TestIdleTransportDoesNotPoll(t *testing.T) {
	fn := newFakeNet(1<<20, 8064, 64<<10)
	n := fn.native(false)
	tr := newTransport(n)
	defer tr.close()
	time.Sleep(20 * time.Millisecond)
	if polls := n.polls.Load(); polls != 0 {
		t.Fatalf("idle transport polled %d times", polls)
	}
	srvN := fn.native(true)
	srv := newTransport(srvN)
	defer srv.close()
	l, err := srv.listen(&net.TCPAddr{Port: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	c, err := tr.dial(context.Background(), "svc:1", &net.TCPAddr{}, &net.TCPAddr{})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "a live connection to be polled", func() bool { return n.polls.Load() > 0 })
	_ = c.Close()
	s, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
}

func TestCloseTransportBusyAndRetry(t *testing.T) {
	fn := newFakeNet(1<<20, 8064, 64<<10)
	srvN := fn.native(true)
	srv := newTransport(srvN)
	l, err := srv.listen(&net.TCPAddr{Port: 1})
	if err != nil {
		t.Fatal(err)
	}
	n := fn.native(false)
	injected := errors.New("native teardown failed")
	n.closeErrs = []error{injected}
	tr := newTransport(n)
	process.Lock()
	if process.t != nil {
		process.Unlock()
		t.Fatal("another test left a process transport")
	}
	process.t = tr
	process.Unlock()
	defer func() { process.Lock(); process.t = nil; process.Unlock() }()

	c, err := tr.dial(context.Background(), "svc:1", &net.TCPAddr{}, &net.TCPAddr{})
	if err != nil {
		t.Fatal(err)
	}
	if err := CloseTransport(); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("CloseTransport() = %v, want EBUSY", err)
	}
	if err := c.SetDeadline(time.Time{}); err != nil {
		t.Fatalf("connection after busy close: %v", err)
	}
	_ = c.Close()
	if err := CloseTransport(); !errors.Is(err, injected) {
		t.Fatalf("CloseTransport() = %v, want the native failure", err)
	}
	if _, err := tr.dial(context.Background(), "svc:1", &net.TCPAddr{}, &net.TCPAddr{}); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("dial after failed close: %v", err)
	}
	if err := CloseTransport(); err != nil {
		t.Fatalf("retried CloseTransport: %v", err)
	}
	process.Lock()
	cleared := process.t == nil
	process.Unlock()
	if !cleared {
		t.Fatal("successful close kept the process transport")
	}
	s, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	_ = l.Close()
	if err := srv.close(); err != nil {
		t.Fatal(err)
	}
	if err := fn.check(); err != nil {
		t.Fatal(err)
	}
}

// A Close racing transport shutdown is never stranded: the poller runs every
// destroy queued before it stops.
func TestCloseRacingTransportShutdown(t *testing.T) {
	for i := 0; i < 200; i++ {
		fn := newFakeNet(1<<20, 8064, 64<<10)
		srv := newTransport(fn.native(true))
		cli := newTransport(fn.native(false))
		l, err := srv.listen(&net.TCPAddr{Port: 1})
		if err != nil {
			t.Fatal(err)
		}
		c, err := cli.dial(context.Background(), "svc:1", &net.TCPAddr{}, &net.TCPAddr{})
		if err != nil {
			t.Fatal(err)
		}
		closed := make(chan error, 1)
		go func() { closed <- c.Close() }()
		for {
			err := cli.close()
			if err == nil {
				break
			}
			if !errors.Is(err, syscall.EBUSY) {
				t.Fatal(err)
			}
			runtime.Gosched()
		}
		awaitError(t, closed, nil)
		s, err := l.Accept()
		if err != nil {
			t.Fatal(err)
		}
		_ = s.Close()
		_ = l.Close()
		if err := srv.close(); err != nil {
			t.Fatal(err)
		}
		if err := fn.check(); err != nil {
			t.Fatal(err)
		}
	}
}

// Both directions stream concurrently with small fragments, a tight window
// and writes and reads of odd sizes; every byte must arrive in order (run
// with -race).
func TestConcurrentBidirectionalStream(t *testing.T) {
	r := newRig(t, 32<<10, 1500, 8<<10, true)
	c, s := r.pair(t)
	const size = 4 << 20
	pattern := func(seed byte) []byte {
		b := make([]byte, size)
		for i := range b {
			b[i] = seed + byte(i*13)
		}
		return b
	}
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	pump := func(w *Conn, data []byte) {
		defer wg.Done()
		for off := 0; off < len(data); {
			end := min(off+7777, len(data))
			if _, err := w.Write(data[off:end]); err != nil {
				errs <- err
				return
			}
			off = end
		}
	}
	drain := func(rd *Conn, want []byte) {
		defer wg.Done()
		got := make([]byte, len(want))
		if _, err := io.ReadFull(rd, got); err != nil {
			errs <- err
			return
		}
		if !bytes.Equal(got, want) {
			errs <- errors.New("stream corrupted")
		}
	}
	up, down := pattern(1), pattern(2)
	wg.Add(4)
	go pump(c, up)
	go pump(s, down)
	go drain(s, up)
	go drain(c, down)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestDialContextRejectsDoneContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if conn, err := DialContext(ctx, "invalid", -1); conn != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("%v, %v", conn, err)
	}
}

func TestListenerAddressMatchesServiceTarget(t *testing.T) {
	for _, c := range []struct {
		target, ip string
		port       int
		ok         bool
	}{
		{"10.96.0.15:9095", "10.96.0.15", 9095, true},
		{"localhost:9095", "127.0.0.1", 9095, true},
		{"10.96.0.15:9095", "10.96.0.16", 9095, false},
		{"10.96.0.15:9095", "10.96.0.15", 9096, false},
		{"echo", "10.96.0.15", 9095, false},
		{"", "10.96.0.15", 9095, false},
	} {
		t.Setenv("DPUMESH_SERVICE", c.target)
		if err := servesAt(c.ip, c.port); (err == nil) != c.ok {
			t.Errorf("servesAt(%s, %d) with DPUMESH_SERVICE=%q: %v", c.ip, c.port, c.target, err)
		}
	}
}
func TestServiceListenerNeedsNoAddress(t *testing.T) {
	for _, c := range []struct {
		target, addr string
		ok           bool
	}{
		{"echo:9095", ":9095", true},
		{"echo.prod.svc.cluster.local:9095", ":9095", true},
		{"10.96.0.15:9095", "10.96.0.15:9095", true},
		{"echo", "", false},
		{":9095", "", false},
		{"echo:0", "", false},
		{"echo:http", "", false},
		{"", "", false},
	} {
		t.Setenv("DPUMESH_SERVICE", c.target)
		addr, err := serviceAddr()
		if (err == nil) != c.ok || err == nil && addr.String() != c.addr {
			t.Errorf("serviceAddr() with DPUMESH_SERVICE=%q: %v, %v", c.target, addr, err)
		}
	}
}
