package dmeshgo

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"
)

// These cases use empty native handles, for which close is explicitly a no-op.
// They test Go wait/cancellation behavior without registering a DOCA device.
func idleConn() *Conn { return &Conn{t: &transport{changed: make(chan struct{})}} }
func awaitError(t *testing.T, done <-chan error, expected error) {
	t.Helper()
	select {
	case err := <-done:
		if !errors.Is(err, expected) {
			t.Fatalf("got %v, want %v", err, expected)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked operation was not woken")
	}
}
func TestReadDeadlineAndCloseWakeWaiters(t *testing.T) {
	for _, closeIt := range []bool{false, true} {
		c := idleConn()
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
		} else {
			if err := c.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
				t.Fatal(err)
			}
			awaitError(t, done, os.ErrDeadlineExceeded)
			// Clearing a deadline must allow a later read to wait for data/EOF.
			if err := c.SetReadDeadline(time.Time{}); err != nil {
				t.Fatal(err)
			}
			go func() { _, err := c.Read(make([]byte, 1)); done <- err }()
			c.t.mu.Lock()
			c.eof = true
			c.notify()
			c.t.mu.Unlock()
			awaitError(t, done, io.EOF)
		}
	}
}
func TestWriteDeadlineAndListenerCancellation(t *testing.T) {
	c := idleConn()
	if err := c.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if n, err := c.Write([]byte("x")); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("%d, %v", n, err)
	}
	l := &Listener{t: c.t}
	c.t.listener = l
	done := make(chan error, 1)
	go func() { _, err := l.Accept(); done <- err }()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	awaitError(t, done, net.ErrClosed)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if conn, err := DialContext(ctx, "invalid", -1); conn != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("%v, %v", conn, err)
	}
}

// A successful Write can leave a buffered native tail while the application is
// computing rather than parked in Read. Shared Comch failures also need progress
// during that interval. Exercise the production scheduler without a DOCA device.
func TestLiveConnectionProgressWithoutWaiters(t *testing.T) {
	tr := newTransport()
	tr.conns[nil] = newConn(tr, nil, nil, nil)
	progressed := make(chan struct{}, 4)
	go tr.runPoll(-1, func() (int, int64) {
		if tr.mu.TryLock() {
			tr.mu.Unlock()
			t.Error("native progress was not serialized")
		}
		select {
		case progressed <- struct{}{}:
		default:
		}
		return 0, -1
	})
	defer func() { close(tr.stop); <-tr.done }()
	for i := 0; i < 3; i++ {
		select {
		case <-progressed:
		case <-time.After(time.Second):
			t.Fatal("live connection stopped progressing without parked readers/writers")
		}
	}
}

func TestIdlePollerWakesForNewConnection(t *testing.T) {
	tr := newTransport()
	progressed := make(chan struct{}, 1)
	go tr.runPoll(-1, func() (int, int64) {
		progressed <- struct{}{}
		return -1, -1
	})
	select {
	case <-progressed:
		t.Fatal("empty transport unexpectedly polled")
	case <-time.After(20 * time.Millisecond):
	}
	tr.mu.Lock()
	tr.conns[nil] = newConn(tr, nil, nil, nil)
	tr.wakePoller()
	tr.mu.Unlock()
	select {
	case <-progressed:
	case <-time.After(time.Second):
		close(tr.stop)
		t.Fatal("new connection did not wake the idle poller")
	}
	<-tr.done
}

func TestCloseTransportBusyKeepsConnectionUsable(t *testing.T) {
	tr := newTransport()
	conn := newConn(tr, nil, nil, nil)
	tr.conns[nil] = conn
	process.Lock()
	if process.t != nil {
		process.Unlock()
		t.Fatal("another test left a process transport")
	}
	process.t = tr
	process.Unlock()
	defer func() { process.Lock(); process.t = nil; process.Unlock() }()
	if err := CloseTransport(); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("CloseTransport() = %v, want EBUSY", err)
	}
	if tr.err != nil || conn.closed {
		t.Fatal("busy close invalidated the active connection")
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		t.Fatalf("connection after busy close: %v", err)
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
