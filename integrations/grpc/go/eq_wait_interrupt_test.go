package dmeshgo

import (
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func newTestWaiter(t *testing.T) (*eqWaiter, int) {
	t.Helper()
	eq, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { unix.Close(eq) })
	fd, err := unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { unix.Close(fd) })
	if err = unix.EpollCtl(eq, unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{Events: unix.EPOLLIN, Fd: int32(fd)}); err != nil {
		t.Fatal(err)
	}
	w, err := newEQWaiter(eq)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	return w, fd
}

// An interrupt ends a parked wait at once, and one that lands before the wait
// is not lost to the deadline the wait sets.
func TestEQWaiterInterrupt(t *testing.T) {
	w, _ := newTestWaiter(t)
	done := make(chan bool, 1)
	go func() {
		woken, err := w.waitUntil(time.Time{}) // no deadline
		if err != nil {
			t.Error(err)
		}
		done <- woken
	}()
	select {
	case <-done:
		t.Fatal("empty EQ returned without an interrupt")
	case <-time.After(20 * time.Millisecond):
	}
	w.interrupt()
	select {
	case woken := <-done:
		if !woken {
			t.Fatal("interrupt not reported")
		}
	case <-time.After(time.Second):
		t.Fatal("interrupt did not end the wait")
	}

	w.interrupt()
	start := time.Now()
	if woken, err := w.waitUntil(time.Now().Add(time.Second)); err != nil || !woken {
		t.Fatalf("early interrupt lost: woken=%v err=%v", woken, err)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("early interrupt waited for the deadline")
	}
	// Consumed: the next wait runs to its deadline.
	start = time.Now()
	if woken, err := w.waitUntil(time.Now().Add(20 * time.Millisecond)); err != nil || woken {
		t.Fatalf("stale interrupt: woken=%v err=%v", woken, err)
	}
	if time.Since(start) < 10*time.Millisecond {
		t.Fatal("wait ended before its deadline")
	}
}

// Readiness still wins over a long deadline, and interrupts racing many waits
// are each observed exactly once.
func TestEQWaiterReadinessAndRacingInterrupts(t *testing.T) {
	w, fd := newTestWaiter(t)
	one := []byte{1, 0, 0, 0, 0, 0, 0, 0}
	go func() {
		time.Sleep(10 * time.Millisecond)
		unix.Write(fd, one)
	}()
	if woken, err := w.waitUntil(time.Now().Add(5 * time.Second)); err != nil || woken {
		t.Fatalf("readiness: woken=%v err=%v", woken, err)
	}
	buf := make([]byte, 8)
	unix.Read(fd, buf)

	for i := 0; i < 200; i++ {
		go w.interrupt()
		deadline := time.Now().Add(2 * time.Second)
		woken, err := w.waitUntil(deadline)
		if err != nil {
			t.Fatal(err)
		}
		if !woken {
			// The interrupt may land after this wait timed out only if the
			// deadline passed, which it cannot have within the loop budget.
			t.Fatalf("round %d: interrupt lost", i)
		}
	}
}
