package dmeshgo

import (
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestFDWaiterNestedReadinessAndOwnership(t *testing.T) {
	eq, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(eq)
	fd, err := unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	if err = unix.EpollCtl(eq, unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{Events: unix.EPOLLIN, Fd: int32(fd)}); err != nil {
		t.Fatal(err)
	}
	w, err := newFDWaiter(eq, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	start := time.Now()
	if err = w.wait(20 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 10*time.Millisecond {
		t.Fatal("empty fd did not wait for deadline")
	}
	one := []byte{1, 0, 0, 0, 0, 0, 0, 0}
	for pass := 0; pass < 2; pass++ {
		done := make(chan error, 1)
		go func() { done <- w.wait(time.Second) }()
		select {
		case err := <-done:
			t.Fatalf("empty fd woke early: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
		if _, err = unix.Write(fd, one); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatal("missed fd wake")
		}
		// Readiness remains level-triggered until the native consumer clears it.
		start = time.Now()
		if err = w.wait(time.Second); err != nil {
			t.Fatal(err)
		}
		if time.Since(start) > 500*time.Millisecond {
			t.Fatal("lost existing readiness")
		}
		buf := make([]byte, 8)
		if _, err = unix.Read(fd, buf); err != nil {
			t.Fatal(err)
		}
	}
	w.close()
	// Closing the Go waiter must not close the native-owned fd.
	if _, err = unix.EpollWait(eq, make([]unix.EpollEvent, 1), 0); err != nil {
		t.Fatal(err)
	}
}

// A try that consumes the fd's raises, as a native poll does, needs no
// readiness check: a raise that lands after a try found nothing, before the
// wait parks, still ends the wait.
func TestFDWaiterRaiseAfterEmptyTry(t *testing.T) {
	eq, fd := newTestFDs(t)
	one := []byte{1, 0, 0, 0, 0, 0, 0, 0}
	buf := make([]byte, 8)
	tries := 0
	w, err := newFDWaiter(eq, func() bool {
		tries++
		if _, err := unix.Read(fd, buf); err == nil {
			return true
		}
		if tries == 1 {
			unix.Write(fd, one)
		}
		return false
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	start := time.Now()
	if woken, err := w.waitUntil(time.Now().Add(5 * time.Second)); err != nil || woken {
		t.Fatalf("woken=%v err=%v", woken, err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("a raise after an empty try was slept through")
	}
	if tries != 2 {
		t.Fatalf("%d tries, want 2", tries)
	}
}

// A deadline that has already passed still gets one try.
func TestFDWaiterPastDeadlineTriesOnce(t *testing.T) {
	eq, _ := newTestFDs(t)
	tries := 0
	w, err := newFDWaiter(eq, func() bool { tries++; return false })
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	if woken, err := w.waitUntil(time.Now().Add(-time.Second)); err != nil || woken {
		t.Fatalf("woken=%v err=%v", woken, err)
	}
	if tries != 1 {
		t.Fatalf("%d tries, want 1", tries)
	}
}

// A wait that ends at once allocates nothing, whether its try succeeds or the
// fd is readable.
func TestFDWaiterWaitAllocatesNothing(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector allocates")
	}
	eq, fd := newTestFDs(t)
	w, err := newFDWaiter(eq, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	if allocs := testing.AllocsPerRun(100, func() { w.waitUntil(time.Time{}) }); allocs != 0 {
		t.Fatalf("%v allocations per wait", allocs)
	}
	r, err := newFDWaiter(eq, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()
	if _, err = unix.Write(fd, []byte{1, 0, 0, 0, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(100, func() { r.waitUntil(time.Time{}) }); allocs != 0 {
		t.Fatalf("%v allocations per readiness wait", allocs)
	}
}
