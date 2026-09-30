package dmeshgo

import (
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestEQWaiterNestedReadinessAndOwnership(t *testing.T) {
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
	w, err := newEQWaiter(eq)
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	start := time.Now()
	if err = w.wait(20 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 10*time.Millisecond {
		t.Fatal("empty EQ did not wait for deadline")
	}
	one := []byte{1, 0, 0, 0, 0, 0, 0, 0}
	for pass := 0; pass < 2; pass++ {
		done := make(chan error, 1)
		go func() { done <- w.wait(time.Second) }()
		select {
		case err := <-done:
			t.Fatalf("empty EQ woke early: %v", err)
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
			t.Fatal("missed EQ wake")
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
	// Closing the Go waiter must not close the native-owned EQ.
	if _, err = unix.EpollWait(eq, make([]unix.EpollEvent, 1), 0); err != nil {
		t.Fatal(err)
	}
}
