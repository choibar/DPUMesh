package dmeshgo

import (
	"errors"
	"os"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// The native EQ exposes a level-triggered epoll set. Register a duplicate with
// Go's netpoller so waiting parks a goroutine rather than an OS thread in cgo.
// Neither this wrapper nor its readiness check consumes the native doorbells:
// the library's own timers (naps, retained-tail deadlines, the backstop) and
// doorbells raise the fd, and draining the EQ settles it.
type eqWaiter struct {
	file *os.File
	raw  syscall.RawConn

	mu      sync.Mutex
	waiting bool // a waitUntil is parked with its deadline set
	woken   bool // interrupt pending for the current or next waitUntil
}

func newEQWaiter(fd int) (*eqWaiter, error) {
	dup, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	if err = unix.SetNonblock(dup, true); err != nil {
		unix.Close(dup)
		return nil, err
	}
	f := os.NewFile(uintptr(dup), "dmesh-eq")
	// SetReadDeadline also verifies that NewFile registered the fd as pollable.
	if err = f.SetReadDeadline(time.Time{}); err != nil {
		f.Close()
		return nil, err
	}
	raw, err := f.SyscallConn()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &eqWaiter{file: f, raw: raw}, nil
}

func (w *eqWaiter) close() { w.file.Close() }

// wait blocks until the EQ is readable or timeout passes.
func (w *eqWaiter) wait(timeout time.Duration) error {
	_, err := w.waitUntil(time.Now().Add(timeout))
	return err
}

// waitUntil blocks until the EQ is readable, the deadline passes (zero: none)
// or interrupt is called; woken reports an interrupt. The deadline is set
// under the lock interrupt takes, so an interrupt is never overwritten by a
// later deadline.
func (w *eqWaiter) waitUntil(deadline time.Time) (woken bool, err error) {
	w.mu.Lock()
	if w.woken {
		w.woken = false
		w.mu.Unlock()
		return true, nil
	}
	if err = w.file.SetReadDeadline(deadline); err != nil {
		w.mu.Unlock()
		return false, err
	}
	w.waiting = true
	w.mu.Unlock()

	var pollErr error
	err = w.raw.Read(func(fd uintptr) bool {
		p := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		for {
			n, err := unix.Poll(p, 0)
			if err == unix.EINTR {
				continue
			}
			pollErr = err
			if p[0].Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLNVAL) != 0 {
				pollErr = syscall.EIO
			}
			return n != 0 || pollErr != nil
		}
	})

	w.mu.Lock()
	w.waiting = false
	woken, w.woken = w.woken, false
	w.mu.Unlock()
	if woken {
		return true, nil
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return false, nil // periodic progress deadlines are normal wakes
	}
	return false, errors.Join(err, pollErr)
}

// interrupt makes the current or the next waitUntil return woken.
func (w *eqWaiter) interrupt() {
	w.mu.Lock()
	w.woken = true
	if w.waiting {
		_ = w.file.SetReadDeadline(time.Unix(1, 0))
	}
	w.mu.Unlock()
}
