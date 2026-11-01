package dmeshgo

import (
	"errors"
	"os"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// A native QP's fd, like the channel's, is a level-triggered epoll set. Register a duplicate with Go's netpoller so waiting parks a goroutine
// rather than an OS thread in cgo. The wrapper itself consumes no native
// doorbell: the library's own timer (re-polls, the backstop) and doorbells
// raise the fd, and polling the QP, or accepting on the channel, settles it.
//
// A wait runs its try as Go's own reads run their syscall: after the
// netpoller's readiness for the fd is reset. The netpoller registers the fd
// edge-triggered, so a raise that lands after a try found nothing still wakes
// the wait, and a try that arms the fd needs no readiness check besides.
type fdWaiter struct {
	file *os.File
	raw  syscall.RawConn

	// The waiting goroutine's own. try attempts what the wait is for and
	// reports success; attempt is w.run, bound once so a wait allocates
	// nothing.
	try     func() bool
	attempt func(uintptr) bool
	tried   bool  // try ran in this wait
	pollErr error // the readiness check's error
	pfd     [1]unix.PollFd
	zero    unix.Timespec

	mu       sync.Mutex
	deadline time.Time // the fd's read deadline as last set
	waiting  bool      // a waitUntil is parked with its deadline set
	woken    bool      // interrupt pending for the current or next waitUntil
}

// newFDWaiter puts a duplicate of fd on the netpoller. try attempts what a
// wait is for and settles the fd when it finds nothing; nil checks only that
// the fd is readable, for an owner that settles it after the wait.
func newFDWaiter(fd int, try func() bool) (*fdWaiter, error) {
	dup, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	if err = unix.SetNonblock(dup, true); err != nil {
		unix.Close(dup)
		return nil, err
	}
	f := os.NewFile(uintptr(dup), "dmesh-fd")
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
	w := &fdWaiter{file: f, raw: raw, try: try}
	w.attempt = w.run
	if w.try == nil {
		w.pfd[0] = unix.PollFd{Fd: int32(dup), Events: unix.POLLIN}
		w.try = w.readable
	}
	return w, nil
}

func (w *fdWaiter) close() { w.file.Close() }

func (w *fdWaiter) run(uintptr) bool {
	w.tried = true
	return w.try()
}

// readable is the try of a waiter whose owner settles the fd itself.
func (w *fdWaiter) readable() bool {
	for {
		n, err := unix.Ppoll(w.pfd[:], &w.zero, nil)
		if err == unix.EINTR {
			continue
		}
		if w.pfd[0].Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLNVAL) != 0 {
			err = syscall.EIO
		}
		w.pollErr = err
		return n != 0 || err != nil
	}
}

// wait runs try until it succeeds or timeout passes.
func (w *fdWaiter) wait(timeout time.Duration) error {
	_, err := w.waitUntil(time.Now().Add(timeout))
	return err
}

// waitUntil runs try, at least once, until it succeeds, parking on the fd
// between tries, or until the deadline passes (zero: none) or interrupt is
// called; woken reports an interrupt. The deadline is set under the lock
// interrupt takes, so an interrupt is never overwritten by a later deadline.
func (w *fdWaiter) waitUntil(deadline time.Time) (woken bool, err error) {
	w.pollErr = nil
	w.mu.Lock()
	if w.woken {
		w.woken = false
		w.mu.Unlock()
		w.try()
		return true, w.pollErr
	}
	if !deadline.Equal(w.deadline) {
		if err = w.file.SetReadDeadline(deadline); err != nil {
			w.mu.Unlock()
			return false, err
		}
		w.deadline = deadline
	}
	w.waiting = true
	w.mu.Unlock()

	w.tried = false
	err = w.raw.Read(w.attempt)
	if errors.Is(err, os.ErrDeadlineExceeded) {
		err = nil // periodic progress deadlines are normal wakes
		if !w.tried {
			w.try() // the deadline passed before the first try
		}
	}

	w.mu.Lock()
	w.waiting = false
	woken, w.woken = w.woken, false
	w.mu.Unlock()
	return woken, errors.Join(err, w.pollErr)
}

// interrupt makes the current or the next waitUntil return woken.
func (w *fdWaiter) interrupt() {
	w.mu.Lock()
	w.woken = true
	if w.waiting {
		w.deadline = time.Unix(1, 0)
		_ = w.file.SetReadDeadline(w.deadline)
	}
	w.mu.Unlock()
}
