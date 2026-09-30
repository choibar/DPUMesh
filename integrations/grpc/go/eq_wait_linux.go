package dmeshgo

import (
	"errors"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// The native EQ exposes a level-triggered epoll set. Register a duplicate with
// Go's netpoller so waiting parks a goroutine rather than an OS thread in cgo.
// Neither this wrapper nor its readiness check consumes the native doorbells.
type eqWaiter struct {
	file *os.File
	raw  syscall.RawConn
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

func (w *eqWaiter) wait(timeout time.Duration) error {
	if err := w.file.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	var pollErr error
	err := w.raw.Read(func(fd uintptr) bool {
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
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return nil // periodic progress and retained-TX deadlines are normal wakes
	}
	return errors.Join(err, pollErr)
}
