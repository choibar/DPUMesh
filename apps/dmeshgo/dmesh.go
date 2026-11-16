// Package dmeshgo exposes the DPUMesh host-side DMA channel (libdmesh_host)
// as net.Conn / net.Listener, so a Go service can dial and serve gRPC (or any
// byte-stream protocol) THROUGH the DPU proxy with no TCP bridge processes.
//
//   - Dial:   an CLIENT channel — the DPU proxy terminates h2 and routes
//     the flow through its outbound L7 stack toward the flow's dst key.
//   - Listen: BACKEND channels — this process registers itself as the backend
//     provider for (ip, port); the proxy connects through the channel instead
//     of dialing TCP.
package dmeshgo

/*
#cgo CFLAGS: -I${SRCDIR}/hostlib -I${SRCDIR}/../../src/transport/common -I${SRCDIR}/../../src/transport/host -I/opt/mellanox/doca/include
#cgo LDFLAGS: -L${SRCDIR}/hostlib/build -ldmesh_hostlib -Wl,-rpath,${SRCDIR}/hostlib/build
#include <stdlib.h>
#include "host_lib.h"
*/
import "C"

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"
	"unsafe"
)

const (
	ModeBackend     = 1 // this process provides the backend for the dst key
	ModeIngressPush = 2 // client flow served by the proxy's outbound stack
)

// PCIAddr is the host PCI function for the comch connection.
var PCIAddr = envOr("DMESH_PCI_ADDR", "94:00.1")

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "dmesh: i/o deadline exceeded" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

// Conn is a byte stream over one DMA channel. One reader goroutine and one
// writer goroutine may use it concurrently (the C side serializes internally).
type Conn struct {
	ch     *C.struct_dmesh_chan
	local  net.Addr
	remote net.Addr

	mu       sync.Mutex
	rd, wd   time.Time
	closed   bool
	closedCh chan struct{}
}

func newConn(ch *C.struct_dmesh_chan, local, remote string) *Conn {
	la, _ := net.ResolveTCPAddr("tcp", local)
	ra, _ := net.ResolveTCPAddr("tcp", remote)
	return &Conn{ch: ch, local: la, remote: ra, closedCh: make(chan struct{})}
}

func (c *Conn) deadline(read bool) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if read {
		return c.rd
	}
	return c.wd
}

func (c *Conn) isClosed() bool {
	select {
	case <-c.closedCh:
		return true
	default:
		return false
	}
}

// Read blocks (with a short backoff poll) until bytes arrive, the deadline
// passes, or the connection closes.
func (c *Conn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	backoff := 2 * time.Microsecond
	for {
		if c.isClosed() {
			return 0, net.ErrClosed
		}
		n := C.dmesh_chan_read(c.ch, unsafe.Pointer(&p[0]), C.size_t(len(p)))
		if n > 0 {
			return int(n), nil
		}
		if n < 0 {
			return 0, net.ErrClosed
		}
		if d := c.deadline(true); !d.IsZero() && time.Now().After(d) {
			return 0, timeoutError{}
		}
		time.Sleep(backoff)
		if backoff < 100*time.Microsecond {
			backoff *= 2
		}
	}
}

// Write pushes all of p into the channel (staging ring backpressure retried).
func (c *Conn) Write(p []byte) (int, error) {
	written := 0
	for written < len(p) {
		if c.isClosed() {
			return written, net.ErrClosed
		}
		n := C.dmesh_chan_write(c.ch, unsafe.Pointer(&p[written]), C.size_t(len(p)-written))
		if n < 0 {
			return written, net.ErrClosed
		}
		written += int(n)
		if int(n) == 0 {
			if d := c.deadline(false); !d.IsZero() && time.Now().After(d) {
				return written, timeoutError{}
			}
			time.Sleep(5 * time.Microsecond)
		}
	}
	return written, nil
}

func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		close(c.closedCh)
		C.dmesh_chan_close(c.ch)
	}
	return nil
}

func (c *Conn) LocalAddr() net.Addr  { return c.local }
func (c *Conn) RemoteAddr() net.Addr { return c.remote }

func (c *Conn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	c.rd, c.wd = t, t
	c.mu.Unlock()
	return nil
}
func (c *Conn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.rd = t
	c.mu.Unlock()
	return nil
}
func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.wd = t
	c.mu.Unlock()
	return nil
}

func (c *Conn) claimed() bool {
	if c.isClosed() {
		return false
	}
	return C.dmesh_chan_claimed(c.ch) != 0
}

func connect(server, srcIP string, srcPort int, dstIP string, dstPort int, workload string, mode int) (*Conn, error) {
	cPci := C.CString(PCIAddr)
	cSrv := C.CString(server)
	cSrc := C.CString(srcIP)
	cDst := C.CString(dstIP)
	cWl := C.CString(workload)
	defer func() {
		C.free(unsafe.Pointer(cPci))
		C.free(unsafe.Pointer(cSrv))
		C.free(unsafe.Pointer(cSrc))
		C.free(unsafe.Pointer(cDst))
		C.free(unsafe.Pointer(cWl))
	}()
	ch := C.dmesh_chan_connect(cPci, cSrv, cSrc, C.uint16_t(srcPort), cDst, C.uint16_t(dstPort), cWl, C.uint32_t(mode))
	if ch == nil {
		return nil, fmt.Errorf("dmesh: channel connect to %s failed (mode %d, dst %s:%d)", server, mode, dstIP, dstPort)
	}
	local := fmt.Sprintf("%s:%d", srcIP, srcPort)
	remote := fmt.Sprintf("%s:%d", dstIP, dstPort)
	return newConn(ch, local, remote), nil
}

// Dial opens a client flow through the DPU proxy toward (dstIP, dstPort).
func Dial(server, srcIP string, srcPort int, dstIP string, dstPort int, workload string) (net.Conn, error) {
	return connect(server, srcIP, srcPort, dstIP, dstPort, workload, ModeIngressPush)
}

// Listener serves flows the proxy routes to (svcIP, svcPort). Accept keeps
// exactly one spare BACKEND channel registered with the proxy: the next
// Accept returns a backend channel only once the DPU proxy has attached a
// flow to it (bytes seen from the DPU), i.e. TCP accept semantics.
//
// One spare channel is always kept registered with the DPU so the proxy can
// connect the next flow immediately, but the spare is NOT handed to the
// caller until it is claimed. The previous design returned the fresh spare
// right away; grpc.Serve then started its ConnectionTimeout (default 120 s)
// on a connection that carries no preface until the proxy actually needs it,
// closed it, and the listener replaced it - burning one DPU slot per
// replica every 120 s (each parked, never freed) and firing a churn wave
// across every service in lockstep.
type Listener struct {
	server, svcIP, workload string
	svcPort                 int
	addr                    net.Addr

	mu     sync.Mutex
	spare  *Conn
	closed bool
}

func Listen(server, svcIP string, svcPort int, workload string) (*Listener, error) {
	addr, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", svcIP, svcPort))
	return &Listener{server: server, svcIP: svcIP, svcPort: svcPort, workload: workload, addr: addr}, nil
}

func (l *Listener) Accept() (net.Conn, error) {
	for {
		if l.isClosed() {
			return nil, net.ErrClosed
		}
		l.mu.Lock()
		spare := l.spare
		l.mu.Unlock()
		if spare == nil {
			c, err := connect(l.server, l.svcIP, l.svcPort, l.svcIP, l.svcPort, l.workload, ModeBackend)
			if err != nil {
				return nil, err
			}
			l.mu.Lock()
			l.spare = c
			l.mu.Unlock()
			spare = c
		}
		// Wait for the proxy to attach a flow. A spare the DPU dropped reads
		// as claimed (dead channels report claimed so nothing waits on them)
		// and is handed out; its first Read returns EOF and the caller closes
		// it, which is the same thing a reset TCP accept would do.
		for !spare.claimed() {
			if l.isClosed() {
				return nil, net.ErrClosed
			}
			if spare.isClosed() {
				break
			}
			time.Sleep(200 * time.Microsecond)
		}
		l.mu.Lock()
		if l.spare == spare {
			l.spare = nil // next Accept registers a fresh spare at once
		}
		l.mu.Unlock()
		if spare.isClosed() {
			continue
		}
		return spare, nil
	}
}

func (l *Listener) isClosed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closed
}

func (l *Listener) Close() error {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	return nil
}

func (l *Listener) Addr() net.Addr { return l.addr }
