package dmeshgo

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

// Migration spares. The DPU asks (PREPARE) for a stream to a destination
// before it drains the stream it replaces. The spare is opened, sends the
// HTTP/2 client preface so the proxy finishes protocol detection and answers
// with its SETTINGS, and is then reported ready (SPARE_READY); the DPU sends
// GOAWAY on the old stream, and the gRPC client's redial takes the spare with
// the server's first bytes already here.

const (
	spareSetup = 2 * time.Second  // dial + first server bytes
	spareTTL   = 10 * time.Second // an untaken spare is closed after this
)

var http2Preface = []byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")

type spare struct {
	conn   *spareConn
	expiry *time.Timer
}

// spareConn replays the server bytes read while the spare was prepared and
// swallows the preface the application writes again, which the spare sent.
type spareConn struct {
	net.Conn
	mu     sync.Mutex
	prefix []byte
	skip   []byte
}

func (s *spareConn) Read(p []byte) (int, error) {
	s.mu.Lock()
	if len(s.prefix) != 0 {
		n := copy(p, s.prefix)
		s.prefix = s.prefix[n:]
		s.mu.Unlock()
		return n, nil
	}
	s.mu.Unlock()
	return s.Conn.Read(p)
}

func (s *spareConn) Write(p []byte) (int, error) {
	s.mu.Lock()
	if len(s.skip) == 0 {
		s.mu.Unlock()
		return s.Conn.Write(p)
	}
	k := min(len(s.skip), len(p))
	if !bytes.Equal(p[:k], s.skip[:k]) {
		s.mu.Unlock()
		return 0, errors.New("dmesh: spare stream used without the HTTP/2 client preface")
	}
	s.skip = s.skip[k:]
	s.mu.Unlock()
	if k == len(p) {
		return k, nil
	}
	n, err := s.Conn.Write(p[k:])
	return k + n, err
}

// prepareSpare runs on its own goroutine for one PREPARE.
func (t *transport) prepareSpare(target string) {
	start := time.Now()
	host, p, err := net.SplitHostPort(target)
	port, perr := strconv.Atoi(p)
	if err != nil || perr != nil {
		log.Printf("DMESH_SPARE bad target %q", target)
		return
	}
	local := &net.TCPAddr{IP: net.ParseIP(envOr("DPUMESH_POD_IP", ""))}
	remote := &net.TCPAddr{IP: net.ParseIP(host), Port: port}
	ctx, cancel := context.WithTimeout(context.Background(), spareSetup)
	c, err := t.dial(ctx, target, local, remote)
	cancel()
	if err != nil {
		log.Printf("DMESH_SPARE dial %s: %v", target, err)
		return
	}
	buf := make([]byte, 4096)
	n := 0
	if _, err = c.Write(http2Preface); err == nil {
		_ = c.SetReadDeadline(time.Now().Add(spareSetup))
		n, err = c.Read(buf)
		_ = c.SetReadDeadline(time.Time{})
	}
	if err != nil || n == 0 {
		log.Printf("DMESH_SPARE prepare %s: %v", target, err)
		_ = c.Close()
		return
	}
	sc := &spareConn{Conn: c, prefix: buf[:n], skip: http2Preface}
	t.mu.Lock()
	if old := t.spares[target]; old != nil {
		old.expiry.Stop()
		go old.conn.Close()
	}
	s := &spare{conn: sc}
	s.expiry = time.AfterFunc(spareTTL, func() {
		t.mu.Lock()
		owned := t.spares[target] == s
		if owned {
			delete(t.spares, target)
		}
		t.mu.Unlock()
		if owned {
			log.Printf("DMESH_SPARE expired target=%s", target)
			_ = sc.Close()
		}
	})
	t.spares[target] = s
	t.mu.Unlock()
	// Registered first: the GOAWAY this triggers makes gRPC redial at once.
	if err := t.n.spareReady(c.qp); err != nil {
		log.Printf("DMESH_SPARE ready %s: %v", target, err)
	}
	log.Printf("DMESH_SPARE ready target=%s setup_us=%d unix_ns=%d", target,
		time.Since(start).Microseconds(), time.Now().UnixNano())
}

// takeSpare returns the prepared stream to target, if any.
func (t *transport) takeSpare(target string) net.Conn {
	t.mu.Lock()
	s := t.spares[target]
	if s != nil {
		delete(t.spares, target)
	}
	t.mu.Unlock()
	if s == nil {
		return nil
	}
	s.expiry.Stop()
	log.Printf("DMESH_SPARE taken target=%s unix_ns=%d", target, time.Now().UnixNano())
	return s.conn
}
