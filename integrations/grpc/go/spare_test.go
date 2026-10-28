package dmeshgo

import (
	"bytes"
	"io"
	"net"
	"testing"
)

// The spare already sent the preface and read the server's first bytes: the
// application's own preface is swallowed and those bytes are read first.
func TestSpareConnReplaysAndSwallowsPreface(t *testing.T) {
	app, peer := net.Pipe()
	defer app.Close()
	defer peer.Close()
	settings := []byte{0, 0, 0, 4, 0, 0, 0, 0, 0}
	sc := &spareConn{Conn: app, prefix: append([]byte(nil), settings...), skip: http2Preface}

	got := make([]byte, 64)
	n, err := sc.Read(got)
	if err != nil || !bytes.Equal(got[:n], settings) {
		t.Fatalf("first read = %x, %v; want the held server bytes", got[:n], err)
	}

	frame := []byte("client settings")
	done := make(chan []byte, 1)
	go func() {
		b := make([]byte, len(frame))
		_, _ = io.ReadFull(peer, b)
		done <- b
	}()
	// Split across writes, as a writer may: part of the preface, then the
	// rest together with the next frame.
	if n, err := sc.Write(http2Preface[:10]); err != nil || n != 10 {
		t.Fatalf("preface head: %d, %v", n, err)
	}
	tail := append(append([]byte(nil), http2Preface[10:]...), frame...)
	if n, err := sc.Write(tail); err != nil || n != len(tail) {
		t.Fatalf("preface tail + frame: %d, %v", n, err)
	}
	if b := <-done; !bytes.Equal(b, frame) {
		t.Fatalf("peer got %q, want only %q", b, frame)
	}
}

func TestSpareConnRejectsMissingPreface(t *testing.T) {
	app, peer := net.Pipe()
	defer app.Close()
	defer peer.Close()
	sc := &spareConn{Conn: app, skip: http2Preface}
	if _, err := sc.Write([]byte("GET / HTTP/1.1\r\n")); err == nil {
		t.Fatal("a non-HTTP/2 first write must fail")
	}
}
