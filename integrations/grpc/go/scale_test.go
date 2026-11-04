package dmeshgo

import (
	"context"
	"net"
	"testing"
	"time"
)

// A SCALE event reaches the installed handler and is answered with the count
// of clients the handler applied it to.
func TestScaleEventReachesHandlerAndIsAcked(t *testing.T) {
	type call struct {
		target string
		count  int
		seq    uint32
	}
	calls := make(chan call, 1)
	SetScaleHandler(func(target string, count int, seq uint32) int {
		calls <- call{target, count, seq}
		return 3
	})
	defer SetScaleHandler(nil)

	fn := newFakeNet(1<<20, 8064, 64<<10)
	srv := newTransport(fn.native(true))
	defer srv.close()
	l, err := srv.listen(&net.TCPAddr{Port: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	cn := fn.native(false)
	tr := newTransport(cn)
	defer tr.close()
	c, err := tr.dial(context.Background(), "svc:1", &net.TCPAddr{}, &net.TCPAddr{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	cn.request(nativeEvent{kind: evScale, target: "10.0.0.7:8080", count: 2, scaleToken: 41, token: -1})
	select {
	case got := <-calls:
		if got != (call{"10.0.0.7:8080", 2, 41}) {
			t.Fatalf("handler got %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SCALE never reached the handler")
	}
	eventually(t, "the SCALE ack", func() bool {
		cn.mu.Lock()
		defer cn.mu.Unlock()
		return len(cn.scaleAcks) == 1 && cn.scaleAcks[0] == [2]int{41, 3}
	})
	if peer, err := l.Accept(); err == nil {
		_ = peer.Close()
	}
}
