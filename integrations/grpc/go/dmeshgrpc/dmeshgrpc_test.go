package dmeshgrpc

import (
	"context"
	"net"
	"testing"
)

func TestDisabledStaysOnTCP(t *testing.T) {
	t.Setenv("DPUMESH_ENABLE", "")
	if Enabled() {
		t.Fatal("Enabled with DPUMESH_ENABLE unset")
	}
	if opts := DialOptions(); opts != nil {
		t.Fatalf("DialOptions = %v, want none", opts)
	}
	l, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if _, ok := l.Addr().(*net.TCPAddr); !ok {
		t.Fatalf("Listen address %v is not TCP", l.Addr())
	}
}

func TestEnabledAddsDialer(t *testing.T) {
	t.Setenv("DPUMESH_ENABLE", "1")
	if !Enabled() {
		t.Fatal("not Enabled with DPUMESH_ENABLE=1")
	}
	if n := len(DialOptions()); n != 1 {
		t.Fatalf("DialOptions has %d options, want 1", n)
	}
}

func TestDialRejectsMalformedAddress(t *testing.T) {
	for _, addr := range []string{"10.0.0.1", "10.0.0.1:x", "10.0.0.1:0", "10.0.0.1:65536"} {
		if _, err := Dial(context.Background(), addr); err == nil {
			t.Errorf("Dial(%q) succeeded", addr)
		}
	}
}
