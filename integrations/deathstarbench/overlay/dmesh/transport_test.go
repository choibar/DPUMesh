package dmesh

import (
	"net"
	"net/http"
	"os"
	"sync"
	"testing"
)

func setup(t *testing.T) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "topology")
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(`{"schema_version":2,"services":{"srv-search":{"vip":"10.80.0.3","port":8082,"tcp":["127.0.0.1:18082","127.0.0.1:19082"],"endpoints":[{"id":"search-0","dma":"10.81.3.1:8082","worker":0,"enabled":true}]}}}`)
	f.Close()
	config.Once = sync.Once{}
	config.err = nil
	t.Setenv("DSB_TOPOLOGY", f.Name())
}
func TestServiceRouting(t *testing.T) {
	setup(t)
	t.Setenv("DSB_TRANSPORT", "dmesh")
	target, opts, err := DialOptions("consul://127.0.0.1:8500/srv-search.default")
	if err != nil || target != "passthrough:///10.80.0.3:8082" || len(opts) != 2 {
		t.Fatalf("%s %v", target, err)
	}
	if _, _, err = DialOptions("consul://127.0.0.1/srv-unknown"); err == nil {
		t.Fatal("unknown service accepted")
	}
	t.Setenv("DSB_REPLICA", "search-0")
	t.Setenv("DPUMESH_SERVICE", "10.81.3.1:8082")
	t.Setenv("DPUMESH_POD_IP", "10.81.3.1")
	t.Setenv("DPUMESH_SERVER", "DPUMesh0")
	ip, port, err := Registration("srv-search", "host-ip", 18082)
	if err != nil || ip != "10.81.3.1" || port != 8082 {
		t.Fatalf("%s %d %v", ip, port, err)
	}
	t.Setenv("DSB_TRANSPORT", "tcp")
	target, opts, err = DialOptions("srv-search")
	if err != nil || target != "dsb-srv-search:///srv-search" || len(opts) != 2 {
		t.Fatalf("%s %v", target, err)
	}
}
func TestBadModeAndTLS(t *testing.T) {
	setup(t)
	t.Setenv("DSB_TRANSPORT", "typo")
	if Validate() == nil {
		t.Fatal("accepted unknown mode")
	}
	t.Setenv("DSB_TRANSPORT", "dmesh")
	t.Setenv("TLS", "true")
	if Validate() == nil {
		t.Fatal("silently accepted DMA TLS")
	}
}
func TestShutdownRejectsLateServers(t *testing.T) {
	t.Setenv("DSB_TRANSPORT", "tcp")
	state.closing = false
	if err := shutdown(); err != nil {
		t.Fatal(err)
	}
	if _, err := Listen("srv-search", 0); err != net.ErrClosed {
		t.Fatalf("late listener: %v", err)
	}
	if err := ServeHTTP(&http.Server{}, "", ""); err != net.ErrClosed {
		t.Fatalf("late HTTP server: %v", err)
	}
	state.closing = false
}
