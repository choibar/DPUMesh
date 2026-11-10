package dmeshgrpc

import (
	"context"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

// healthServer serves gRPC health over TCP and records each RPC's client
// address, so a test can tell the connections RPCs arrived on apart.
func healthServer(t *testing.T) (string, func() map[string]int) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	peers := make(map[string]int)
	s := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if p, ok := peer.FromContext(ctx); ok {
			mu.Lock()
			peers[p.Addr.String()]++
			mu.Unlock()
		}
		return h(ctx, req)
	}))
	healthpb.RegisterHealthServer(s, health.NewServer())
	go func() { _ = s.Serve(l) }()
	t.Cleanup(s.Stop)
	return l.Addr().String(), func() map[string]int {
		mu.Lock()
		defer mu.Unlock()
		out := make(map[string]int, len(peers))
		for k, v := range peers {
			out[k] = v
		}
		return out
	}
}

func check(t *testing.T, cc *grpc.ClientConn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := healthpb.NewHealthClient(cc).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
}

func TestDisabledStaysOnTCP(t *testing.T) {
	t.Setenv("DPUMESH_ENABLE", "")
	if Enabled() {
		t.Fatal("Enabled with DPUMESH_ENABLE unset")
	}
	l, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if _, ok := l.Addr().(*net.TCPAddr); !ok {
		t.Fatalf("Listen address %v is not TCP", l.Addr())
	}
	// A dmesh:/// target still works, as its one TCP address.
	addr, _ := healthServer(t)
	cc, err := grpc.NewClient("dmesh:///"+addr,
		append(DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	check(t, cc)
}

func TestEnabledAddsDialerAndResolver(t *testing.T) {
	t.Setenv("DPUMESH_ENABLE", "1")
	if !Enabled() {
		t.Fatal("not Enabled with DPUMESH_ENABLE=1")
	}
	if n := len(DialOptions()); n != 2 {
		t.Fatalf("DialOptions has %d options, want 2", n)
	}
}

func TestDialRejectsMalformedAddress(t *testing.T) {
	for _, addr := range []string{"10.0.0.1", "10.0.0.1:x", "10.0.0.1:0", "10.0.0.1:65536",
		"conn/1/10.0.0.1", "conn//10.0.0.1:80", "conn/x/10.0.0.1:80", "conn/1"} {
		if _, err := Dial(context.Background(), addr); err == nil {
			t.Errorf("Dial(%q) succeeded", addr)
		}
	}
}

func TestDialAddressStripsStreamPrefix(t *testing.T) {
	for in, want := range map[string]string{"10.0.0.1:80": "10.0.0.1:80", "conn/3/10.0.0.1:80": "10.0.0.1:80"} {
		if got, err := dialAddress(in); err != nil || got != want {
			t.Errorf("dialAddress(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
}

// fakeCC records the resolver's state pushes.
type fakeCC struct {
	resolver.ClientConn
	mu     sync.Mutex
	states []resolver.State
}

func (f *fakeCC) UpdateState(s resolver.State) error {
	f.mu.Lock()
	f.states = append(f.states, s)
	f.mu.Unlock()
	return nil
}

func (f *fakeCC) ParseServiceConfig(string) *serviceconfig.ParseResult {
	return &serviceconfig.ParseResult{}
}

func (f *fakeCC) addrs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.states) == 0 {
		return nil
	}
	var out []string
	for _, a := range f.states[len(f.states)-1].Addresses {
		out = append(out, a.Addr)
	}
	return out
}

func build(t *testing.T, target string) (*fakeCC, resolver.Resolver) {
	t.Helper()
	cc := &fakeCC{}
	r, err := scaleBuilder{}.Build(resolver.Target{URL: *mustURL(t, "dmesh:///"+target)}, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return cc, r
}

func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Every ClientConn of a target follows its SCALE, one built later starts at
// the current count, an older SCALE changes nothing, and a closed resolver no
// longer counts.
func TestScaleAppliesToEveryClientOfTarget(t *testing.T) {
	const target = "10.9.0.1:8080"
	a, ra := build(t, target)
	b, rb := build(t, target)
	other, ro := build(t, "10.9.0.2:8080")
	defer ro.Close()
	if got := a.addrs(); !equal(got, []string{target}) {
		t.Fatalf("initial addresses %v", got)
	}
	if n := applyScale(target, 3, 10); n != 2 {
		t.Fatalf("applied to %d clients, want 2", n)
	}
	want := []string{target, "conn/1/" + target, "conn/2/" + target}
	for _, cc := range []*fakeCC{a, b} {
		if got := cc.addrs(); !equal(got, want) {
			t.Fatalf("after SCALE 3: %v, want %v", got, want)
		}
	}
	if got := other.addrs(); !equal(got, []string{"10.9.0.2:8080"}) {
		t.Fatalf("another target changed: %v", got)
	}
	if n := applyScale(target, 1, 9); n != 2 {
		t.Fatalf("stale SCALE reported %d clients", n)
	}
	if got := a.addrs(); !equal(got, want) {
		t.Fatalf("stale SCALE applied: %v", got)
	}
	late, rl := build(t, target)
	defer rl.Close()
	if got := late.addrs(); !equal(got, want) {
		t.Fatalf("late client starts at %v, want %v", got, want)
	}
	rb.Close()
	if n := applyScale(target, 1, 11); n != 2 {
		t.Fatalf("after one close applied to %d clients, want 2", n)
	}
	if got := a.addrs(); !equal(got, []string{target}) {
		t.Fatalf("after SCALE 1: %v", got)
	}
	ra.Close()
}

// Over a real gRPC client: SCALE 2 opens a second connection and round_robin
// sends RPCs over both; SCALE 1 returns to one.
func TestScaleSpreadsRPCsOverStreams(t *testing.T) {
	addr, peers := healthServer(t)
	cc, err := grpc.NewClient("dmesh:///"+addr,
		grpc.WithResolvers(scaleBuilder{}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, a string) (net.Conn, error) {
			a, err := dialAddress(a)
			if err != nil {
				return nil, err
			}
			return (&net.Dialer{}).DialContext(ctx, "tcp", a)
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	check(t, cc)
	applyScale(addr, 2, 1)
	deadline := time.Now().Add(5 * time.Second)
	for len(peers()) < 2 && time.Now().Before(deadline) {
		check(t, cc)
	}
	before := peers()
	if len(before) != 2 {
		t.Fatalf("RPCs arrived on %d connections, want 2: %v", len(before), before)
	}
	for i := 0; i < 20; i++ {
		check(t, cc)
	}
	after := peers()
	for p, n := range after {
		if n-before[p] < 5 {
			t.Fatalf("round_robin left connection %s with %d of 20 RPCs: %v", p, n-before[p], after)
		}
	}
}
