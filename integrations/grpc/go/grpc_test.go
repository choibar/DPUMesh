package dmeshgo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"dmeshgo/bench"
)

// A raw-codec test service: unary echo, a unary call that waits for its
// context, a bidirectional echo stream and a server-streamed burst.
type testService struct {
	cancelled chan error // server-side context errors of cancelled calls
}

var testDesc = grpc.ServiceDesc{
	ServiceName: "dmesh.Test",
	HandlerType: (*any)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Echo", Handler: func(_ any, _ context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
			in := new([]byte)
			if err := dec(in); err != nil {
				return nil, err
			}
			return *in, nil
		}},
		{MethodName: "Hang", Handler: func(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
			if err := dec(new([]byte)); err != nil {
				return nil, err
			}
			<-ctx.Done()
			srv.(*testService).cancelled <- ctx.Err()
			return nil, ctx.Err()
		}},
	},
	Streams: []grpc.StreamDesc{
		{StreamName: "Bidi", ServerStreams: true, ClientStreams: true, Handler: func(srv any, stream grpc.ServerStream) error {
			for {
				in := new([]byte)
				if err := stream.RecvMsg(in); err != nil {
					if stream.Context().Err() != nil {
						srv.(*testService).cancelled <- stream.Context().Err()
					}
					return err
				}
				if err := stream.SendMsg(*in); err != nil {
					return err
				}
			}
		}},
		{StreamName: "Burst", ServerStreams: true, Handler: func(_ any, stream grpc.ServerStream) error {
			in := new([]byte)
			if err := stream.RecvMsg(in); err != nil {
				return err
			}
			for i := 0; i < 64; i++ {
				msg := bytes.Repeat([]byte{byte(i)}, 32<<10)
				if err := stream.SendMsg(msg); err != nil {
					return err
				}
			}
			return nil
		}},
	},
}

var raw = grpc.ForceCodec(bench.RawCodec{})

type grpcRig struct {
	*rig
	svc    *testService
	server *grpc.Server
	served chan error
	cc     *grpc.ClientConn
	dials  int
	mu     sync.Mutex
}

func newGRPCRig(t *testing.T, window, frag, maxPost int, serverOpts ...grpc.ServerOption) *grpcRig {
	t.Helper()
	g := &grpcRig{rig: newRig(t, window, frag, maxPost, true), svc: &testService{cancelled: make(chan error, 16)},
		served: make(chan error, 1)}
	g.server = grpc.NewServer(append([]grpc.ServerOption{grpc.ForceServerCodec(bench.RawCodec{})}, serverOpts...)...)
	g.server.RegisterService(&testDesc, g.svc)
	lis := g.lis
	go func() { g.served <- g.server.Serve(lis) }()
	cc, err := grpc.NewClient("passthrough:///dmesh-test",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			g.mu.Lock()
			g.dials++
			g.mu.Unlock()
			c, err := g.dial(ctx)
			if err != nil {
				return nil, err
			}
			return c, nil
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	g.cc = cc
	// Registered after newRig's cleanup, so this runs first: gRPC stops before
	// the rig closes transports and checks leases.
	t.Cleanup(func() {
		_ = g.cc.Close()
		g.server.Stop()
		<-g.served
		eventually(t, "every stream to close", func() bool {
			return len(g.connsOf(g.cli)) == 0 && len(g.connsOf(g.srv)) == 0
		})
	})
	return g
}

func (g *grpcRig) dialCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.dials
}

func (g *grpcRig) connsOf(tr *transport) []*Conn {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	out := make([]*Conn, 0, len(tr.conns))
	for _, c := range tr.conns {
		out = append(out, c)
	}
	return out
}

func (g *grpcRig) echo(ctx context.Context, req []byte) ([]byte, error) {
	var out []byte
	err := g.cc.Invoke(ctx, "/dmesh.Test/Echo", req, &out, raw)
	return out, err
}

func TestGRPCUnaryMessageSizes(t *testing.T) {
	g := newGRPCRig(t, 256<<10, 8064, 16<<10)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, size := range []int{0, 1, 64, 8064, 8065, 16 << 10, 16<<10 + 1, 65537, 1 << 20} {
		req := make([]byte, size)
		for i := range req {
			req[i] = byte(i*31 + size)
		}
		out, err := g.echo(ctx, req)
		if err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		if !bytes.Equal(out, req) {
			t.Fatalf("size %d: echo mismatch (%d bytes back)", size, len(out))
		}
	}
}

func TestGRPCConcurrentCalls(t *testing.T) {
	g := newGRPCRig(t, 128<<10, 8064, 16<<10)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for w := 0; w < 64; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				req := []byte(fmt.Sprintf("worker %d call %d %s", w, i, bytes.Repeat([]byte{'x'}, (w*i)%3000)))
				out, err := g.echo(ctx, req)
				if err != nil {
					errs <- err
					return
				}
				if !bytes.Equal(out, req) {
					errs <- fmt.Errorf("worker %d call %d: echo mismatch", w, i)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if n := g.dialCount(); n != 1 {
		t.Fatalf("%d dials for one channel", n)
	}
}

// A call deadline travels as an HTTP/2 stream reset: the client reports
// DeadlineExceeded, the server handler's context is cancelled, and the
// connection keeps serving.
func TestGRPCDeadlineCancelsServerContext(t *testing.T) {
	g := newGRPCRig(t, 256<<10, 8064, 16<<10)
	if _, err := g.echo(context.Background(), []byte("warm")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var out []byte
	err := g.cc.Invoke(ctx, "/dmesh.Test/Hang", []byte("x"), &out, raw)
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("hang call: %v", err)
	}
	select {
	case err := <-g.svc.cancelled:
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("server context ended with %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server handler context was never cancelled")
	}
	if out, err := g.echo(context.Background(), []byte("after")); err != nil || string(out) != "after" {
		t.Fatalf("connection after a reset stream: %q, %v", out, err)
	}
	if n := g.dialCount(); n != 1 {
		t.Fatalf("%d dials; the reset must not cost the connection", n)
	}
}

func TestGRPCStreamCancelAndBurst(t *testing.T) {
	g := newGRPCRig(t, 64<<10, 8064, 16<<10)
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := g.cc.NewStream(ctx, &testDesc.Streams[0], "/dmesh.Test/Bidi", raw)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		msg := bytes.Repeat([]byte{byte(i)}, 1000*i)
		if err := stream.SendMsg(msg); err != nil {
			t.Fatal(err)
		}
		in := new([]byte)
		if err := stream.RecvMsg(in); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(*in, msg) {
			t.Fatalf("message %d mismatch", i)
		}
	}
	cancel()
	if err := stream.RecvMsg(new([]byte)); status.Code(err) != codes.Canceled {
		t.Fatalf("cancelled stream: %v", err)
	}
	select {
	case err := <-g.svc.cancelled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("server stream context: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server stream context was never cancelled")
	}

	// A server-streamed burst larger than the transport window arrives whole.
	burst, err := g.cc.NewStream(context.Background(), &testDesc.Streams[1], "/dmesh.Test/Burst", raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := burst.SendMsg([]byte("go")); err != nil {
		t.Fatal(err)
	}
	if err := burst.CloseSend(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		in := new([]byte)
		if err := burst.RecvMsg(in); err != nil {
			t.Fatalf("burst message %d: %v", i, err)
		}
		if len(*in) != 32<<10 || (*in)[0] != byte(i) || (*in)[len(*in)-1] != byte(i) {
			t.Fatalf("burst message %d corrupted", i)
		}
	}
}

// GracefulStop lets an in-flight stream finish, sends GOAWAY and closes;
// the client then fails fast without hanging on the dead connection.
func TestGRPCGracefulStop(t *testing.T) {
	g := newGRPCRig(t, 256<<10, 8064, 16<<10)
	stream, err := g.cc.NewStream(context.Background(), &testDesc.Streams[0], "/dmesh.Test/Bidi", raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.SendMsg([]byte("before")); err != nil {
		t.Fatal(err)
	}
	if err := stream.RecvMsg(new([]byte)); err != nil {
		t.Fatal(err)
	}
	stopped := make(chan struct{})
	go func() { g.server.GracefulStop(); close(stopped) }()
	time.Sleep(20 * time.Millisecond)
	// The open stream still works while the server drains.
	if err := stream.SendMsg([]byte("during")); err != nil {
		t.Fatal(err)
	}
	in := new([]byte)
	if err := stream.RecvMsg(in); err != nil || string(*in) != "during" {
		t.Fatalf("stream during graceful stop: %q, %v", *in, err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if err := stream.RecvMsg(new([]byte)); err == nil {
		t.Fatal("stream did not end")
	}
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("GracefulStop did not finish after the last stream ended")
	}
	if err := <-g.served; err != nil {
		t.Fatalf("Serve after GracefulStop: %v", err)
	}
	g.served <- nil // for the cleanup
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := g.echo(ctx, []byte("late")); status.Code(err) != codes.Unavailable {
		t.Fatalf("call after server stop: %v", err)
	}
}

// Closing the client channel fails an in-flight call promptly.
func TestGRPCClientCloseDuringCall(t *testing.T) {
	g := newGRPCRig(t, 256<<10, 8064, 16<<10)
	done := make(chan error, 1)
	go func() {
		var out []byte
		done <- g.cc.Invoke(context.Background(), "/dmesh.Test/Hang", []byte("x"), &out, raw)
	}()
	time.Sleep(50 * time.Millisecond)
	if err := g.cc.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("in-flight call after close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight call outlived its channel")
	}
	select {
	case err := <-g.svc.cancelled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("server context: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never saw the client go away")
	}
}

// HTTP/2 PINGs from server keepalive cross the transport and are answered,
// so an idle connection survives them and keeps serving.
func TestGRPCKeepalivePings(t *testing.T) {
	if testing.Short() {
		t.Skip("waits through keepalive intervals")
	}
	g := newGRPCRig(t, 256<<10, 8064, 16<<10,
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: time.Second, Timeout: 500 * time.Millisecond}))
	if _, err := g.echo(context.Background(), []byte("warm")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2500 * time.Millisecond)
	if out, err := g.echo(context.Background(), []byte("idle")); err != nil || string(out) != "idle" {
		t.Fatalf("call after keepalive pings: %q, %v", out, err)
	}
	if n := g.dialCount(); n != 1 {
		t.Fatalf("%d dials; keepalive closed the connection", n)
	}
}
