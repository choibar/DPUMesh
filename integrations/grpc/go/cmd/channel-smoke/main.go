// channel-smoke verifies shared-channel isolation and native channel reuse over
// real gRPC. Run server and client on separately configured DPUMesh processes.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"dmeshgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const method = "/dmesh.ChannelSmoke/Echo"

var sizes = []int{1, 8064, 8065, 8192, 8193, 65537}

type rawCodec struct{}

func (rawCodec) Name() string { return "raw" }
func (rawCodec) Marshal(v any) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("raw codec: expected []byte, got %T", v)
	}
	return b, nil
}
func (rawCodec) Unmarshal(data []byte, v any) error {
	b, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("raw codec: expected *[]byte, got %T", v)
	}
	*b = append((*b)[:0], data...)
	return nil
}

type echoService interface{ echo() }
type echoServer struct{}

func (echoServer) echo() {}

var service = grpc.ServiceDesc{
	ServiceName: "dmesh.ChannelSmoke",
	HandlerType: (*echoService)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Echo",
		Handler: func(srv any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			var in []byte
			if err := decode(&in); err != nil {
				return nil, err
			}
			handler := func(_ context.Context, request any) (any, error) { return request, nil }
			if interceptor != nil {
				return interceptor(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: method}, handler)
			}
			return in, nil
		},
	}},
}

type peer struct {
	conn     *grpc.ClientConn
	dials    atomic.Int64
	sequence uint64 // used by one caller at a time
	label    string
	rpcLimit time.Duration

	closeMu      sync.Mutex
	closePending int
	closeErr     error
	closeChanged chan struct{}
}

// gRPC may discard the error returned by net.Conn.Close. Observe it directly,
// and wait for every native connection to finish closing before reporting pass.
type observedConn struct {
	net.Conn
	owner *peer
	once  sync.Once
	err   error
}

func (c *observedConn) Close() error {
	c.once.Do(func() {
		c.err = c.Conn.Close()
		p := c.owner
		p.closeMu.Lock()
		p.closePending--
		p.closeErr = errors.Join(p.closeErr, c.err)
		close(p.closeChanged)
		p.closeChanged = make(chan struct{})
		p.closeMu.Unlock()
	})
	return c.err
}

func (p *peer) close(ctx context.Context) error {
	grpcErr := p.conn.Close()
	for {
		p.closeMu.Lock()
		pending, nativeErr, changed := p.closePending, p.closeErr, p.closeChanged
		p.closeMu.Unlock()
		if pending == 0 {
			if nativeErr != nil {
				nativeErr = fmt.Errorf("%s native close: %w", p.label, nativeErr)
			}
			return errors.Join(grpcErr, nativeErr)
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return errors.Join(grpcErr, nativeErr, fmt.Errorf("%s native close incomplete: %w", p.label, ctx.Err()))
		}
	}
}

func openPeer(ctx context.Context, label, ip string, port int, rpcLimit time.Duration) (*peer, error) {
	p := &peer{label: label, rpcLimit: rpcLimit, closeChanged: make(chan struct{})}
	cc, err := grpc.NewClient("passthrough:///"+label,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDisableRetry(),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			p.dials.Add(1)
			conn, err := dmeshgo.DialContext(ctx, ip, port)
			if err != nil {
				return nil, err
			}
			p.closeMu.Lock()
			p.closePending++
			p.closeMu.Unlock()
			return &observedConn{Conn: conn, owner: p}, nil
		}))
	if err != nil {
		return nil, err
	}
	p.conn = cc
	if err := p.check(ctx, 1); err != nil {
		return nil, errors.Join(err, p.close(ctx))
	}
	return p, nil
}

func (p *peer) check(ctx context.Context, size int) error {
	p.sequence++
	payload := make([]byte, size)
	seed := p.sequence
	for _, b := range []byte(p.label) {
		seed = seed*31 + uint64(b)
	}
	for i := range payload {
		payload[i] = byte(seed + uint64(i)*17)
	}
	callCtx, cancel := context.WithTimeout(ctx, p.rpcLimit)
	defer cancel()
	var reply []byte
	if err := p.conn.Invoke(callCtx, method, payload, &reply, grpc.ForceCodec(rawCodec{})); err != nil {
		return fmt.Errorf("%s request %d (%d bytes): %w", p.label, p.sequence, size, err)
	}
	if !bytes.Equal(payload, reply) {
		return fmt.Errorf("%s request %d: payload mismatch (%d sent, %d received)", p.label, p.sequence, size, len(reply))
	}
	if dials := p.dials.Load(); dials != 1 {
		return fmt.Errorf("%s reconnected unexpectedly: %d native dials", p.label, dials)
	}
	return nil
}

func closeTransport(ctx context.Context) error {
	for {
		err := dmeshgo.CloseTransport()
		if !errors.Is(err, syscall.EBUSY) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("channel still busy after connections closed: %w", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func runClient(ctx context.Context, ip string, port, rounds int, rpcLimit time.Duration) error {
	defer func() {
		if err := dmeshgo.CloseTransport(); err != nil {
			log.Printf("final channel cleanup: %v", err)
		}
	}()
	b, err := openPeer(ctx, "sibling-B", ip, port, rpcLimit)
	if err != nil {
		return err
	}
	defer b.close(ctx)
	type trafficResult struct {
		bytes int
		err   error
	}
	trafficCtx, cancelTraffic := context.WithCancel(ctx)
	defer cancelTraffic()
	stop := make(chan struct{})
	done := make(chan trafficResult, 1)
	go func() {
		var transferred, calls int
		for {
			select {
			case <-stop:
				// Enough data to wrap the receive ring, even on a fast test run.
				if transferred >= 2<<20 {
					done <- trafficResult{bytes: transferred}
					return
				}
			default:
			}
			size := sizes[calls%len(sizes)]
			if err := b.check(trafficCtx, size); err != nil {
				done <- trafficResult{bytes: transferred, err: err}
				return
			}
			transferred += size
			calls++
		}
	}()
	for round := 0; round < rounds; round++ {
		select {
		case result := <-done:
			return fmt.Errorf("sibling traffic stopped: %w", result.err)
		default:
		}
		a, err := openPeer(ctx, fmt.Sprintf("transient-A-%d", round), ip, port, rpcLimit)
		if err != nil {
			return err
		}
		var checkErr error
		for _, size := range sizes {
			if checkErr = a.check(ctx, size); checkErr != nil {
				break
			}
		}
		if err := errors.Join(checkErr, a.close(ctx)); err != nil {
			return err
		}
		log.Printf("FLOW_CLOSED round=%d sibling_dials=%d", round+1, b.dials.Load())
	}
	close(stop)
	select {
	case result := <-done:
		if result.err != nil {
			return result.err
		}
		log.Printf("SIBLING_OK bytes=%d native_dials=%d", result.bytes, b.dials.Load())
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := b.check(ctx, 65537); err != nil {
		return err
	}
	if err := b.close(ctx); err != nil {
		return err
	}
	if err := closeTransport(ctx); err != nil {
		return fmt.Errorf("close shared channel: %w", err)
	}
	log.Printf("CHANNEL_CLOSED")

	reopened, err := openPeer(ctx, "reopened-channel", ip, port, rpcLimit)
	if err != nil {
		return err
	}
	for _, size := range sizes {
		if err := reopened.check(ctx, size); err != nil {
			_ = reopened.close(ctx)
			return err
		}
	}
	if err := reopened.close(ctx); err != nil {
		return err
	}
	if err := closeTransport(ctx); err != nil {
		return fmt.Errorf("close reopened channel: %w", err)
	}
	log.Printf("CHANNEL_SMOKE_OK rounds=%d reopened=true", rounds)
	return nil
}

func runServer(ctx context.Context) error {
	listener, err := dmeshgo.ListenService()
	if err != nil {
		return err
	}
	s := grpc.NewServer(grpc.ForceServerCodec(rawCodec{}))
	s.RegisterService(&service, echoServer{})
	done := make(chan error, 1)
	go func() { done <- s.Serve(listener) }()
	log.Printf("CHANNEL_SMOKE_SERVER_READY service=%s", os.Getenv("DPUMESH_SERVICE"))
	select {
	case err = <-done:
	case <-ctx.Done():
		s.Stop()
		err = <-done
	}
	// Serve can also return because Accept failed; close established transports
	// before releasing the listener and shared channel in that path too.
	s.Stop()
	if errors.Is(err, grpc.ErrServerStopped) {
		err = nil
	}
	return errors.Join(err, listener.Close(), dmeshgo.CloseTransport())
}

func main() {
	mode := flag.String("mode", "client", "client or server")
	rounds := flag.Int("rounds", 4, "transient connection open/close cycles")
	timeout := flag.Duration("timeout", 90*time.Second, "overall client deadline")
	rpcLimit := flag.Duration("rpc-timeout", 5*time.Second, "deadline for each verified RPC")
	flag.Parse()
	ip := os.Getenv("DPUMESH_SERVICE_IP")
	port, err := strconv.Atoi(os.Getenv("DPUMESH_SERVICE_PORT"))
	// Only the client dials an address; the server serves DPUMESH_SERVICE.
	if *mode == "client" && (err != nil || ip == "" || port < 1 || port > 65535) ||
		*rounds < 1 || *timeout <= 0 || *rpcLimit <= 0 {
		log.Fatal("set DPUMESH_SERVICE_IP/PORT for the client and positive rounds/timeouts")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	switch *mode {
	case "server":
		err = runServer(ctx)
	case "client":
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
		err = runClient(ctx, ip, port, *rounds, *rpcLimit)
	default:
		err = fmt.Errorf("unknown mode %q", *mode)
	}
	if err != nil {
		log.Fatal(err)
	}
}
