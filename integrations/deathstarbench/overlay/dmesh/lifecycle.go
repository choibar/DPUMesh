package dmesh

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"dmeshgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

var state struct {
	sync.Mutex
	closing   bool
	servers   []*grpc.Server
	http      []*http.Server
	clients   []*grpc.ClientConn
	listeners []net.Listener
}

func TrackClient(c *grpc.ClientConn) error {
	state.Lock()
	defer state.Unlock()
	if state.closing {
		c.Close()
		return net.ErrClosed
	}
	state.clients = append(state.clients, c)
	return nil
}
func Serve(s *grpc.Server, l net.Listener) error {
	state.Lock()
	if state.closing {
		state.Unlock()
		l.Close()
		s.Stop()
		return net.ErrClosed
	}
	h := health.NewServer()
	h.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(s, h)
	state.servers = append(state.servers, s)
	state.Unlock()
	return s.Serve(l)
}
func ServeHTTP(s *http.Server, cert, key string) error {
	state.Lock()
	if state.closing {
		state.Unlock()
		return net.ErrClosed
	}
	state.http = append(state.http, s)
	state.Unlock()
	if cert != "" {
		return s.ListenAndServeTLS(cert, key)
	}
	return s.ListenAndServe()
}
func shutdown() error {
	state.Lock()
	state.closing = true
	servers, httpServers, clients, listeners := state.servers, state.http, state.clients, state.listeners
	state.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for _, s := range servers {
		wg.Add(1)
		go func(s *grpc.Server) {
			defer wg.Done()
			done := make(chan struct{})
			go func() { s.GracefulStop(); close(done) }()
			select {
			case <-done:
			case <-ctx.Done():
				s.Stop()
				<-done
			}
		}(s)
	}
	for _, s := range httpServers {
		wg.Add(1)
		go func(s *http.Server) {
			defer wg.Done()
			if s.Shutdown(ctx) != nil {
				s.Close()
			}
		}(s)
	}
	wg.Wait()
	var err error
	for _, c := range clients {
		err = errors.Join(err, c.Close())
	}
	for _, l := range listeners {
		if e := l.Close(); e != nil && !errors.Is(e, net.ErrClosed) {
			err = errors.Join(err, e)
		}
	}
	if Enabled() {
		err = errors.Join(err, dmeshgo.CloseTransport())
	}
	return err
}

// Run owns process shutdown, including partial startup failures. Cleanup errors
// remain visible in the command's exit status; they are never turned into success.
func Run(run func() error, deregister func()) error {
	if err := Validate(); err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run() }()
	var result error
	finished := false
	select {
	case result = <-done:
		finished = true
	case <-ctx.Done():
	}
	result = errors.Join(normalExit(result), shutdown())
	if !finished {
		select {
		case e := <-done:
			result = errors.Join(result, normalExit(e))
		case <-time.After(5 * time.Second):
			result = errors.Join(result, fmt.Errorf("server did not stop within deadline"))
		}
	}
	if deregister != nil {
		deregister()
	}
	return result
}
func normalExit(err error) error {
	if errors.Is(err, grpc.ErrServerStopped) || errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}
