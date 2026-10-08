// gRPC echo server over the DPUMesh host API: one listener for the process's
// DPUMESH_SERVICE target; the DPU creates one backend flow per worker on demand,
// shared by that worker's client streams. Serves the
// raw-codec echo RPC (bench.MethodPing).
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"dmeshgo"
	"dmeshgo/bench"
)

func main() {
	service := os.Getenv("DPUMESH_SERVICE")
	lis, err := dmeshgo.ListenService()
	if err != nil {
		log.Fatalf("listen %s: %v", service, err)
	}
	s := grpc.NewServer(grpc.ForceServerCodec(bench.RawCodec{}), grpc.ConnectionTimeout(24*time.Hour))
	bench.RegisterEcho(s)
	log.Printf("bench-server: serving %s", service)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- s.Serve(lis) }()
	select {
	case err = <-done:
	case <-ctx.Done():
		s.Stop()
		err = <-done
	}
	s.Stop()
	if errors.Is(err, grpc.ErrServerStopped) {
		err = nil
	}
	err = errors.Join(err, lis.Close(), dmeshgo.CloseTransport())
	if err != nil {
		log.Fatalf("serve: %v", err)
	}
}
