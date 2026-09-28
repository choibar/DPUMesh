// gRPC echo (health) server listening ON THE DMA CHANNEL: it registers itself
// with the DPU proxy as the backend for its DPUMESH_SERVICE target, and serves
// h2 connections the proxy opens through the channel.
package main

import (
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"dmeshgo"
)

func main() {

	lis, err := dmeshgo.ListenService()
	if err != nil {
		log.Fatalf("dmesh listen: %v", err)
	}
	log.Printf("echo-server: backend channel registered for %s (configured DPUMESH_SERVER)", os.Getenv("DPUMESH_SERVICE"))

	s := grpc.NewServer(grpc.ConnectionTimeout(24 * time.Hour))
	h := health.NewServer()
	h.SetServingStatus("echo", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(s, h)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
