// gRPC echo (health) server listening ON THE DMA CHANNEL: it registers itself
// with the DPU proxy as the backend for the echo service key, and serves h2
// connections the proxy opens through the channel.
package main

import (
	"log"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"dmeshgo"
)

func main() {

	lis, err := dmeshgo.ListenAddress(serviceIP(), servicePort())
	if err != nil {
		log.Fatalf("dmesh listen: %v", err)
	}
	log.Printf("echo-server: backend channel registered for %s:%d (configured DPUMESH_SERVER)", serviceIP(), servicePort())

	s := grpc.NewServer(grpc.ConnectionTimeout(24 * time.Hour))
	h := health.NewServer()
	h.SetServingStatus("echo", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(s, h)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// The service address is the ClusterIP:port the DPU routes on.
func serviceIP() string {
	v := os.Getenv("DPUMESH_SERVICE_IP")
	if v == "" {
		log.Fatal("DPUMESH_SERVICE_IP is not set")
	}
	return v
}
func servicePort() int {
	p, err := strconv.Atoi(os.Getenv("DPUMESH_SERVICE_PORT"))
	if err != nil || p <= 0 {
		log.Fatal("DPUMESH_SERVICE_PORT is not set")
	}
	return p
}
