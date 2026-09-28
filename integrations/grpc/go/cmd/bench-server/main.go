// gRPC echo server over the DPUMesh host API: one listener for the process's
// DPUMESH_SERVICE at DPUMESH_SERVICE_IP:DPUMESH_SERVICE_PORT; the library keeps
// DPUMESH_BACKEND_POOL spare backend flows for the DPU proxy to claim, one per
// client stream. Serves the raw-codec echo RPC (bench.MethodPing).
package main

import (
	"log"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"

	"dmeshgo"
	"dmeshgo/bench"
)

func main() {
	ip := os.Getenv("DPUMESH_SERVICE_IP")
	port, _ := strconv.Atoi(os.Getenv("DPUMESH_SERVICE_PORT"))
	if ip == "" || port == 0 {
		log.Fatal("DPUMESH_SERVICE_IP and DPUMESH_SERVICE_PORT select the service address")
	}
	lis, err := dmeshgo.ListenAddress(ip, port)
	if err != nil {
		log.Fatalf("listen %s:%d: %v", ip, port, err)
	}
	s := grpc.NewServer(grpc.ForceServerCodec(bench.RawCodec{}), grpc.ConnectionTimeout(24*time.Hour))
	bench.RegisterEcho(s)
	log.Printf("bench-server: serving %s at %s:%d", os.Getenv("DPUMESH_SERVICE"), ip, port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
