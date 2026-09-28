// gRPC echo server over the DPUMesh host API: one listener for the process's
// DPUMESH_SERVICE target; the library keeps DPUMESH_BACKEND_POOL spare backend
// flows for the DPU proxy to claim, one per client stream. Serves the
// raw-codec echo RPC (bench.MethodPing).
package main

import (
	"log"
	"os"
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
	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
