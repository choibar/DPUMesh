// Package dmeshgrpc puts a gRPC-go program on DPUMesh by configuration alone.
// With DPUMESH_ENABLE=1, Listen serves the process's DPUMESH_SERVICE target and
// DialOptions route "<ip>:<port>" targets through the DPU. Otherwise both keep
// their TCP behavior, so one build runs with or without the mesh.
package dmeshgrpc

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"

	"dmeshgo"
	"google.golang.org/grpc"
)

// Enabled reports whether DPUMESH_ENABLE is "1".
func Enabled() bool { return os.Getenv("DPUMESH_ENABLE") == "1" }

// Listen serves DPUMESH_SERVICE over DPUMesh when Enabled, and otherwise
// listens on tcpAddr over TCP.
func Listen(tcpAddr string) (net.Listener, error) {
	if !Enabled() {
		return net.Listen("tcp", tcpAddr)
	}
	l, err := dmeshgo.ListenService()
	if err != nil {
		return nil, err
	}
	return l, nil
}

// DialOptions returns the options that route a client's connections over
// DPUMesh when Enabled, and none otherwise.
func DialOptions() []grpc.DialOption {
	if !Enabled() {
		return nil
	}
	return []grpc.DialOption{grpc.WithContextDialer(Dial)}
}

// Dial opens a DPUMesh stream to an "<ip>:<port>" service address. It has the
// signature grpc.WithContextDialer expects.
func Dial(ctx context.Context, addr string) (net.Conn, error) {
	host, p, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(p)
	if err != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf("dmeshgrpc: %q has no valid port", addr)
	}
	return dmeshgo.DialContext(ctx, host, port)
}
