// Package dmeshgrpc puts a gRPC-go program on DPUMesh by configuration alone.
// With DPUMESH_ENABLE=1, Listen serves the process's DPUMESH_SERVICE target and
// DialOptions route "<ip>:<port>" targets through the DPU. Otherwise both keep
// their TCP behavior, so one build runs with or without the mesh. A target
// written "dmesh:///<ip>:<port>" also follows the DPU's flow scaling (see
// Scheme).
package dmeshgrpc

import (
	"context"
	"net"
	"os"
	"sync"

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
// DPUMesh when Enabled. Otherwise a "dmesh:///" target still resolves, to its
// one address over TCP.
func DialOptions() []grpc.DialOption {
	if !Enabled() {
		return []grpc.DialOption{grpc.WithResolvers(scaleBuilder{})}
	}
	installScale.Do(func() { dmeshgo.SetScaleHandler(applyScale) })
	return []grpc.DialOption{grpc.WithContextDialer(Dial), grpc.WithResolvers(scaleBuilder{})}
}

var installScale sync.Once

// Dial opens a DPUMesh stream to an "<ip>:<port>" service address, or to a
// scaled target's "conn/<k>/<ip>:<port>". It has the signature
// grpc.WithContextDialer expects.
func Dial(ctx context.Context, addr string) (net.Conn, error) {
	addr, err := dialAddress(addr)
	if err != nil {
		return nil, err
	}
	port, err := splitTarget(addr)
	if err != nil {
		return nil, err
	}
	host, _, _ := net.SplitHostPort(addr)
	return dmeshgo.DialContext(ctx, host, port)
}
