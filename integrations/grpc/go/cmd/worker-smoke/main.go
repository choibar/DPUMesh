// worker-smoke checks concurrent first use, connection isolation, and recovery
// after an externally orchestrated backend server restart. It uses bench-server.
package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"dmeshgo"
	"dmeshgo/bench"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var sequence atomic.Uint64

func check(cc *grpc.ClientConn, size int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := make([]byte, size)
	// Distinct payloads detect cross-flow replies and stale buffer reuse.
	tag := sequence.Add(1)
	for i := range req {
		req[i] = byte(tag >> uint((i%8)*8))
	}
	var out []byte
	if err := cc.Invoke(ctx, bench.MethodPing, req, &out, grpc.ForceCodec(bench.RawCodec{})); err != nil {
		return err
	}
	if !bytes.Equal(req, out) {
		return fmt.Errorf("echo mismatch at %d bytes", size)
	}
	return nil
}
func run() (err error) {
	ip := os.Getenv("DPUMESH_SERVICE_IP")
	port, err := strconv.Atoi(os.Getenv("DPUMESH_SERVICE_PORT"))
	if err != nil {
		return err
	}
	conns := make([]*grpc.ClientConn, 4)
	defer func() {
		for _, cc := range conns {
			if cc != nil {
				err = errors.Join(err, cc.Close())
			}
		}
		err = errors.Join(err, dmeshgo.CloseTransport())
	}()
	for i := range conns {
		cc, err := grpc.NewClient(fmt.Sprintf("passthrough:///worker-smoke-%d", i), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return dmeshgo.DialContext(ctx, ip, port) }), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		conns[i] = cc
	}
	// Start all connections and first RPCs concurrently to exercise deduplication.
	errs := make(chan error, len(conns))
	var wg sync.WaitGroup
	for _, cc := range conns {
		wg.Add(1)
		go func(cc *grpc.ClientConn) { defer wg.Done(); errs <- check(cc, 64) }(cc)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	fmt.Println("CONCURRENT_FIRST_RPC_OK")
	// The other flows continue issuing RPCs while one flow closes.
	errs = make(chan error, 3)
	for _, cc := range conns[1:] {
		wg.Add(1)
		go func(cc *grpc.ClientConn) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if err := check(cc, 64); err != nil {
					errs <- err
					return
				}
			}
		}(cc)
	}
	for i := 0; i < 1; i++ {
		if err := conns[i].Close(); err != nil {
			return err
		}
		conns[i] = nil
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	fmt.Println("PARTIAL_CLOSE_OK")
	fmt.Println("RESTART_READY")
	if _, err := bufio.NewReader(os.Stdin).ReadString('\n'); err != nil {
		return err
	}
	// Keep the client-side flows alive while the server channel is replaced.
	for _, cc := range conns[1:] {
		deadline := time.Now().Add(30 * time.Second)
		for {
			err := check(cc, 64)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("restart recovery: %w", err)
			}
			time.Sleep(100 * time.Millisecond)
		}
		for _, size := range []int{1, 8064, 8065, 8192, 8193, 65537, 1048577} {
			if err := check(cc, size); err != nil {
				return err
			}
		}
	}
	fmt.Println("SERVER_RESTART_RECOVERY_OK")
	return nil
}
func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("WORKER_SMOKE_OK")
}
