// gRPC echo bench client over the DPUMesh host API: P gRPC connections, each
// on its own DMA stream to DPUMESH_SERVICE_IP:DPUMESH_SERVICE_PORT through the
// DPU proxy, M concurrent Ping RPC loops per connection. Prints RESULT req/s.
//
// Env: BENCH_P (conns, 1), BENCH_M (in-flight per conn, 64), BENCH_PAYLOAD
// (bytes, 64), BENCH_WARM/BENCH_DUR (seconds, 3/10).
package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"dmeshgo"
	"dmeshgo/bench"
)

func envInt(k string, d int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil && v > 0 {
		return v
	}
	return d
}

func main() {
	p := envInt("BENCH_P", 1)
	m := envInt("BENCH_M", 64)
	payload := envInt("BENCH_PAYLOAD", 64)
	warm := time.Duration(envInt("BENCH_WARM", 3)) * time.Second
	dur := time.Duration(envInt("BENCH_DUR", 10)) * time.Second
	ip := os.Getenv("DPUMESH_SERVICE_IP")
	port := envInt("DPUMESH_SERVICE_PORT", 0)
	if ip == "" || port == 0 {
		log.Fatal("DPUMESH_SERVICE_IP and DPUMESH_SERVICE_PORT select the service address")
	}
	req := bytes.Repeat([]byte{0xAB}, payload)

	conns := make([]*grpc.ClientConn, p)
	for i := range conns {
		cc, err := grpc.NewClient(fmt.Sprintf("passthrough:///bench-%d", i),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return dmeshgo.DialContext(ctx, ip, port)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("client %d: %v", i, err)
		}
		conns[i] = cc
	}
	// Preflight: one verified echo per connection before any load.
	pctx, pcancel := context.WithTimeout(context.Background(), 30*time.Second)
	for i, cc := range conns {
		var out []byte
		if err := cc.Invoke(pctx, bench.MethodPing, req, &out, grpc.ForceCodec(bench.RawCodec{})); err != nil {
			log.Fatalf("preflight conn %d failed: %v", i, err)
		}
		if !bytes.Equal(out, req) {
			log.Fatalf("preflight conn %d: echo mismatch (%d bytes back)", i, len(out))
		}
	}
	pcancel()
	log.Printf("preflight OK: %d conns, %dB echo verified", p, payload)

	var total int64
	var stop, measuring atomic.Bool
	var wg sync.WaitGroup
	var latMu sync.Mutex
	var lats []time.Duration
	for _, cc := range conns {
		for j := 0; j < m; j++ {
			wg.Add(1)
			go func(cc *grpc.ClientConn) {
				defer wg.Done()
				var out []byte
				var local int64
				var mine []time.Duration
				ctx := context.Background()
				for !stop.Load() {
					t0 := time.Now()
					if err := cc.Invoke(ctx, bench.MethodPing, req, &out, grpc.ForceCodec(bench.RawCodec{})); err != nil {
						if !stop.Load() {
							log.Printf("rpc error: %v", err)
						}
						return
					}
					if measuring.Load() {
						local++
						if len(mine) < 200000 {
							mine = append(mine, time.Since(t0))
						}
					}
				}
				atomic.AddInt64(&total, local)
				latMu.Lock()
				lats = append(lats, mine...)
				latMu.Unlock()
			}(cc)
		}
	}
	time.Sleep(warm)
	measuring.Store(true)
	start := time.Now()
	time.Sleep(dur)
	measuring.Store(false)
	elapsed := time.Since(start)
	stop.Store(true)
	wg.Wait()
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	pct := func(q float64) time.Duration {
		if len(lats) == 0 {
			return 0
		}
		return lats[int(q*float64(len(lats)-1))]
	}
	log.Printf("RESULT: %.0f req/s (P=%d M=%d payload=%dB dur=%v total=%d) lat_p50=%v p99=%v",
		float64(total)/elapsed.Seconds(), p, m, payload, elapsed.Round(time.Millisecond), total, pct(0.5), pct(0.99))
	for _, cc := range conns {
		cc.Close()
	}
}
