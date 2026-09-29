// health-bench loads any gRPC server's standard health Check over DPUMesh:
// C gRPC connections to each "<ip>:<port>" service address, each on its own
// DPUMesh stream, then M concurrent Check loops per connection on one target
// at a time. Every connection opens before the first load and closes after
// the last, so no stream closes between measurements. It prints
// MEASURE_START/MEASURE_END around each window and a RESULT line per target,
// and closes its DPUMesh transport before exiting.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dmeshgo"
	"dmeshgo/dmeshgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	targets := flag.String("targets", "", "comma-separated service addresses, <ip>:<port>")
	conns := flag.Int("conns", 1, "gRPC connections per target")
	inflight := flag.Int("m", 64, "concurrent calls per connection")
	warm := flag.Duration("warm", 3*time.Second, "unmeasured warm-up per target")
	dur := flag.Duration("dur", 20*time.Second, "measured duration per target")
	flag.Parse()
	if *targets == "" {
		log.Fatal("-targets <ip>:<port>[,...] selects the service addresses")
	}
	list := strings.Split(*targets, ",")
	var all []*grpc.ClientConn
	clients := make([][]healthpb.HealthClient, len(list))
	for t, target := range list {
		for i := 0; i < *conns; i++ {
			cc, err := grpc.NewClient("passthrough:///"+target,
				grpc.WithContextDialer(dmeshgrpc.Dial),
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Fatalf("%s connection %d: %v", target, i, err)
			}
			all = append(all, cc)
			client := healthpb.NewHealthClient(cc)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if _, err := client.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
				log.Fatalf("%s preflight %d: %v", target, i, err)
			}
			cancel()
			clients[t] = append(clients[t], client)
		}
	}
	for t, target := range list {
		run(target, clients[t], *inflight, *warm, *dur)
	}
	for _, cc := range all {
		cc.Close()
	}
	if err := dmeshgo.CloseTransport(); err != nil {
		log.Fatalf("close transport: %v", err)
	}
}

func run(target string, clients []healthpb.HealthClient, inflight int, warm, dur time.Duration) {
	conns := len(clients)
	var stop, measuring atomic.Bool
	var calls, errors atomic.Int64
	latencies := make([][]time.Duration, conns*inflight)
	var wg sync.WaitGroup
	for i := range latencies {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			client := clients[i%conns]
			for !stop.Load() {
				start := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
				cancel()
				if !measuring.Load() {
					continue
				}
				if err != nil {
					errors.Add(1)
					continue
				}
				calls.Add(1)
				latencies[i] = append(latencies[i], time.Since(start))
			}
		}(i)
	}
	time.Sleep(warm)
	measuring.Store(true)
	fmt.Printf("MEASURE_START %s\n", target)
	time.Sleep(dur)
	measuring.Store(false)
	fmt.Printf("MEASURE_END %s\n", target)
	stop.Store(true)
	wg.Wait()

	var all []time.Duration
	for _, l := range latencies {
		all = append(all, l...)
	}
	sort.Slice(all, func(a, b int) bool { return all[a] < all[b] })
	pct := func(p float64) float64 {
		if len(all) == 0 {
			return 0
		}
		return float64(all[int(p*float64(len(all)-1))].Microseconds())
	}
	fmt.Printf("RESULT target=%s conns=%d m=%d calls=%d calls_per_s=%.0f p50_us=%.0f p99_us=%.0f errors=%d\n",
		target, conns, inflight, calls.Load(), float64(calls.Load())/dur.Seconds(), pct(0.5), pct(0.99), errors.Load())
}
