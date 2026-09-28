// Transport-level echo benchmark over dmeshgo: P connections to the
// service at DPUMESH_SERVICE_IP:DPUMESH_SERVICE_PORT, each streaming fixed-size
// messages with a bounded number in flight while a reader consumes the echoes.
// The peer is apps/dma_bench/dpu/dpumesh_dpu in DMESH_MODE=echo, so the
// numbers compare with the C benchmark (apps/dma_bench/host/dpumesh_host).
//
// Env: BENCH_P (connections, 1), BENCH_SIZE (bytes, 8192), BENCH_WINDOW
// (bytes in flight per connection, 262144), BENCH_DURATION (seconds, 5).
// Prints one line per second and a BENCH_DONE summary with RTT percentiles.
package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"dmeshgo"
)

func envInt(k string, d int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil && v > 0 {
		return v
	}
	return d
}

type stats struct {
	msgs, bytes atomic.Int64
	rttMu       sync.Mutex
	rtts        []time.Duration
}

func (s *stats) rtt(d time.Duration) {
	s.rttMu.Lock()
	if len(s.rtts) < 1<<20 {
		s.rtts = append(s.rtts, d)
	}
	s.rttMu.Unlock()
}

// run drives one connection until stop: the writer keeps up to window bytes
// outstanding, the reader consumes whole messages and records the round trip.
func run(c net.Conn, size, window int, stop *atomic.Bool, st *stats, wg *sync.WaitGroup) {
	defer wg.Done()
	inflight := make(chan time.Time, window/size+1)
	msg := make([]byte, size)
	for i := range msg {
		msg[i] = byte(i)
	}
	var rerr atomic.Value
	var rd sync.WaitGroup
	rd.Add(1)
	go func() {
		defer rd.Done()
		buf := make([]byte, size)
		for {
			if _, err := io.ReadFull(c, buf); err != nil {
				if !stop.Load() {
					rerr.Store(err)
				}
				return
			}
			sent, ok := <-inflight
			if !ok {
				return
			}
			st.rtt(time.Since(sent))
			st.msgs.Add(1)
			st.bytes.Add(int64(size))
		}
	}()
	for !stop.Load() {
		if e := rerr.Load(); e != nil {
			log.Printf("read: %v", e)
			break
		}
		inflight <- time.Now() // blocks while the window is full
		if _, err := c.Write(msg); err != nil {
			if !stop.Load() {
				log.Printf("write: %v", err)
			}
			break
		}
	}
	c.Close() // unblocks the reader
	rd.Wait()
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p * float64(len(sorted)-1))
	return sorted[i]
}

func main() {
	p := envInt("BENCH_P", 1)
	size := envInt("BENCH_SIZE", 8192)
	window := envInt("BENCH_WINDOW", 262144)
	dur := time.Duration(envInt("BENCH_DURATION", 5)) * time.Second
	ip := os.Getenv("DPUMESH_SERVICE_IP")
	port := envInt("DPUMESH_SERVICE_PORT", 0)
	if ip == "" || port == 0 {
		log.Fatal("DPUMESH_SERVICE_IP and DPUMESH_SERVICE_PORT select the service address")
	}
	if window < size {
		window = size
	}

	if path := os.Getenv("BENCH_CPUPROFILE"); path != "" {
		f, err := os.Create(path)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	conns := make([]net.Conn, p)
	for i := range conns {
		c, err := dmeshgo.DialAddress(ip, port)
		if err != nil {
			log.Fatalf("dial %d: %v", i, err)
		}
		conns[i] = c
	}
	log.Printf("bench-echo: %d conns, %d B messages, %d B window, %v", p, size, window, dur)

	var st stats
	var stop atomic.Bool
	var wg sync.WaitGroup
	start := time.Now()
	for _, c := range conns {
		wg.Add(1)
		go run(c, size, window, &stop, &st, &wg)
	}
	// per-second rate line
	var lastMsgs, lastBytes int64
	tick := time.NewTicker(time.Second)
	deadline := time.After(dur)
loop:
	for {
		select {
		case <-tick.C:
			m, b := st.msgs.Load(), st.bytes.Load()
			fmt.Printf("BENCH_RATE msg_per_sec=%d gbps=%.2f\n", m-lastMsgs, float64(b-lastBytes)*8/1e9)
			lastMsgs, lastBytes = m, b
		case <-deadline:
			break loop
		}
	}
	tick.Stop()
	elapsed := time.Since(start)
	stop.Store(true)
	wg.Wait()

	st.rttMu.Lock()
	rtts := st.rtts
	st.rttMu.Unlock()
	sort.Slice(rtts, func(i, j int) bool { return rtts[i] < rtts[j] })
	var sum time.Duration
	for _, r := range rtts {
		sum += r
	}
	avg := time.Duration(0)
	if len(rtts) > 0 {
		avg = sum / time.Duration(len(rtts))
	}
	m, b := st.msgs.Load(), st.bytes.Load()
	fmt.Printf("BENCH_DONE conns=%d size=%d window=%d wall_sec=%.2f msgs=%d msg_per_sec=%.0f gbps=%.2f rtt_avg_us=%.1f rtt_p50_us=%.0f rtt_p99_us=%.0f\n",
		p, size, window, elapsed.Seconds(), m, float64(m)/elapsed.Seconds(), float64(b)*8/elapsed.Seconds()/1e9,
		float64(avg.Microseconds()), float64(percentile(rtts, 0.5).Microseconds()), float64(percentile(rtts, 0.99).Microseconds()))
	if err := dmeshgo.CloseTransport(); err != nil {
		log.Printf("close transport: %v", err)
	}
}
