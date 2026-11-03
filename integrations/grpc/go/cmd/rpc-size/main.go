// rpc-size measures unary gRPC echo calls as the message grows: the request and
// the response are both SIZE bytes (protoc-free, the bench package's raw codec).
// One binary is both ends. With DPUMESH_ENABLE=1 the server serves
// DPUMESH_SERVICE and the client's connections are DPUMesh streams; otherwise
// both use TCP, for the no-sidecar baseline.
//
//	rpc-size -role server -port 9000
//	rpc-size -role client -target <ip>:<port> -sizes 64,1024,... -phases latency,rate,max
//
// Per size the client runs each phase in turn, every one after an unmeasured
// warm-up:
//
//	latency  one call outstanding on one connection
//	rate     open loop at -rate calls/s (or the size's entry of -rates) over
//	         -conns connections; latency counts from each call's scheduled start
//	max      closed loop, -m calls outstanding per connection
//
// and prints one line per phase: RESULT_JSON {...} with the window's start and
// end (unix ns), so a harness can take CPU over exactly that window.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"dmeshgo"
	"dmeshgo/bench"
	"dmeshgo/dmeshgrpc"
)

const maxMsg = 64 << 20

func main() {
	role := flag.String("role", "", "server or client")
	port := flag.Int("port", 9000, "server: TCP port (DPUMesh serves DPUMESH_SERVICE)")
	target := flag.String("target", "", "client: <ip>:<port> of the server")
	sizes := flag.String("sizes", "64,1024,16384,262144", "client: message sizes in bytes")
	phases := flag.String("phases", "latency,rate,max", "client: phases per size")
	conns := flag.Int("conns", 4, "client: connections for the rate and max phases")
	m := flag.Int("m", 16, "client: calls outstanding per connection in the max phase")
	rate := flag.Int("rate", 1000, "client: calls/s of the rate phase")
	rates := flag.String("rates", "", "client: calls/s per size for the rate phase (overrides -rate)")
	warm := flag.Duration("warm", 3*time.Second, "client: unmeasured warm-up per phase")
	dur := flag.Duration("dur", 10*time.Second, "client: measured window per phase")
	flag.Parse()
	switch *role {
	case "server":
		serve(*port)
	case "client":
		ss := ints(*sizes)
		rs := ints(*rates)
		if len(rs) != 0 && len(rs) != len(ss) {
			log.Fatal("-rates needs one entry per size")
		}
		os.Exit(client(*target, ss, strings.Split(*phases, ","), *conns, *m, *rate, rs, *warm, *dur))
	default:
		log.Fatal("-role server|client")
	}
}

func ints(s string) []int {
	var out []int
	for _, f := range strings.Split(s, ",") {
		if f == "" {
			continue
		}
		v, err := strconv.Atoi(f)
		if err != nil || v <= 0 {
			log.Fatalf("bad number %q", f)
		}
		out = append(out, v)
	}
	return out
}

func serve(port int) {
	l, err := dmeshgrpc.Listen(fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer(grpc.ForceServerCodec(bench.RawCodec{}), grpc.MaxRecvMsgSize(maxMsg), grpc.MaxSendMsgSize(maxMsg))
	bench.RegisterEcho(s)
	log.Printf("serving (dpumesh=%v) on %v", dmeshgrpc.Enabled(), l.Addr())
	if err := s.Serve(l); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

type result struct {
	Phase    string  `json:"phase"`
	Size     int     `json:"size"`
	Conns    int     `json:"conns"`
	Inflight int     `json:"inflight,omitempty"`
	Offered  int     `json:"offered,omitempty"`
	RPS      float64 `json:"rps"`
	Gbps     float64 `json:"gbps_each_way"`
	Calls    int64   `json:"calls"`
	Errors   int64   `json:"errors"`
	P50      float64 `json:"p50_us"`
	P90      float64 `json:"p90_us"`
	P99      float64 `json:"p99_us"`
	Start    int64   `json:"start_ns"`
	End      int64   `json:"end_ns"`
}

// window collects the calls that complete inside the measured window.
type window struct {
	measuring atomic.Bool
	calls     atomic.Int64
	errors    atomic.Int64
	mu        sync.Mutex
	lats      []time.Duration
}

func (w *window) done(lat time.Duration, err error) {
	if !w.measuring.Load() {
		return
	}
	if err != nil {
		w.errors.Add(1)
		return
	}
	w.calls.Add(1)
	w.mu.Lock()
	if len(w.lats) < 2000000 {
		w.lats = append(w.lats, lat)
	}
	w.mu.Unlock()
}

// run warms up, opens the window, waits dur, closes it, then stops the load.
func (w *window) run(r *result, size int, warm, dur time.Duration, stop func()) {
	time.Sleep(warm)
	start := time.Now()
	w.measuring.Store(true)
	time.Sleep(dur)
	w.measuring.Store(false)
	end := time.Now()
	stop()
	el := end.Sub(start).Seconds()
	r.Size, r.Start, r.End = size, start.UnixNano(), end.UnixNano()
	r.Calls, r.Errors = w.calls.Load(), w.errors.Load()
	r.RPS = float64(r.Calls) / el
	r.Gbps = r.RPS * float64(size) * 8 / 1e9
	w.mu.Lock()
	sort.Slice(w.lats, func(i, j int) bool { return w.lats[i] < w.lats[j] })
	pct := func(q float64) float64 {
		if len(w.lats) == 0 {
			return 0
		}
		return float64(w.lats[int(q*float64(len(w.lats)-1))]) / float64(time.Microsecond)
	}
	r.P50, r.P90, r.P99 = pct(0.5), pct(0.9), pct(0.99)
	w.mu.Unlock()
}

func call(cc *grpc.ClientConn, req []byte, out *[]byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := cc.Invoke(ctx, bench.MethodPing, req, out, grpc.ForceCodec(bench.RawCodec{}))
	if err == nil && len(*out) != len(req) {
		err = fmt.Errorf("echo of %d bytes came back with %d", len(req), len(*out))
	}
	return err
}

// closed keeps `inflight` calls outstanding on each connection.
func closed(phase string, ccs []*grpc.ClientConn, inflight, size int, warm, dur time.Duration) result {
	req := bytes.Repeat([]byte{0xAB}, size)
	var w window
	var stopping atomic.Bool
	var wg sync.WaitGroup
	for _, cc := range ccs {
		for j := 0; j < inflight; j++ {
			wg.Add(1)
			go func(cc *grpc.ClientConn) {
				defer wg.Done()
				var out []byte
				for !stopping.Load() {
					t0 := time.Now()
					err := call(cc, req, &out)
					w.done(time.Since(t0), err)
				}
			}(cc)
		}
	}
	r := result{Phase: phase, Conns: len(ccs), Inflight: inflight}
	w.run(&r, size, warm, dur, func() { stopping.Store(true); wg.Wait() })
	return r
}

// open starts `rps` calls a second, spread over the connections, each on a
// goroutine of its own, whatever the outstanding ones do.
func open(ccs []*grpc.ClientConn, rps, size int, warm, dur time.Duration) result {
	req := bytes.Repeat([]byte{0xAB}, size)
	var w window
	var wg sync.WaitGroup
	done := make(chan struct{})
	go func() {
		interval := time.Second / time.Duration(rps)
		next := time.Now()
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
			}
			next = next.Add(interval)
			if d := time.Until(next); d > 0 {
				time.Sleep(d)
			}
			wg.Add(1)
			go func(cc *grpc.ClientConn, sched time.Time) {
				defer wg.Done()
				var out []byte
				err := call(cc, req, &out)
				w.done(time.Since(sched), err)
			}(ccs[i%len(ccs)], next)
		}
	}()
	r := result{Phase: "rate", Conns: len(ccs), Offered: rps}
	w.run(&r, size, warm, dur, func() { close(done); wg.Wait() })
	return r
}

func client(target string, sizes []int, phases []string, n, m, rate int, rates []int, warm, dur time.Duration) int {
	if target == "" {
		log.Fatal("-target <ip>:<port>")
	}
	ccs := make([]*grpc.ClientConn, n)
	for i := range ccs {
		opts := append(dmeshgrpc.DialOptions(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsg), grpc.MaxCallSendMsgSize(maxMsg)))
		cc, err := grpc.NewClient("passthrough:///"+target, opts...)
		if err != nil {
			log.Fatalf("connection %d: %v", i, err)
		}
		var out []byte
		if err := call(cc, []byte("preflight"), &out); err != nil {
			log.Fatalf("preflight %d: %v", i, err)
		}
		ccs[i] = cc
	}
	log.Printf("%d connections to %s (dpumesh=%v)", n, target, dmeshgrpc.Enabled())
	failed := 0
	for k, size := range sizes {
		for _, ph := range phases {
			var r result
			switch ph {
			case "latency":
				r = closed("latency", ccs[:1], 1, size, warm, dur)
			case "rate":
				rps := rate
				if len(rates) != 0 {
					rps = rates[k]
				}
				r = open(ccs, rps, size, warm, dur)
			case "max":
				r = closed("max", ccs, m, size, warm, dur)
			default:
				log.Fatalf("unknown phase %q", ph)
			}
			if r.Errors != 0 {
				failed = 1
			}
			b, _ := json.Marshal(r)
			fmt.Printf("RESULT_JSON %s\n", b)
			log.Printf("%s %dB: %.0f calls/s (%.2f Gbps each way) p50 %.0f p99 %.0f us errors %d",
				ph, size, r.RPS, r.Gbps, r.P50, r.P99, r.Errors)
		}
	}
	// Stay up a moment so the harness samples the last window's end with this
	// process alive.
	time.Sleep(time.Second)
	for _, cc := range ccs {
		cc.Close()
	}
	if dmeshgrpc.Enabled() {
		if err := dmeshgo.CloseTransport(); err != nil {
			log.Printf("close transport: %v", err)
			failed = 1
		}
	}
	fmt.Println("RPC_SIZE_DONE")
	return failed
}

var _ net.Conn // dmeshgrpc returns net.Conn values
