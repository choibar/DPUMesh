// channel-bench measures verified unary gRPC echo over the native DPUMesh
// transport. Run server and client in separately configured host processes.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"dmeshgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const method = "/dmesh.ChannelBench/Echo"
const payloadBytes = 64

type rawCodec struct{}

func (rawCodec) Name() string { return "raw" }
func (rawCodec) Marshal(v any) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("raw codec: expected []byte, got %T", v)
	}
	return b, nil
}
func (rawCodec) Unmarshal(data []byte, v any) error {
	b, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("raw codec: expected *[]byte, got %T", v)
	}
	*b = append((*b)[:0], data...)
	return nil
}

type echoService interface{ echo() }
type echoServer struct{}

func (echoServer) echo() {}

var service = grpc.ServiceDesc{
	ServiceName: "dmesh.ChannelBench",
	HandlerType: (*echoService)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Echo",
		Handler: func(srv any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			var in []byte
			if err := decode(&in); err != nil {
				return nil, err
			}
			handler := func(_ context.Context, request any) (any, error) { return request, nil }
			if interceptor != nil {
				return interceptor(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: method}, handler)
			}
			return in, nil
		},
	}},
}

// net.Conn.Close errors are not surfaced by grpc.ClientConn.Close or Stop.
// Track the native closes explicitly so teardown failures invalidate the run.
type closeTracker struct {
	mu      sync.Mutex
	pending int
	err     error
	changed chan struct{}
}

func newCloseTracker() *closeTracker { return &closeTracker{changed: make(chan struct{})} }

func (t *closeTracker) observe(conn net.Conn) net.Conn {
	t.mu.Lock()
	t.pending++
	t.mu.Unlock()
	return &observedConn{Conn: conn, tracker: t}
}

func (t *closeTracker) wait(ctx context.Context) error {
	for {
		t.mu.Lock()
		pending, err, changed := t.pending, t.err, t.changed
		t.mu.Unlock()
		if pending == 0 {
			return err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return errors.Join(err, fmt.Errorf("%d native closes pending: %w", pending, ctx.Err()))
		}
	}
}

type observedConn struct {
	net.Conn
	tracker *closeTracker
	once    sync.Once
	err     error
}

func (c *observedConn) Close() error {
	c.once.Do(func() {
		c.err = c.Conn.Close()
		c.tracker.mu.Lock()
		c.tracker.pending--
		c.tracker.err = errors.Join(c.tracker.err, c.err)
		close(c.tracker.changed)
		c.tracker.changed = make(chan struct{})
		c.tracker.mu.Unlock()
	})
	return c.err
}

type observedListener struct {
	net.Listener
	tracker *closeTracker
}

func (l observedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return l.tracker.observe(c), nil
}

type peer struct {
	conn  *grpc.ClientConn
	dials atomic.Int64
}

func openPeer(index int, ip string, port int, tracker *closeTracker) (*peer, error) {
	p := &peer{}
	cc, err := grpc.NewClient(fmt.Sprintf("passthrough:///channel-bench-%d", index),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDisableRetry(),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{})),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			p.dials.Add(1)
			conn, err := dmeshgo.DialContext(ctx, ip, port)
			if err != nil {
				return nil, err
			}
			return tracker.observe(conn), nil
		}))
	p.conn = cc
	return p, err
}

func closeTransport(ctx context.Context) error {
	for {
		err := dmeshgo.CloseTransport()
		if !errors.Is(err, syscall.EBUSY) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("channel still busy: %w", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func invoke(ctx context.Context, p *peer, request []byte, reply *[]byte, limit time.Duration) error {
	callCtx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	if err := p.conn.Invoke(callCtx, method, request, reply); err != nil {
		return err
	}
	if !bytes.Equal(request, *reply) {
		return fmt.Errorf("payload mismatch: %d bytes sent, %d received", len(request), len(*reply))
	}
	if dials := p.dials.Load(); dials != 1 {
		return fmt.Errorf("unexpected reconnect: %d dial attempts", dials)
	}
	return nil
}

type config struct {
	connections int
	concurrency int
	warmup      time.Duration
	duration    time.Duration
	rpcTimeout  time.Duration
	startFile   string
}

// Split a fixed offered concurrency across connections, e.g. 64 -> 22/21/21.
func distribute(total, connections int) []int {
	d := make([]int, connections)
	for i := range d {
		d[i] = total / connections
		if i < total%connections {
			d[i]++
		}
	}
	return d
}

// Count successful completions in [start,end), including calls crossing the
// warmup boundary. Calls finishing during the final drain are excluded.
func inWindow(completed, start, end time.Time) bool {
	return !completed.Before(start) && completed.Before(end)
}

type workerResult struct {
	latencies []time.Duration
	completed uint64
	errors    uint64
	err       error
}

type result struct {
	Event              string    `json:"event"`
	OK                 bool      `json:"ok"`
	Connections        int       `json:"connections"`
	Concurrency        int       `json:"concurrency"`
	ConcurrencyPerConn []int     `json:"concurrency_per_conn"`
	PayloadBytes       int       `json:"payload_bytes"`
	WarmupSeconds      float64   `json:"warmup_seconds"`
	DurationSeconds    float64   `json:"duration_seconds"`
	ElapsedSeconds     float64   `json:"elapsed_secs"`
	ClientCPUPercent   float64   `json:"client_process_cpu_pct"`
	ClientCPUSeconds   float64   `json:"client_cpu_sample_seconds"`
	MeasurementStart   time.Time `json:"measurement_start"`
	MeasurementEnd     time.Time `json:"measurement_end"`
	Completed          uint64    `json:"completed"`
	TotalCompleted     uint64    `json:"total_completed_including_warmup_and_drain"`
	RPCErrors          uint64    `json:"rpc_errors"`
	NativeDials        []int64   `json:"native_dials"`
	Reconnects         int64     `json:"reconnects"`
	QPS                float64   `json:"qps"`
	MeanLatencyUS      float64   `json:"latency_mean_us"`
	P50LatencyUS       float64   `json:"latency_p50_us"`
	P99LatencyUS       float64   `json:"latency_p99_us"`
	Error              string    `json:"error,omitempty"`
}

// Every measured completion contributes a latency, with no sampling cap.
func summarize(latencies []time.Duration) (mean, p50, p99 float64) {
	if len(latencies) == 0 {
		return 0, 0, 0
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var sum float64
	for _, elapsed := range latencies {
		sum += float64(elapsed)
	}
	percentile := func(p float64) float64 {
		return float64(latencies[int(math.Ceil(p*float64(len(latencies))))-1]) / float64(time.Microsecond)
	}
	return sum / float64(len(latencies)) / float64(time.Microsecond), percentile(0.5), percentile(0.99)
}

func emit(v any) error { return json.NewEncoder(os.Stdout).Encode(v) }

func marker(event string, at time.Time) error {
	return emit(struct {
		Event     string    `json:"event"`
		Timestamp time.Time `json:"timestamp"`
		EmittedAt time.Time `json:"emitted_at"`
	}{event, at, time.Now()})
}

func waitUntil(ctx context.Context, at time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t := time.NewTimer(time.Until(at))
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Project the controller's wall-clock timestamp onto the local monotonic clock
// so later wall-clock adjustments cannot change the measurement duration.
func parseLoadStart(data []byte, now time.Time) (time.Time, error) {
	start, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(data)))
	if err != nil {
		return time.Time{}, fmt.Errorf("expected RFC3339Nano load start: %w", err)
	}
	if !start.After(now) {
		return time.Time{}, fmt.Errorf("load start %s is not in the future (now %s)",
			start.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	}
	return now.Add(start.Sub(now)), nil
}

// Only a missing file is retried. Publish it with rename so a partial write
// cannot be mistaken for an invalid timestamp. The overall deadline also
// bounds controller/barrier failures.
func waitForStartFile(ctx context.Context, path string) (time.Time, error) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return time.Time{}, err
		}
		data, err := os.ReadFile(path)
		if err == nil {
			start, parseErr := parseLoadStart(data, time.Now())
			if parseErr != nil {
				return time.Time{}, fmt.Errorf("start file %q: %w", path, parseErr)
			}
			return start, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return time.Time{}, fmt.Errorf("read start file %q: %w", path, err)
		}
		select {
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func processCPUSeconds() (float64, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, err
	}
	return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6, nil
}

func runClient(ctx context.Context, ip string, port int, c config) (r result, runErr error) {
	r = result{Event: "result", Connections: c.connections, Concurrency: c.concurrency,
		ConcurrencyPerConn: distribute(c.concurrency, c.connections), PayloadBytes: payloadBytes,
		WarmupSeconds: c.warmup.Seconds(), DurationSeconds: c.duration.Seconds()}
	tracker := newCloseTracker()
	var peers []*peer
	// Cleanup runs on both success and error, independently of the load context.
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, p := range peers {
			runErr = errors.Join(runErr, p.conn.Close())
		}
		runErr = errors.Join(runErr, tracker.wait(cleanupCtx), closeTransport(cleanupCtx))
		for _, p := range peers {
			dials := p.dials.Load()
			r.NativeDials = append(r.NativeDials, dials)
			if dials != 1 {
				runErr = errors.Join(runErr, fmt.Errorf("expected one native dial per connection, got %d", dials))
			}
			if dials > 1 {
				r.Reconnects += dials - 1
			}
		}
		r.OK = runErr == nil
		if runErr != nil {
			r.Error = runErr.Error()
		}
	}()
	for i := 0; i < c.connections; i++ {
		p, err := openPeer(i, ip, port, tracker)
		if err != nil {
			return r, err
		}
		peers = append(peers, p)
		payload := bytes.Repeat([]byte{byte(i + 1)}, payloadBytes)
		var reply []byte
		if err := invoke(ctx, p, payload, &reply, c.rpcTimeout); err != nil {
			r.RPCErrors++
			return r, fmt.Errorf("connection %d preflight: %w", i+1, err)
		}
	}
	log.Printf("PREFLIGHT_OK connections=%d concurrency=%v payload=%dB", c.connections, r.ConcurrencyPerConn, payloadBytes)

	loadCtx, cancelLoad := context.WithCancel(ctx)
	defer cancelLoad()
	ready := sync.WaitGroup{}
	ready.Add(c.concurrency)
	begin := make(chan struct{})
	finished := make(chan workerResult, c.concurrency)
	workersJoined := false
	defer func() {
		if !workersJoined {
			// Barrier failure can return before begin is closed. Cancellation
			// releases those workers before native connection cleanup starts.
			cancelLoad()
			for i := 0; i < c.concurrency; i++ {
				<-finished
			}
		}
	}()
	workerID := 0
	for connID, p := range peers {
		for j := 0; j < r.ConcurrencyPerConn[connID]; j++ {
			id := workerID
			workerID++
			go func(p *peer) {
				w := workerResult{latencies: make([]time.Duration, 0, 16384)}
				defer func() { finished <- w }()
				payload := bytes.Repeat([]byte{byte(id + 1)}, payloadBytes)
				binary.LittleEndian.PutUint64(payload[8:16], uint64(id))
				var reply []byte
				ready.Done()
				select {
				case <-begin:
				case <-loadCtx.Done():
					return
				}
				for sequence := uint64(1); ; sequence++ {
					if loadCtx.Err() != nil || !time.Now().Before(r.MeasurementEnd) {
						return
					}
					binary.LittleEndian.PutUint64(payload[:8], sequence)
					started := time.Now()
					err := invoke(loadCtx, p, payload, &reply, c.rpcTimeout)
					completed := time.Now()
					if err != nil {
						w.errors++
						w.err = fmt.Errorf("worker %d request %d: %w", id, sequence, err)
						cancelLoad()
						return
					}
					w.completed++
					if inWindow(completed, r.MeasurementStart, r.MeasurementEnd) {
						w.latencies = append(w.latencies, completed.Sub(started))
					}
				}
			}(p)
		}
	}
	ready.Wait()
	loadStart := time.Now()
	if c.startFile != "" {
		if err := marker("load_ready", time.Now()); err != nil {
			return r, err
		}
		var err error
		loadStart, err = waitForStartFile(loadCtx, c.startFile)
		if err != nil {
			return r, err
		}
		if err := waitUntil(loadCtx, loadStart); err != nil {
			return r, err
		}
	}
	r.MeasurementStart = loadStart.Add(c.warmup)
	r.MeasurementEnd = r.MeasurementStart.Add(c.duration)
	// Closing begin publishes the immutable time window to every worker.
	close(begin)
	if err := waitUntil(loadCtx, r.MeasurementStart); err == nil {
		cpuStart, cpuErr := processCPUSeconds()
		cpuStartAt := time.Now()
		runErr = errors.Join(cpuErr, marker("measure_start", r.MeasurementStart))
		if err = waitUntil(loadCtx, r.MeasurementEnd); err == nil {
			cpuEnd, cpuErr := processCPUSeconds()
			r.ClientCPUSeconds = time.Since(cpuStartAt).Seconds()
			if cpuErr == nil && r.ClientCPUSeconds > 0 {
				r.ClientCPUPercent = 100 * (cpuEnd - cpuStart) / r.ClientCPUSeconds
			}
			runErr = errors.Join(runErr, cpuErr, marker("measure_end", r.MeasurementEnd))
		} else {
			runErr = errors.Join(runErr, err)
		}
	} else {
		runErr = err
	}
	var latencies []time.Duration
	for i := 0; i < c.concurrency; i++ {
		w := <-finished
		r.TotalCompleted += w.completed
		r.RPCErrors += w.errors
		latencies = append(latencies, w.latencies...)
		runErr = errors.Join(runErr, w.err)
	}
	workersJoined = true
	r.Completed = uint64(len(latencies))
	r.ElapsedSeconds = r.MeasurementEnd.Sub(r.MeasurementStart).Seconds()
	r.QPS = float64(r.Completed) / r.ElapsedSeconds
	r.MeanLatencyUS, r.P50LatencyUS, r.P99LatencyUS = summarize(latencies)
	if r.Completed == 0 {
		runErr = errors.Join(runErr, errors.New("no successful RPC completions in measurement window"))
	}
	return r, runErr
}

func runServer(ctx context.Context) error {
	listener, err := dmeshgo.ListenService()
	if err != nil {
		return err
	}
	tracker := newCloseTracker()
	s := grpc.NewServer(grpc.ForceServerCodec(rawCodec{}), grpc.ConnectionTimeout(24*time.Hour))
	s.RegisterService(&service, echoServer{})
	done := make(chan error, 1)
	go func() { done <- s.Serve(observedListener{Listener: listener, tracker: tracker}) }()
	log.Printf("CHANNEL_BENCH_SERVER_READY service=%s", os.Getenv("DPUMESH_SERVICE"))
	select {
	case err = <-done:
	case <-ctx.Done():
		stopped := make(chan struct{})
		go func() { s.GracefulStop(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			s.Stop()
			<-stopped
		}
		err = <-done
	}
	s.Stop()
	if errors.Is(err, grpc.ErrServerStopped) {
		err = nil
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return errors.Join(err, listener.Close(), tracker.wait(cleanupCtx), closeTransport(cleanupCtx))
}

func main() {
	mode := flag.String("mode", "client", "client or server")
	c := config{}
	flag.IntVar(&c.connections, "connections", 1, "number of gRPC/native connections (1..4)")
	flag.IntVar(&c.concurrency, "concurrency", 64, "total concurrent RPC loops, divided across connections")
	flag.DurationVar(&c.warmup, "warmup", 3*time.Second, "warmup before measurement")
	flag.DurationVar(&c.duration, "duration", 10*time.Second, "measurement duration")
	flag.DurationVar(&c.rpcTimeout, "rpc-timeout", 5*time.Second, "deadline for each verified RPC")
	flag.StringVar(&c.startFile, "start-file", "", "optional file containing a common future load-start timestamp (RFC3339Nano)")
	timeout := flag.Duration("timeout", 90*time.Second, "overall client deadline")
	flag.Parse()
	ip := os.Getenv("DPUMESH_SERVICE_IP")
	port, err := strconv.Atoi(os.Getenv("DPUMESH_SERVICE_PORT"))
	// Only the client dials an address; the server serves DPUMESH_SERVICE.
	if *mode == "client" && (err != nil || ip == "" || port < 1 || port > 65535) || c.connections < 1 || c.connections > 4 ||
		c.concurrency < c.connections || c.warmup < 0 || c.duration <= 0 || c.rpcTimeout <= 0 || *timeout <= 0 {
		log.Fatal("set DPUMESH_SERVICE_IP/PORT for the client, connections 1..4, concurrency >= connections, and valid durations")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	switch *mode {
	case "server":
		err = runServer(ctx)
		if err == nil {
			log.Printf("CHANNEL_BENCH_SERVER_CLOSED")
		}
	case "client":
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
		var r result
		r, err = runClient(ctx, ip, port, c)
		err = errors.Join(err, emit(r))
	default:
		err = fmt.Errorf("unknown mode %q", *mode)
	}
	if err != nil {
		log.Fatal(err)
	}
}
