// Package dmesh adapts HotelReservation to the current process-channel API.
package dmesh

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"dmeshgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
)

type Endpoint struct {
	ID      string `json:"id"`
	DMA     string `json:"dma"`
	TCP     string `json:"tcp"`
	Worker  int    `json:"worker"`
	Enabled bool   `json:"enabled"`
}
type Service struct {
	Endpoints []Endpoint `json:"endpoints"`
	VIP       string     `json:"vip"`
	Port      int        `json:"port"`
	TCP       []string   `json:"tcp"`
}
type Topology struct {
	SchemaVersion int                `json:"schema_version"`
	Services      map[string]Service `json:"services"`
}

var config struct {
	sync.Once
	topology Topology
	err      error
}

func Enabled() bool { return os.Getenv("DSB_TRANSPORT") == "dmesh" }
func topology() (Topology, error) {
	config.Do(func() {
		b, err := os.ReadFile(os.Getenv("DSB_TOPOLOGY"))
		if err != nil {
			config.err = err
			return
		}
		config.err = json.Unmarshal(b, &config.topology)
		if config.err == nil && config.topology.SchemaVersion != 2 {
			config.err = fmt.Errorf("endpoint routing requires topology schema_version=2")
		}
	})
	return config.topology, config.err
}
func service(target string) (string, Service, error) {
	name := target[strings.LastIndex(target, "/")+1:]
	name = strings.SplitN(name, ".", 2)[0]
	t, err := topology()
	if err != nil {
		return "", Service{}, err
	}
	s, ok := t.Services[name]
	if !ok || net.ParseIP(s.VIP).To4() == nil || s.Port < 1 || s.Port > 65535 {
		return "", Service{}, fmt.Errorf("invalid or unknown service %q", name)
	}
	return name, s, nil
}

// DialOptions preserves caller-supplied tracing/keepalive options. DMA resolves
// a service VIP once; the DPU owns backend selection. TCP lists real replicas.
func DialOptions(target string) (string, []grpc.DialOption, error) {
	mode := os.Getenv("DSB_TRANSPORT")
	if mode == "" || mode == "consul" {
		return target, nil, nil
	}
	if mode != "tcp" && mode != "dmesh" {
		return "", nil, fmt.Errorf("unknown transport %q", mode)
	}
	name, s, err := service(target)
	if err != nil {
		return "", nil, err
	}
	if id := os.Getenv("DSB_PROBE_ENDPOINT"); id != "" {
		found := false
		for _, e := range s.Endpoints {
			if e.ID == id {
				host, port, err := net.SplitHostPort(e.DMA)
				if err != nil {
					return "", nil, err
				}
				n, err := strconv.Atoi(port)
				if err != nil {
					return "", nil, err
				}
				s.VIP, s.Port, s.TCP = host, n, []string{e.TCP}
				found = true
			}
		}
		if !found {
			return "", nil, fmt.Errorf("unknown endpoint %q for %s", id, name)
		}
	}
	if mode == "dmesh" {
		address := net.JoinHostPort(s.VIP, strconv.Itoa(s.Port))
		dial := func(ctx context.Context, addr string) (net.Conn, error) {
			if addr != address {
				return nil, fmt.Errorf("unexpected DMA target %q", addr)
			}
			return dmeshgo.DialContext(ctx, s.VIP, s.Port)
		}
		return "passthrough:///" + address, []grpc.DialOption{
			grpc.WithContextDialer(dial),
			grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"pick_first":{}}]}`),
		}, nil
	}
	if len(s.TCP) == 0 {
		return "", nil, fmt.Errorf("no TCP replicas for %s", name)
	}
	rb := manual.NewBuilderWithScheme("dsb-" + name)
	addrs := make([]resolver.Address, len(s.TCP))
	for i, a := range s.TCP {
		addrs[i] = resolver.Address{Addr: a}
	}
	rb.InitialState(resolver.State{Addresses: addrs})
	return rb.Scheme() + ":///" + name, []grpc.DialOption{grpc.WithResolvers(rb),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`)}, nil
}

func Listen(name string, port int) (net.Listener, error) {
	state.Lock()
	defer state.Unlock()
	if state.closing {
		return nil, net.ErrClosed
	}
	var l net.Listener
	var err error
	if Enabled() {
		_, s, e := service(name)
		if e != nil {
			return nil, e
		}
		if _, e := ownEndpoint(s); e != nil {
			return nil, e
		}
		l, err = dmeshgo.ListenService()
	} else {
		l, err = net.Listen("tcp", net.JoinHostPort("", strconv.Itoa(port)))
	}
	if err == nil {
		state.listeners = append(state.listeners, l)
	}
	return l, err
}

func Validate() error {
	mode := os.Getenv("DSB_TRANSPORT")
	if mode != "" && mode != "consul" && mode != "tcp" && mode != "dmesh" {
		return fmt.Errorf("unknown transport %q", mode)
	}
	if mode == "tcp" || mode == "dmesh" {
		if _, err := topology(); err != nil {
			return err
		}
	}
	if !Enabled() {
		return nil
	}
	tls := strings.ToLower(os.Getenv("TLS"))
	if tls != "" && tls != "0" && tls != "false" {
		return fmt.Errorf("DMA proxy requires plaintext gRPC; TLS=%s is unsupported", tls)
	}
	for _, key := range []string{"DPUMESH_SERVER", "DPUMESH_PCI_ADDR", "DPUMESH_POD_IP", "DPUMESH_WORKLOAD", "DPUMESH_CONFIG"} {
		if os.Getenv(key) == "" {
			return fmt.Errorf("missing %s", key)
		}
	}
	return nil
}

// Registration advertises the replica endpoint for DMA. Consul IDs remain unique.
func Registration(name, ip string, port int) (string, int, error) {
	if Enabled() {
		_, s, err := service(name)
		if err != nil {
			return "", 0, err
		}
		e, err := ownEndpoint(s)
		if err != nil {
			return "", 0, err
		}
		host, port, err := net.SplitHostPort(e.DMA)
		if err != nil {
			return "", 0, err
		}
		n, err := strconv.Atoi(port)
		return host, n, err
	}
	if os.Getenv("DSB_TRANSPORT") == "tcp" {
		return "127.0.0.1", port, nil
	}
	return ip, port, nil
}

// An endpoint belongs to exactly one process; a service VIP is never a listener.
func ownEndpoint(s Service) (Endpoint, error) {
	for _, e := range s.Endpoints {
		if e.ID == os.Getenv("DSB_REPLICA") {
			host, _, err := net.SplitHostPort(e.DMA)
			if err != nil || host != os.Getenv("DPUMESH_POD_IP") || e.DMA != os.Getenv("DPUMESH_SERVICE") || fmt.Sprintf("DPUMesh%d", e.Worker) != os.Getenv("DPUMESH_SERVER") {
				return Endpoint{}, fmt.Errorf("replica endpoint/worker does not match process configuration")
			}
			return e, nil
		}
	}
	return Endpoint{}, fmt.Errorf("unknown replica %q", os.Getenv("DSB_REPLICA"))
}
