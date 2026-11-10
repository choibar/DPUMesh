package dmeshgrpc

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc/resolver"
)

// Scheme names DPUMesh targets that follow the DPU's flow scaling:
// "dmesh:///<ip>:<port>". The resolver gives a ClientConn one address per
// stream the DPU asks it to keep to the target (SCALE), and round_robin
// spreads RPCs over the streams that are READY. Address 0 is the target
// itself; address k > 0 is "conn/<k>/<target>", which Dial opens as another
// stream to the same target.
const Scheme = "dmesh"

const roundRobin = `{"loadBalancingConfig":[{"round_robin":{}}]}`

const connPrefix = "conn/"

type scaleTarget struct {
	count     int
	seq       uint32 // last SCALE applied; 0 = none yet
	resolvers map[*scaleResolver]struct{}
}

// scaling is the process's scale state per target, shared by every ClientConn
// of that target: one built later starts at the current count.
var scaling = struct {
	sync.Mutex
	// apply serializes state pushes, so ClientConns see SCALEs in order. It
	// is never taken by Close, which gRPC may call while holding its own locks.
	apply   sync.Mutex
	targets map[string]*scaleTarget
}{targets: make(map[string]*scaleTarget)}

type scaleResolver struct {
	target string
	cc     resolver.ClientConn
}

func (*scaleResolver) ResolveNow(resolver.ResolveNowOptions) {}

func (r *scaleResolver) Close() {
	scaling.Lock()
	if st := scaling.targets[r.target]; st != nil {
		delete(st.resolvers, r)
	}
	scaling.Unlock()
}

func (r *scaleResolver) push(count int) {
	_ = r.cc.UpdateState(resolver.State{
		Addresses:     scaleAddresses(r.target, count),
		ServiceConfig: r.cc.ParseServiceConfig(roundRobin),
	})
}

func scaleAddresses(target string, count int) []resolver.Address {
	if count < 1 {
		count = 1
	}
	out := make([]resolver.Address, count)
	for k := range out {
		addr := target
		if k > 0 {
			addr = connPrefix + strconv.Itoa(k) + "/" + target
		}
		out[k] = resolver.Address{Addr: addr}
	}
	return out
}

type scaleBuilder struct{}

func (scaleBuilder) Scheme() string { return Scheme }

func (scaleBuilder) Build(t resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	target := t.Endpoint()
	if _, err := splitTarget(target); err != nil {
		return nil, err
	}
	r := &scaleResolver{target: target, cc: cc}
	scaling.apply.Lock()
	defer scaling.apply.Unlock()
	scaling.Lock()
	st := scaling.targets[target]
	if st == nil {
		st = &scaleTarget{count: 1, resolvers: make(map[*scaleResolver]struct{})}
		scaling.targets[target] = st
	}
	st.resolvers[r] = struct{}{}
	count := st.count
	scaling.Unlock()
	r.push(count)
	return r, nil
}

// applyScale sets target's stream count for every ClientConn of it and
// returns how many there are. A SCALE older than the last applied one (seq
// wraps) changes nothing.
func applyScale(target string, count int, seq uint32) int {
	if count < 1 {
		count = 1
	}
	scaling.apply.Lock()
	defer scaling.apply.Unlock()
	scaling.Lock()
	st := scaling.targets[target]
	if st == nil {
		st = &scaleTarget{count: 1, resolvers: make(map[*scaleResolver]struct{})}
		scaling.targets[target] = st
	}
	if st.seq != 0 && int32(seq-st.seq) <= 0 {
		n := len(st.resolvers)
		scaling.Unlock()
		return n
	}
	st.seq, st.count = seq, count
	rs := make([]*scaleResolver, 0, len(st.resolvers))
	for r := range st.resolvers {
		rs = append(rs, r)
	}
	scaling.Unlock()
	for _, r := range rs {
		r.push(count)
	}
	return len(rs)
}

// dialAddress strips a "conn/<k>/" stream prefix.
func dialAddress(addr string) (string, error) {
	rest, ok := strings.CutPrefix(addr, connPrefix)
	if !ok {
		return addr, nil
	}
	i := strings.IndexByte(rest, '/')
	if i <= 0 {
		return "", fmt.Errorf("dmeshgrpc: malformed stream address %q", addr)
	}
	if _, err := strconv.Atoi(rest[:i]); err != nil {
		return "", fmt.Errorf("dmeshgrpc: malformed stream address %q", addr)
	}
	return rest[i+1:], nil
}

func splitTarget(addr string) (int, error) {
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, err
	}
	port, err := strconv.Atoi(p)
	if err != nil || port < 1 || port > 65535 {
		return 0, fmt.Errorf("dmeshgrpc: %q has no valid port", addr)
	}
	return port, nil
}
