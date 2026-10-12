package dmesh

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

type auditKey struct{}
type auditCounter struct{ Calls, Errors, InBytes, OutBytes, HandlerNs atomic.Uint64 }
type auditHandler struct {
	methods     sync.Map
	connections atomic.Uint64
}

func (h *auditHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	c, _ := h.methods.LoadOrStore(info.FullMethodName, &auditCounter{})
	return context.WithValue(ctx, auditKey{}, c.(*auditCounter))
}
func (h *auditHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	c, _ := ctx.Value(auditKey{}).(*auditCounter)
	if c == nil {
		return
	}
	switch e := s.(type) {
	case *stats.InPayload:
		c.InBytes.Add(uint64(e.Length))
	case *stats.OutPayload:
		c.OutBytes.Add(uint64(e.Length))
	case *stats.End:
		c.Calls.Add(1)
		if e.Error != nil {
			c.Errors.Add(1)
		}
		c.HandlerNs.Add(uint64(e.EndTime.Sub(e.BeginTime)))
	}
}
func (h *auditHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (h *auditHandler) HandleConn(_ context.Context, s stats.ConnStats) {
	if _, ok := s.(*stats.ConnBegin); ok {
		h.connections.Add(1)
	}
}
func (h *auditHandler) dump(dir string) {
	m := map[string]any{}
	h.methods.Range(func(k, v any) bool {
		c := v.(*auditCounter)
		m[k.(string)] = map[string]uint64{"calls": c.Calls.Load(), "errors": c.Errors.Load(), "in_bytes": c.InBytes.Load(), "out_bytes": c.OutBytes.Load(), "handler_ns": c.HandlerNs.Load()}
		return true
	})
	b, _ := json.Marshal(map[string]any{"time": time.Now().UnixNano(), "pid": os.Getpid(), "workload": os.Getenv("DPUMESH_WORKLOAD"), "connections": h.connections.Load(), "methods": m})
	file := filepath.Join(dir, os.Getenv("DPUMESH_WORKLOAD")+".json")
	if os.WriteFile(file+".tmp", b, 0600) == nil {
		os.Rename(file+".tmp", file)
	}
}

// NewServer optionally records replica RPC counters when DSB_AUDIT_DIR is set.
// With no audit directory it is the ordinary gRPC constructor.
func NewServer(opts ...grpc.ServerOption) *grpc.Server {
	if dir := os.Getenv("DSB_AUDIT_DIR"); dir != "" {
		if err := os.MkdirAll(dir, 0700); err != nil {
			panic(err)
		}
		h := &auditHandler{}
		opts = append(opts, grpc.StatsHandler(h))
		go func() {
			for {
				h.dump(dir)
				time.Sleep(500 * time.Millisecond)
			}
		}()
	}
	return grpc.NewServer(opts...)
}
