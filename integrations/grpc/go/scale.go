package dmeshgo

import (
	"log"
	"sync"
	"time"
)

// Flow scaling. The DPU asks (SCALE) that each client of a destination keep
// count streams to it; the dispatcher places one svc-to-svc pair's streams on
// distinct workers, so the pair's traffic spreads over that many DPU cores.
// The streams belong to the application's client (gRPC opens one per resolver
// address); dmeshgo forwards the count to the handler a client library
// installed and reports how many clients applied it.

var scaleState struct {
	sync.Mutex
	handler func(target string, count int, seq uint32) int
}

// SetScaleHandler installs the function that applies a SCALE to the
// application's clients of target ("ip:port") and returns how many applied
// it. seq orders the requests for one target: a handler ignores one older
// than the last it applied. dmeshgrpc installs its resolver's.
func SetScaleHandler(h func(target string, count int, seq uint32) int) {
	scaleState.Lock()
	scaleState.handler = h
	scaleState.Unlock()
}

// scale runs on its own goroutine for one SCALE, which came on qp.
func (t *transport) scale(qp qpID, target string, count int, token uint32) {
	scaleState.Lock()
	h := scaleState.handler
	scaleState.Unlock()
	applied := 0
	if h != nil {
		applied = h(target, count, token)
	}
	if err := t.n.scaleAck(qp, token, applied); err != nil {
		log.Printf("DMESH_SCALE ack %s: %v", target, err)
	}
	log.Printf("DMESH_SCALE target=%s count=%d applied=%d unix_ns=%d", target, count, applied,
		time.Now().UnixNano())
}
