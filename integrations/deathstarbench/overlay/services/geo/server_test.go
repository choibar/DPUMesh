package geo

import (
	"context"
	"sync"
	"testing"

	"github.com/hailocab/go-geoindex"
)

// Run with -race: the dependency lazily writes a global distance cache even
// when each RPC searches a distinct, already-populated index.
func TestConcurrentNearbyAcrossServers(t *testing.T) {
	servers := make([]*Server, 8)
	for n := range servers {
		index := geoindex.NewClusteringIndex()
		for i := 0; i < 16; i++ {
			index.Add(&geoindex.GeoPoint{Pid: string(rune('a' + i)), Plat: 37 + float64(i)*0.001, Plon: -122})
		}
		servers[n] = &Server{index: index}
	}
	var wg sync.WaitGroup
	for _, server := range servers {
		wg.Add(1)
		go func(s *Server) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				points := s.getNearbyPoints(context.Background(), 37+float64(i%10)*0.001, -122)
				if len(points) != maxSearchResults {
					t.Errorf("nearby results = %d, want %d", len(points), maxSearchResults)
					return
				}
			}
		}(server)
	}
	wg.Wait()
}
