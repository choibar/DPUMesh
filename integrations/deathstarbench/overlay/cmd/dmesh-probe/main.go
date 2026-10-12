package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/dialer"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/dmesh"
	user "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/user/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func main() {
	target := flag.String("service", "srv-user", "service name")
	business := flag.Bool("login", false, "also verify a real login RPC")
	endpoint := flag.String("endpoint", "", "probe a specific replica instead of the service VIP")
	calls := flag.Int("calls", 1, "business RPCs on the single client connection")
	parallel := flag.Int("parallel", 1, "concurrent business RPCs")
	flag.Parse()
	if *calls < 1 || *parallel < 1 {
		fmt.Fprintln(os.Stderr, "calls and parallel must be positive")
		os.Exit(2)
	}
	if *endpoint != "" {
		os.Setenv("DSB_PROBE_ENDPOINT", *endpoint)
	}
	err := dmesh.Run(func() error {
		c, err := dialer.Dial(*target)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		result, err := healthpb.NewHealthClient(c).Check(ctx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true))
		if err != nil {
			return err
		}
		if result.Status != healthpb.HealthCheckResponse_SERVING {
			return fmt.Errorf("not serving: %v", result)
		}
		cancelled, cancelCall := context.WithCancel(context.Background())
		cancelCall()
		_, e := healthpb.NewHealthClient(c).Check(cancelled, &healthpb.HealthCheckRequest{})
		if status.Code(e) != codes.Canceled {
			return fmt.Errorf("cancellation status: %v", e)
		}
		expired, expire := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer expire()
		_, e = healthpb.NewHealthClient(c).Check(expired, &healthpb.HealthCheckRequest{})
		if status.Code(e) != codes.DeadlineExceeded {
			return fmt.Errorf("deadline status: %v", e)
		}
		if *business {
			var wg sync.WaitGroup
			jobs := make(chan int)
			errors := make(chan error, *calls)
			for i := 0; i < *parallel; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range jobs {
						r, e := user.NewUserClient(c).CheckUser(ctx, &user.Request{Username: "Cornell_30", Password: "0000000000"})
						if e != nil {
							errors <- e
						} else if !r.Correct {
							errors <- fmt.Errorf("seeded login failed")
						}
					}
				}()
			}
			for i := 0; i < *calls; i++ {
				jobs <- i
			}
			close(jobs)
			wg.Wait()
			close(errors)
			for e := range errors {
				return e
			}
		}
		fmt.Printf("PROBE_OK %s login=%v\n", *target, *business)
		return nil
	}, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
