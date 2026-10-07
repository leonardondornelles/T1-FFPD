package main

import (
	"fmt"
	"testing"
	"time"
)

func TestRuns(t *testing.T) {
	for _, mode := range []string{"channel", "semaphore"} {
		for _, k := range []int{1, 10, 100} {
			for _, n := range []int{0, 1, 300} {
				t.Run(fmt.Sprintf("%s/K%d/N%d", mode, k, n), func(t *testing.T) {
					r, err := run(config{Mode: mode, K: k, Producers: 3, Consumers: 7, Items: n, Timeout: time.Millisecond, Sample: time.Microsecond})
					if err != nil {
						t.Fatal(err)
					}
					if !r.Verified || r.Produced != 3*n || r.MeanOccupancy < 0 || r.MeanOccupancy > float64(k) {
						t.Fatalf("resultado inválido: %+v", r)
					}
					sum := 0
					for _, count := range r.PerConsumer {
						sum += count
					}
					if sum != r.Consumed {
						t.Fatal("contagem por consumidor")
					}
				})
			}
		}
	}
}
func TestTimeoutContinues(t *testing.T) {
	for _, mode := range []string{"channel", "semaphore"} {
		r, err := run(config{Mode: mode, K: 1, Producers: 1, Consumers: 1, Items: 3, ProducerDelay: 10 * time.Millisecond, Timeout: time.Millisecond, Sample: time.Millisecond})
		if err != nil || !r.Verified || r.Timeouts[0] == 0 {
			t.Fatalf("%s: %+v %v", mode, r, err)
		}
	}
}
func TestInvalidConfig(t *testing.T) {
	base := config{Mode: "channel", K: 1, Producers: 1, Consumers: 1, Items: 1, Timeout: time.Millisecond, Sample: time.Millisecond}
	cases := []config{base, base, base, base, base, base, base, base}
	cases[0].Mode = "invalid"
	cases[1].K = 0
	cases[2].Producers = 0
	cases[3].Consumers = 0
	cases[4].Items = -1
	cases[5].Timeout = 0
	cases[6].Sample = 0
	cases[7].ConsumerDelay = -1
	for _, c := range cases {
		if _, err := run(c); err == nil {
			t.Fatalf("aceitou %+v", c)
		}
	}
}
