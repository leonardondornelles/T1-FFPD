package main

import (
	"sync"
	"testing"
	"time"
)

func TestSemaphoreAtomicCheck(t *testing.T) {
	r, err := run(config{Mode: "semaphore", K: 10, Producers: 4, Consumers: 4, Items: 2500, Timeout: time.Millisecond, Sample: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if r.Atomic == nil || !r.Atomic.OK {
		t.Fatalf("asserção atômica: %+v", r.Atomic)
	}
	if r.Atomic.Produced != 10000 || r.Atomic.Consumed != 10000 {
		t.Fatalf("totais atômicos inesperados: %+v", r.Atomic)
	}
	if r.Atomic.Produced != int64(r.Produced) || r.Atomic.Consumed != int64(r.Consumed) {
		t.Fatalf("contadores divergem da contagem por IDs: %+v vs %d/%d", r.Atomic, r.Produced, r.Consumed)
	}
}

func TestAtomicCheckAusenteNoChannel(t *testing.T) {
	r, err := run(config{Mode: "channel", K: 10, Producers: 2, Consumers: 2, Items: 100, Timeout: time.Millisecond, Sample: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if r.Atomic != nil {
		t.Fatalf("channel não deveria reportar asserção atômica: %+v", r.Atomic)
	}
}

func TestSemaphoreFIFO(t *testing.T) {
	const k = 4
	q := newSemaphoreQueue(k)
	for volta := 0; volta < 3; volta++ {
		for i := 1; i <= k; i++ {
			q.put(item{id: volta*k + i})
		}
		if got := q.occupancy(); got != k {
			t.Fatalf("volta %d: ocupação %d, esperado %d", volta, got, k)
		}
		for i := 1; i <= k; i++ {
			v, ok, expired := q.get(0)
			if !ok || expired {
				t.Fatalf("volta %d: ok=%v expired=%v", volta, ok, expired)
			}
			if want := volta*k + i; v.id != want {
				t.Fatalf("volta %d: FIFO quebrado, veio %d esperado %d", volta, v.id, want)
			}
		}
		if got := q.occupancy(); got != 0 {
			t.Fatalf("volta %d: buffer deveria estar vazio, ocupação %d", volta, got)
		}
	}
}

func TestSemaphoreOcupacaoLimitada(t *testing.T) {
	const k, p, c, n = 4, 4, 4, 2000
	q := newSemaphoreQueue(k)
	parar := make(chan struct{})
	erros := make(chan int, 1)
	var vigia sync.WaitGroup
	vigia.Add(1)
	go func() {
		defer vigia.Done()
		for {
			select {
			case <-parar:
				return
			default:
				if got := q.occupancy(); got < 0 || got > k {
					select {
					case erros <- got:
					default:
					}
					return
				}
			}
		}
	}()

	var produtores, consumidores sync.WaitGroup
	consumidores.Add(c)
	for i := 0; i < c; i++ {
		go func() {
			defer consumidores.Done()
			for {
				if _, ok, _ := q.get(0); !ok {
					return
				}
			}
		}()
	}
	produtores.Add(p)
	for i := 0; i < p; i++ {
		go func(id int) {
			defer produtores.Done()
			for j := 0; j < n; j++ {
				q.put(item{id: id*n + j})
			}
		}(i)
	}
	produtores.Wait()
	q.finish(c)
	consumidores.Wait()
	close(parar)
	vigia.Wait()

	select {
	case got := <-erros:
		t.Fatalf("ocupação observada fora de [0,%d]: %d", k, got)
	default:
	}
	produced, consumed, ok := q.integrity()
	if !ok || produced != p*n || consumed != p*n {
		t.Fatalf("integridade: produced=%d consumed=%d ok=%v", produced, consumed, ok)
	}
	if got := q.occupancy(); got != 0 {
		t.Fatalf("buffer não esvaziou: %d", got)
	}
}
