package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"
)

type item struct {
	id   int
	stop bool
}
type queue interface {
	put(item)
	get(time.Duration) (item, bool, bool) // item, aberto, timeout
	occupancy() int
	finish(int)
}

type channelQueue struct{ ch chan item }

func (q *channelQueue) put(v item) { q.ch <- v }
func (q *channelQueue) get(timeout time.Duration) (item, bool, bool) {
	if timeout == 0 {
		v, ok := <-q.ch
		return v, ok, false
	}
	select {
	case v, ok := <-q.ch:
		return v, ok, false
	case <-time.After(timeout):
		return item{}, true, true
	}
}
func (q *channelQueue) occupancy() int { return len(q.ch) }
func (q *channelQueue) finish(_ int)   { close(q.ch) }

// Os canais carregam permissões; os itens ficam exclusivamente no buffer circular.
type semaphoreQueue struct {
	notEmpty, notFull chan struct{}
	mutex             sync.Mutex
	buffer            []item
	head, tail, size  int
}

func newSemaphoreQueue(k int) *semaphoreQueue {
	q := &semaphoreQueue{notEmpty: make(chan struct{}, k), notFull: make(chan struct{}, k), buffer: make([]item, k)}
	for i := 0; i < k; i++ {
		q.notFull <- struct{}{}
	}
	return q
}
func (q *semaphoreQueue) put(v item) {
	<-q.notFull // Nunca esperar uma permissão segurando o mutex.
	q.mutex.Lock()
	q.buffer[q.tail] = v
	q.tail = (q.tail + 1) % len(q.buffer)
	q.size++
	q.mutex.Unlock()
	q.notEmpty <- struct{}{}
}
func (q *semaphoreQueue) get(timeout time.Duration) (item, bool, bool) {
	if timeout == 0 {
		<-q.notEmpty
	} else {
		select {
		case <-q.notEmpty:
		case <-time.After(timeout):
			return item{}, true, true
		}
	}
	q.mutex.Lock()
	v := q.buffer[q.head]
	q.buffer[q.head] = item{}
	q.head = (q.head + 1) % len(q.buffer)
	q.size--
	q.mutex.Unlock()
	q.notFull <- struct{}{}
	return v, !v.stop, false
}
func (q *semaphoreQueue) occupancy() int {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	return q.size
}
func (q *semaphoreQueue) finish(consumers int) {
	// Produtores já terminaram: uma sentinela FIFO para cada consumidor.
	for i := 0; i < consumers; i++ {
		q.put(item{stop: true})
	}
}

type config struct {
	Mode                                          string
	K, Producers, Consumers, Items                int
	ProducerDelay, ConsumerDelay, Timeout, Sample time.Duration
}
type result struct {
	Mode          string  `json:"mode"`
	K             int     `json:"k"`
	Producers     int     `json:"producers"`
	Consumers     int     `json:"consumers"`
	Produced      int     `json:"produced"`
	Consumed      int     `json:"consumed"`
	PerConsumer   []int   `json:"per_consumer"`
	Timeouts      []int   `json:"timeouts"`
	Seconds       float64 `json:"seconds"`
	Throughput    float64 `json:"throughput_items_s"`
	MeanOccupancy float64 `json:"mean_occupancy_sampled"`
	Samples       int     `json:"samples"`
	Verified      bool    `json:"verified"`
}

func (c config) validate() error {
	if c.Mode != "channel" && c.Mode != "semaphore" {
		return fmt.Errorf("mode deve ser channel ou semaphore")
	}
	if c.K < 1 || c.Producers < 1 || c.Consumers < 1 || c.Items < 0 {
		return fmt.Errorf("k, p, c devem ser positivos e n não negativo")
	}
	if c.Items > int(^uint(0)>>1)/c.Producers {
		return fmt.Errorf("p*n excede o limite de int")
	}
	if c.Timeout <= 0 || c.Sample <= 0 || c.ProducerDelay < 0 || c.ConsumerDelay < 0 {
		return fmt.Errorf("timeout e sample devem ser positivos; atrasos não negativos")
	}
	return nil
}
func run(c config) (result, error) {
	if err := c.validate(); err != nil {
		return result{}, err
	}
	var q queue
	if c.Mode == "channel" {
		q = &channelQueue{make(chan item, c.K)}
	} else {
		q = newSemaphoreQueue(c.K)
	}
	r := result{Mode: c.Mode, K: c.K, Producers: c.Producers, Consumers: c.Consumers, PerConsumer: make([]int, c.Consumers), Timeouts: make([]int, c.Consumers)}
	ids := make([][]int, c.Consumers)
	produced := make([]int, c.Producers)
	start := time.Now()
	samplingDone := make(chan struct{})
	var sampler sync.WaitGroup
	sampler.Add(1)
	go func() {
		defer sampler.Done()
		ticker := time.NewTicker(c.Sample)
		defer ticker.Stop()
		sum := 0
		sample := func() { sum += q.occupancy(); r.Samples++ }
		sample()
		for {
			select {
			case <-ticker.C:
				sample()
			case <-samplingDone:
				sample()
				r.MeanOccupancy = float64(sum) / float64(r.Samples)
				return
			}
		}
	}()
	var consumers, producers sync.WaitGroup
	consumers.Add(c.Consumers)
	for id := 0; id < c.Consumers; id++ {
		go func(id int) {
			defer consumers.Done()
			timeout := time.Duration(0)
			if id == 0 {
				timeout = c.Timeout
			}
			for {
				v, ok, expired := q.get(timeout)
				if expired {
					r.Timeouts[id]++
					continue
				} // Timeout não significa fim da produção.
				if !ok {
					return
				}
				if c.ConsumerDelay > 0 {
					time.Sleep(c.ConsumerDelay)
				}
				ids[id] = append(ids[id], v.id)
				r.PerConsumer[id]++
			}
		}(id)
	}
	producers.Add(c.Producers)
	for id := 0; id < c.Producers; id++ {
		go func(id int) {
			defer producers.Done()
			for j := 0; j < c.Items; j++ {
				if c.ProducerDelay > 0 {
					time.Sleep(c.ProducerDelay)
				}
				q.put(item{id: id*c.Items + j})
				produced[id]++
			}
		}(id)
	}
	producers.Wait()
	q.finish(c.Consumers)
	consumers.Wait()
	r.Seconds = time.Since(start).Seconds()
	close(samplingDone)
	sampler.Wait()
	for _, n := range produced {
		r.Produced += n
	}
	seen := make([]bool, c.Producers*c.Items)
	for _, list := range ids {
		for _, id := range list {
			if id < 0 || id >= len(seen) || seen[id] {
				return r, fmt.Errorf("item inválido ou duplicado: %d", id)
			}
			seen[id] = true
			r.Consumed++
		}
	}
	r.Verified = r.Produced == r.Consumed && r.Consumed == len(seen) && q.occupancy() == 0
	r.Throughput = float64(r.Consumed) / r.Seconds
	if !r.Verified {
		return r, fmt.Errorf("verificação falhou: produzido=%d consumido=%d esperado=%d", r.Produced, r.Consumed, len(seen))
	}
	return r, nil
}
func main() {
	var c config
	flag.StringVar(&c.Mode, "mode", "channel", "channel ou semaphore")
	flag.IntVar(&c.K, "k", 10, "capacidade do buffer")
	flag.IntVar(&c.Producers, "p", 2, "produtores")
	flag.IntVar(&c.Consumers, "c", 3, "consumidores")
	flag.IntVar(&c.Items, "n", 1000, "itens por produtor")
	flag.DurationVar(&c.ProducerDelay, "producer-delay", 0, "atraso por produção")
	flag.DurationVar(&c.ConsumerDelay, "consumer-delay", 0, "atraso por consumo")
	flag.DurationVar(&c.Timeout, "timeout", 10*time.Millisecond, "espera do consumidor 0")
	flag.DurationVar(&c.Sample, "sample", 100*time.Microsecond, "intervalo de amostragem da ocupação")
	asJSON := flag.Bool("json", false, "resultado JSON")
	flag.Parse()
	r, err := run(c)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Erro:", err)
		os.Exit(1)
	}
	if *asJSON {
		if err := json.NewEncoder(os.Stdout).Encode(r); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	fmt.Printf("Modo=%s K=%d | produzido=%d consumido=%d | verificação=%t\n", r.Mode, r.K, r.Produced, r.Consumed, r.Verified)
	fmt.Printf("Tempo=%.6fs | throughput=%.2f itens/s | ocupação média amostrada=%.3f/%d (%d amostras)\n", r.Seconds, r.Throughput, r.MeanOccupancy, r.K, r.Samples)
	for id, n := range r.PerConsumer {
		fmt.Printf("Consumidor %d: %d itens, %d timeouts\n", id, n, r.Timeouts[id])
	}
}
