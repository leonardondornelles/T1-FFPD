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

type channelQueue struct {
	ch    chan item
	meter *occupancyMeter
}

func (q *channelQueue) put(v item) {
	q.ch <- v
	q.meter.observe(len(q.ch))
}
func (q *channelQueue) get(timeout time.Duration) (item, bool, bool) {
	if timeout == 0 {
		v, ok := <-q.ch
		q.meter.observe(len(q.ch))
		return v, ok, false
	}
	select {
	case v, ok := <-q.ch:
		q.meter.observe(len(q.ch))
		return v, ok, false
	case <-time.After(timeout):
		return item{}, true, true
	}
}
func (q *channelQueue) occupancy() int { return len(q.ch) }
func (q *channelQueue) finish(_ int)   { close(q.ch) }

type config struct {
	Mode                                          string
	K, Producers, Consumers, Items        int
	ProducerDelay, ConsumerDelay, Timeout time.Duration
	Occupancy                             bool
}
type result struct {
	Mode          string       `json:"mode"`
	K             int          `json:"k"`
	Producers     int          `json:"producers"`
	Consumers     int          `json:"consumers"`
	Produced      int          `json:"produced"`
	Consumed      int          `json:"consumed"`
	PerConsumer   []int        `json:"per_consumer"`
	Timeouts      []int        `json:"timeouts"`
	Seconds       float64      `json:"seconds"`
	Throughput    float64      `json:"throughput_items_s"`
	MeanOccupancy float64      `json:"mean_occupancy"`
	PeakOccupancy int          `json:"peak_occupancy"`
	OccupancyWin  float64      `json:"occupancy_window_s"`
	Verified      bool         `json:"verified"`
	Atomic        *atomicCheck `json:"atomic_check,omitempty"`
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
	if c.Timeout <= 0 || c.ProducerDelay < 0 || c.ConsumerDelay < 0 {
		return fmt.Errorf("timeout deve ser positivo; atrasos não negativos")
	}
	return nil
}
func run(c config) (result, error) {
	if err := c.validate(); err != nil {
		return result{}, err
	}
	meter := newOccupancyMeter(c.Occupancy)
	var q queue
	if c.Mode == "channel" {
		q = &channelQueue{make(chan item, c.K), meter}
	} else {
		q = newSemaphoreQueue(c.K, meter)
	}
	r := result{Mode: c.Mode, K: c.K, Producers: c.Producers, Consumers: c.Consumers, PerConsumer: make([]int, c.Consumers), Timeouts: make([]int, c.Consumers)}
	ids := make([][]int, c.Consumers)
	produced := make([]int, c.Producers)
	meter.begin()
	start := time.Now()
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
	meter.stop()
	q.finish(c.Consumers)
	consumers.Wait()
	r.Seconds = time.Since(start).Seconds()
	r.MeanOccupancy, r.PeakOccupancy, r.OccupancyWin = meter.result()
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
	// Execução curta demais para o relógio: throughput fica indefinido em vez de +Inf.
	if r.Seconds > 0 {
		r.Throughput = float64(r.Consumed) / r.Seconds
	}
	if reporter, isReporter := q.(integrityReporter); isReporter {
		produced, consumed, valid := reporter.integrity()
		r.Atomic = &atomicCheck{Produced: produced, Consumed: consumed, OK: valid}
		if !valid || produced != int64(r.Produced) {
			return r, fmt.Errorf("asserção atômica falhou: totalProduzido=%d totalConsumido=%d", produced, consumed)
		}
	}
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
	flag.BoolVar(&c.Occupancy, "occupancy", true, "medir ocupação (desligue para medir throughput sem interferência)")
	asJSON := flag.Bool("json", false, "resultado JSON")
	bench := flag.Bool("bench", false, "executa a matriz de experimentos (K=1,10,100 nas duas versões)")
	benchReps := flag.Int("bench-reps", 5, "repetições por combinação no modo -bench")
	benchJSONL := flag.String("bench-jsonl", "", "arquivo JSONL com os dados brutos do -bench")
	flag.Parse()

	if *bench {
		if err := runBench(c, []string{"channel", "semaphore"}, []int{1, 10, 100}, *benchReps, *benchJSONL, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "Erro:", err)
			os.Exit(1)
		}
		return
	}
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
	fmt.Printf("Tempo=%.6fs | throughput=%.2f itens/s\n", r.Seconds, r.Throughput)
	if r.OccupancyWin > 0 {
		fmt.Printf("Ocupação média (ponderada no tempo)=%.3f/%d | pico=%d | janela=%.6fs\n", r.MeanOccupancy, r.K, r.PeakOccupancy, r.OccupancyWin)
	}
	if r.Atomic != nil {
		fmt.Printf("Asserção atômica: totalProduzido(%d) == totalConsumido(%d) -> %t\n", r.Atomic.Produced, r.Atomic.Consumed, r.Atomic.OK)
	}
	for id, n := range r.PerConsumer {
		fmt.Printf("Consumidor %d: %d itens, %d timeouts\n", id, n, r.Timeouts[id])
	}
}
