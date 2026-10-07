package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
)

// Throughput e ocupação em execuções separadas: medir ocupação custa um lock e
// uma leitura de relógio por operação, o que contaminaria o throughput.
func runBench(base config, modes []string, ks []int, reps int, jsonl string, w io.Writer) error {
	if reps < 1 {
		return fmt.Errorf("bench-reps deve ser positivo")
	}
	var raw *os.File
	if jsonl != "" {
		f, err := os.Create(jsonl)
		if err != nil {
			return err
		}
		defer f.Close()
		raw = f
	}

	total := base.Producers * base.Items
	fmt.Fprintf(w, "P=%d C=%d | %d itens por execução (%d por produtor) | %d repetições por combinação\n",
		base.Producers, base.Consumers, total, base.Items, reps)
	fmt.Fprintf(w, "Throughput medido sem instrumentação de ocupação; ocupação medida em execuções à parte.\n\n")
	fmt.Fprintf(w, "%-10s %5s %14s %10s %12s %8s %10s\n", "versão", "K", "throughput", "desvio", "ocup.média", "pico", "cons.0/méd")
	fmt.Fprintf(w, "%-10s %5s %14s %10s %12s %8s %10s\n", "------", "---", "itens/s", "%", "itens", "itens", "razão")

	for _, mode := range modes {
		for _, k := range ks {
			c := base
			c.Mode, c.K = mode, k

			// Aquecimento: a primeira execução paga alocação de heap.
			c.Occupancy = false
			if _, err := run(c); err != nil {
				return fmt.Errorf("%s K=%d: %w", mode, k, err)
			}

			throughput := make([]float64, 0, reps)
			unmeasured := 0
			shares := make([]float64, 0, reps)
			for i := 0; i < reps; i++ {
				r, err := run(c)
				if err != nil {
					return fmt.Errorf("%s K=%d: %w", mode, k, err)
				}
				if r.Seconds <= 0 {
					unmeasured++
					continue
				}
				throughput = append(throughput, r.Throughput)
				if len(r.PerConsumer) > 0 {
					media := float64(r.Consumed) / float64(len(r.PerConsumer))
					if media > 0 {
						shares = append(shares, float64(r.PerConsumer[0])/media)
					}
				}
				if raw != nil {
					if err := json.NewEncoder(raw).Encode(r); err != nil {
						return err
					}
				}
			}

			c.Occupancy = true
			occupancy := make([]float64, 0, reps)
			peak := 0
			for i := 0; i < reps; i++ {
				r, err := run(c)
				if err != nil {
					return fmt.Errorf("%s K=%d (ocupação): %w", mode, k, err)
				}
				if r.OccupancyWin <= 0 {
					unmeasured++
					continue
				}
				occupancy = append(occupancy, r.MeanOccupancy)
				if r.PeakOccupancy > peak {
					peak = r.PeakOccupancy
				}
				if raw != nil {
					if err := json.NewEncoder(raw).Encode(r); err != nil {
						return err
					}
				}
			}

			if unmeasured > 0 {
				return fmt.Errorf("%s K=%d: %d de %d execuções terminaram abaixo da resolução do relógio; aumente -n", mode, k, unmeasured, reps)
			}
			tpMean := mean(throughput)
			fmt.Fprintf(w, "%-10s %5d %14.0f %9.1f%% %12.2f %8d %10.2f\n",
				mode, k, tpMean, 100*stddev(throughput)/tpMean, mean(occupancy), peak, mean(shares))
		}
	}
	if jsonl != "" {
		fmt.Fprintf(w, "\nDados brutos: %s\n", jsonl)
	}
	return nil
}

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sum := 0.0
	for _, x := range xs {
		sum += x
	}
	return sum / float64(len(xs))
}

func stddev(xs []float64) float64 {
	if len(xs) < 2 {
		return 0
	}
	m := mean(xs)
	sum := 0.0
	for _, x := range xs {
		sum += (x - m) * (x - m)
	}
	return math.Sqrt(sum / float64(len(xs)-1))
}

