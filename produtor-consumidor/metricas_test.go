package main

import (
	"math"
	"testing"
	"time"
)

// 1,5 e não 1: 1 seria a média aritmética, sem peso de tempo.
func TestOccupancyMeterPonderaPeloTempo(t *testing.T) {
	m := newOccupancyMeter(true)
	m.begin()
	m.observe(2)
	time.Sleep(30 * time.Millisecond)
	m.observe(0)
	time.Sleep(10 * time.Millisecond)
	m.stop()

	mean, peak, window := m.result()
	if peak != 2 {
		t.Fatalf("pico %d, esperado 2", peak)
	}
	if window < 35e-3 || window > 60e-3 {
		t.Fatalf("janela fora do esperado: %v", window)
	}
	if math.Abs(mean-1.5) > 0.3 {
		t.Fatalf("média ponderada %f, esperado ~1.5", mean)
	}
}

func TestOccupancyMeterDesligado(t *testing.T) {
	m := newOccupancyMeter(false)
	m.begin()
	m.observe(5)
	time.Sleep(time.Millisecond)
	m.stop()
	if mean, peak, window := m.result(); mean != 0 || peak != 0 || window != 0 {
		t.Fatalf("desligado deveria zerar: %f %d %f", mean, peak, window)
	}
}

func TestOccupancyMeterIgnoraAposStop(t *testing.T) {
	m := newOccupancyMeter(true)
	m.begin()
	m.observe(1)
	time.Sleep(5 * time.Millisecond)
	m.stop()
	_, _, windowAntes := m.result()

	m.observe(99)
	time.Sleep(5 * time.Millisecond)
	mean, peak, window := m.result()
	if peak != 1 {
		t.Fatalf("pico %d: sentinela entrou na medição", peak)
	}
	if window != windowAntes {
		t.Fatalf("janela mudou depois de stop: %v -> %v", windowAntes, window)
	}
	if mean > 1.0001 {
		t.Fatalf("média %f contaminada após stop", mean)
	}
}

// Janela zerada (execução abaixo da resolução do relógio) deve anular a média.
func TestOccupancyDentroDosLimitesNasDuasVersoes(t *testing.T) {
	for _, mode := range []string{"channel", "semaphore"} {
		for _, k := range []int{1, 10, 100} {
			r, err := run(config{Mode: mode, K: k, Producers: 4, Consumers: 4, Items: 500, Timeout: time.Millisecond, Occupancy: true})
			if err != nil {
				t.Fatal(err)
			}
			if r.MeanOccupancy < 0 || r.MeanOccupancy > float64(k) {
				t.Fatalf("%s K=%d: média %f fora de [0,%d]", mode, k, r.MeanOccupancy, k)
			}
			if r.PeakOccupancy < 0 || r.PeakOccupancy > k {
				t.Fatalf("%s K=%d: pico %d fora de [0,%d]", mode, k, r.PeakOccupancy, k)
			}
			if r.OccupancyWin < 0 {
				t.Fatalf("%s K=%d: janela negativa %f", mode, k, r.OccupancyWin)
			}
			if r.OccupancyWin == 0 && r.MeanOccupancy != 0 {
				t.Fatalf("%s K=%d: janela zerada deveria anular a média, veio %f", mode, k, r.MeanOccupancy)
			}
		}
	}
}

// Com K=1 e o buffer sempre cheio, area/janela encosta em 1: regressão de ULP.
func TestOccupancyMeterNaoExcedeOPico(t *testing.T) {
	for i := 0; i < 200; i++ {
		r, err := run(config{Mode: "semaphore", K: 1, Producers: 3, Consumers: 7, Items: 300, Timeout: time.Millisecond, Occupancy: true})
		if err != nil {
			t.Fatal(err)
		}
		if r.MeanOccupancy > float64(r.PeakOccupancy) {
			t.Fatalf("média %.17g excede o pico %d", r.MeanOccupancy, r.PeakOccupancy)
		}
		if r.MeanOccupancy > 1 {
			t.Fatalf("média %.17g excede K=1", r.MeanOccupancy)
		}
	}
}
