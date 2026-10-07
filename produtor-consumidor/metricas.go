package main

import (
	"sync"
	"time"
)

// Integral da ocupação no tempo. Substitui amostragem por ticker, que no Windows
// entrega ~500us para um intervalo pedido de 100us. Desligado, não toca o relógio.
type occupancyMeter struct {
	enabled bool

	mu    sync.Mutex
	open  bool
	level int
	peak  int
	area  float64 // itens * segundos
	last  time.Time
	start time.Time
}

func newOccupancyMeter(enabled bool) *occupancyMeter { return &occupancyMeter{enabled: enabled} }

func (m *occupancyMeter) begin() {
	if !m.enabled {
		return
	}
	m.mu.Lock()
	now := time.Now()
	m.start, m.last, m.open = now, now, true
	m.mu.Unlock()
}

// Nível absoluto, não delta: a corrida entre os dois lados não pode zerar abaixo.
func (m *occupancyMeter) observe(level int) {
	if !m.enabled {
		return
	}
	m.mu.Lock()
	if m.open {
		now := time.Now()
		m.area += float64(m.level) * now.Sub(m.last).Seconds()
		m.last, m.level = now, level
		if level > m.peak {
			m.peak = level
		}
	}
	m.mu.Unlock()
}

// Fecha a janela antes das sentinelas, para que elas não entrem na média.
func (m *occupancyMeter) stop() {
	if !m.enabled {
		return
	}
	m.mu.Lock()
	if m.open {
		now := time.Now()
		m.area += float64(m.level) * now.Sub(m.last).Seconds()
		m.last, m.open = now, false
	}
	m.mu.Unlock()
}

func (m *occupancyMeter) result() (mean float64, peak int, window float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	window = m.last.Sub(m.start).Seconds()
	if window <= 0 {
		return 0, m.peak, 0
	}
	mean = m.area / window
	// Erro de acumulação pode passar do pico por alguns ULPs.
	if mean > float64(m.peak) {
		mean = float64(m.peak)
	}
	return mean, m.peak, window
}
