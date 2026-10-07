package main

import (
	"sync"
	"sync/atomic"
	"time"
)

const cacheLine = 64

type semaphore chan struct{}

func newSemaphore(capacity, permits int) semaphore {
	s := make(semaphore, capacity)
	for i := 0; i < permits; i++ {
		s <- struct{}{}
	}
	return s
}

func (s semaphore) acquire() { <-s }
func (s semaphore) release() { s <- struct{}{} }

func (s semaphore) acquireTimeout(d time.Duration) bool {
	select {
	case <-s:
		return true
	case <-time.After(d):
		return false
	}
}

// Algoritmo clássico. A visibilidade entre produtor e consumidor vem dos
// semáforos, não dos mutexes, que são disjuntos.
type semaphoreQueue struct {
	notFull, notEmpty semaphore
	buffer            []item

	mutexIn  sync.Mutex
	in       int
	produced atomic.Int64
	_        [cacheLine]byte // separa os dois lados: evita falso compartilhamento

	mutexOut sync.Mutex
	out      int
	consumed atomic.Int64
	_        [cacheLine]byte

	count atomic.Int64
}

func newSemaphoreQueue(k int) *semaphoreQueue {
	return &semaphoreQueue{
		notFull:  newSemaphore(k, k),
		notEmpty: newSemaphore(k, 0),
		buffer:   make([]item, k),
	}
}

func (q *semaphoreQueue) put(v item) {
	q.notFull.acquire() // nunca esperar por espaço segurando o mutex
	q.mutexIn.Lock()
	q.buffer[q.in] = v
	q.in = (q.in + 1) % len(q.buffer)
	q.mutexIn.Unlock()
	q.count.Add(1) // antes de notEmpty, senão a ocupação observada sai de [0,K]
	if !v.stop {
		q.produced.Add(1)
	}
	q.notEmpty.release()
}

func (q *semaphoreQueue) get(timeout time.Duration) (item, bool, bool) {
	if timeout == 0 {
		q.notEmpty.acquire()
	} else if !q.notEmpty.acquireTimeout(timeout) {
		return item{}, true, true
	}
	q.mutexOut.Lock()
	v := q.buffer[q.out]
	q.buffer[q.out] = item{}
	q.out = (q.out + 1) % len(q.buffer)
	q.mutexOut.Unlock()
	q.count.Add(-1)
	if !v.stop {
		q.consumed.Add(1)
	}
	q.notFull.release()
	return v, !v.stop, false
}

func (q *semaphoreQueue) occupancy() int { return int(q.count.Load()) }

func (q *semaphoreQueue) finish(consumers int) {
	// FIFO: nenhum item de trabalho fica atrás das sentinelas.
	for i := 0; i < consumers; i++ {
		q.put(item{stop: true})
	}
}

func (q *semaphoreQueue) integrity() (produced, consumed int64, ok bool) {
	produced, consumed = q.produced.Load(), q.consumed.Load()
	return produced, consumed, produced == consumed && q.count.Load() == 0
}

type integrityReporter interface {
	integrity() (produced, consumed int64, ok bool)
}

type atomicCheck struct {
	Produced int64 `json:"produced"`
	Consumed int64 `json:"consumed"`
	OK       bool  `json:"ok"`
}
