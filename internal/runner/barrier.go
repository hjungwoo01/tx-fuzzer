package runner

import (
	"sync"
	"time"
)

// CyclicBarrier coordinates n parties across repeated "phases" (barriers).
// Await returns true if the barrier released due to all parties arriving,
// and false if this caller timed out and forced an early release.
type CyclicBarrier struct {
	n     int
	count int
	phase int

	mu sync.Mutex
	ch chan struct{} // closed to release waiters for current phase
}

func NewCyclicBarrier(n int) *CyclicBarrier {
	if n <= 0 {
		panic("CyclicBarrier: n must be > 0")
	}
	return &CyclicBarrier{
		n:  n,
		ch: make(chan struct{}),
	}
}

// Await blocks until n parties have called it for this phase or until timeout.
// It returns true if the release was triggered by all parties arriving,
// and false if this caller timed out and forced an early release.
// Importantly, a timeout does NOT wedge other parties: it aborts the phase
// by advancing the barrier and releasing everyone waiting.
func (b *CyclicBarrier) Await(timeout time.Duration) bool {
	// Fast path: register arrival and see if we're the last.
	b.mu.Lock()
	myCh := b.ch
	b.count++
	if b.count == b.n {
		// All arrived: release this phase normally.
		close(b.ch)                // release waiters
		b.ch = make(chan struct{}) // next phase
		b.count = 0
		b.phase++
		b.mu.Unlock()
		return true
	}
	b.mu.Unlock()

	// Not last: wait for release or timeout.
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-myCh:
		// Released by last arriver (or someone else's timeout)
		return true
	case <-timer.C:
		// We timed out. Abort this phase if still active,
		// and move the barrier forward so others aren't wedged.
		b.mu.Lock()
		// If phase hasn't been released yet (myCh still current), abort it.
		if myCh == b.ch {
			close(b.ch)
			b.ch = make(chan struct{})
			b.count = 0
			b.phase++
			b.mu.Unlock()
			return false
		}
		// Phase already advanced while we were timing out.
		b.mu.Unlock()
		return true
	}
}
