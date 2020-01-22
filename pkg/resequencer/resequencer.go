package resequencer

import "sync"

// Resequencer is a tool to produce ordered numeric sequence based on numbers that come out of
// order
//
// Numbers must be positive integers starting with 1.
type Resequencer struct {
	// Output channel for ordered numbers
	Out chan uint64

	size int

	m         sync.Mutex
	seq       map[uint64]struct{}
	sequenced uint64
	sent      uint64
}

// New creates Resequencer capable of ordering numbers in range [1, size]
func New(size int) *Resequencer {
	return &Resequencer{
		Out:  make(chan uint64, size),
		size: size,
		seq:  make(map[uint64]struct{}, size),
	}
}

// Orders number to be sequenced
func (r *Resequencer) Sequence(n uint64) {
	r.m.Lock()
	defer r.m.Unlock()

	if n == r.sent+1 {
		r.send(n)
	} else {
		r.seq[n] = struct{}{}
		if n > r.sequenced {
			r.sequenced = n
		}
	}
	r.resequence()
}

// Calls f with every remaining number that has not been sent to Out channel yet
func (r *Resequencer) DumpUnsequenced(f func(n uint64)) {
	r.m.Lock()
	defer r.m.Unlock()

	for n := range r.seq {
		f(n)
	}
	r.seq = make(map[uint64]struct{}, r.size)
}

// Start sequence from n
func (r *Resequencer) StartAt(n uint64) {
	r.m.Lock()
	defer r.m.Unlock()

	r.sequenced = n
	r.sent = n
}

// Reset sequence so that it starts from number following to.
// Dumps all ordered numbers up to and including to.
func (r *Resequencer) Reset(to uint64, dump func(n uint64)) {
	r.m.Lock()
	defer r.m.Unlock()

	for n := range r.seq {
		if n <= to {
			dump(n)
			delete(r.seq, n)
		}
	}
	r.sequenced = to
	r.sent = to
}

// Closes output channel
func (r *Resequencer) Close() {
	close(r.Out)
}

func (r *Resequencer) send(n uint64) {
	delete(r.seq, n)
	r.sent++
	r.Out <- n
}

func (r *Resequencer) resequence() {
	for n := r.sent + 1; n <= r.sequenced; n++ {
		if _, ok := r.seq[n]; !ok {
			break
		}
		r.send(n)
	}
}
