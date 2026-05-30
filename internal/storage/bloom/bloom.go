package bloom

import (
	"math"
)

// Filter is a space-efficient probabilistic data structure for membership tests.
// False positives are possible; false negatives are not.
type Filter struct {
	bits []uint64
	k    uint // number of hash functions
	m    uint // bit array size
}

// New creates a filter sized for n expected items at false-positive rate p.
func New(n uint, p float64) *Filter {
	m := optimalM(n, p)
	k := optimalK(m, n)
	return &Filter{
		bits: make([]uint64, (m+63)/64),
		k:    k,
		m:    m,
	}
}

func optimalM(n uint, p float64) uint {
	return uint(math.Ceil(-float64(n) * math.Log(p) / (math.Log(2) * math.Log(2))))
}

func optimalK(m, n uint) uint {
	k := uint(math.Round(float64(m) / float64(n) * math.Log(2)))
	if k < 1 {
		return 1
	}
	return k
}

// Add inserts key into the filter.
func (f *Filter) Add(key []byte) {
	h1, h2 := hash(key)
	for i := uint(0); i < f.k; i++ {
		pos := (h1 + uint64(i)*h2) % uint64(f.m)
		f.bits[pos/64] |= 1 << (pos % 64)
	}
}

// Contains reports whether key may be in the set.
// A false return guarantees absence; a true return is probabilistic.
func (f *Filter) Contains(key []byte) bool {
	h1, h2 := hash(key)
	for i := uint(0); i < f.k; i++ {
		pos := (h1 + uint64(i)*h2) % uint64(f.m)
		if f.bits[pos/64]&(1<<(pos%64)) == 0 {
			return false
		}
	}
	return true
}

// double hashing: two independent 64-bit hashes derived from FNV-1a.
func hash(data []byte) (uint64, uint64) {
	const (
		offset64 uint64 = 14695981039346656037
		prime64  uint64 = 1099511628211
	)
	h := offset64
	for _, b := range data {
		h ^= uint64(b)
		h *= prime64
	}
	h2 := h ^ (h >> 33)
	h2 *= 0xff51afd7ed558ccd
	h2 ^= h2 >> 33
	return h, h2
}
