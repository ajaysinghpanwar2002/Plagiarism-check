package simhash

import (
	"fmt"
	"math/bits"
	"strings"

	"github.com/cespare/xxhash/v2"
)

type Simhash struct {
	Low  uint64
	High uint64
}

func New(processedText string) Simhash {
	tokens := strings.Split(processedText, " ")
	if len(tokens) == 0 {
		return Simhash{Low: 0, High: 0}
	}

	weights := make(map[string]int, len(tokens))
	for _, tok := range tokens {
		if tok != "" {
			weights[tok]++
		}
	}

	var vec [128]int

	for tok, w := range weights {
		// NEW: Use xxhash.Sum64 for a much faster, non-cryptographic hash.
		// We will generate two 64-bit hashes with different seeds to create a 128-bit fingerprint.
		// This is a common and effective technique.
		// Seed 0 for the lower 64 bits, Seed 1 for the higher 64 bits.
		lowHash := xxhash.Sum64String(tok)
		highHash := xxhash.Sum64String(tok + "1") // Add a salt for the second hash

		// Process lower 64 bits
		for i := 0; i < 64; i++ {
			if (lowHash>>i)&1 == 1 {
				vec[i] += w
			} else {
				vec[i] -= w
			}
		}

		// Process higher 64 bits
		for i := 0; i < 64; i++ {
			if (highHash>>i)&1 == 1 {
				vec[i+64] += w
			} else {
				vec[i+64] -= w
			}
		}
	}

	var low, high uint64
	for i, v := range vec {
		if v > 0 {
			if i < 64 {
				low |= 1 << uint(i)
			} else {
				high |= 1 << uint(i-64)
			}
		}
	}

	return Simhash{Low: low, High: high}
}

func (s Simhash) String() string {
	return fmt.Sprintf("%016x%016x", s.High, s.Low)
}

func HammingDistance(s1, s2 Simhash) int {
	return bits.OnesCount64(s1.Low^s2.Low) + bits.OnesCount64(s1.High^s2.High)
}

func ParseSimhashFromString(s string) (Simhash, error) {
	var sh Simhash
	if len(s) != 32 {
		return sh, fmt.Errorf("invalid simhash string length: expected 32, got %d", len(s))
	}
	_, err := fmt.Sscanf(s, "%016x%016x", &sh.High, &sh.Low)
	if err != nil {
		return sh, fmt.Errorf("failed to parse simhash string: %w", err)
	}
	return sh, nil
}
