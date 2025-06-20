package simhash

import (
	"crypto/md5"
	"fmt"
	"regexp"
	"strings"
)

type Simhash struct {
	Low  uint64
	High uint64
}

var unicodeWordRegex = regexp.MustCompile(`[\pL\p{Nd}]+`)

func tokenizeUnicode(text string) []string {
	lower := strings.ToLower(text)
	words := unicodeWordRegex.FindAllString(lower, -1)
	return words
}

func New(text string) Simhash {
	tokens := tokenizeUnicode(text)
	if len(tokens) == 0 {
		return Simhash{Low: 0, High: 0}
	}

	weights := make(map[string]int, len(tokens))
	for _, tok := range tokens {
		weights[tok]++
	}

	var vec [128]int

	for tok, w := range weights {
		hash := md5.Sum([]byte(tok))
		for byteIdx, b := range hash {
			for bit := 0; bit < 8; bit++ {
				idx := byteIdx*8 + bit
				if (b>>bit)&1 == 1 {
					vec[idx] += w
				} else {
					vec[idx] -= w
				}
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
	popCount := func(n uint64) int {
		count := 0
		for n > 0 {
			n &= (n - 1) // Brian Kernighan's algorithm to count set bits
			count++
		}
		return count
	}
	return popCount(s1.Low^s2.Low) + popCount(s1.High^s2.High)
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
