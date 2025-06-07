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

