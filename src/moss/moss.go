package moss

import (
	"hash/fnv"
	"strings"
	"unicode"

	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// normalizeText efficiently cleans and normalizes the input string.
// It performs Unicode normalization (NFKD)
// in a single pass over the string for better performance.
func normalizeText(text string) string {
	t := transform.Chain(norm.NFKD)
	normalizedText, _, _ := transform.String(t, text)

	var sb strings.Builder
	sb.Grow(len(normalizedText))

	spaceAdded := false
	for _, r := range normalizedText {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			sb.WriteRune(unicode.ToLower(r)) // ToLower is safe; it has no effect on caseless scripts.
			spaceAdded = false
		} else {
			if !spaceAdded {
				sb.WriteRune(' ')
				spaceAdded = true
			}
		}
	}

	return strings.TrimSpace(sb.String())
}

func GenerateKgrams(text string, k int) []string {
	normalized := normalizeText(text)
	words := strings.Fields(normalized)

	if len(words) < k {
		if len(words) > 0 {
			return []string{strings.Join(words, " ")}
		}
		return []string{}
	}

	kgrams := make([]string, 0, len(words)-k+1)
	for i := 0; i <= len(words)-k; i++ {
		kgrams = append(kgrams, strings.Join(words[i:i+k], " "))
	}
	return kgrams
}

func hashKgram(kgram string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(kgram))
	return h.Sum32()
}

// GenerateFingerprint creates a document fingerprint using the winnowing algorithm.
// This optimized version uses a deque (double-ended queue) to find the minimum hash
// in a sliding window in O(n) time, which is a major performance improvement.
func GenerateFingerprint(text string, k, windowSize int) []uint32 {
	if k <= 0 || windowSize <= 0 {
		return []uint32{}
	}

	kgrams := GenerateKgrams(text, k)
	if len(kgrams) == 0 {
		return []uint32{}
	}

	hashes := make([]uint32, len(kgrams))
	for i, kg := range kgrams {
		hashes[i] = hashKgram(kg)
	}

	// If the number of hashes is smaller than the window, the fingerprint
	// is simply the smallest hash.
	if len(hashes) <= windowSize {
		if len(hashes) == 0 {
			return []uint32{}
		}
		minHash := hashes[0]
		for _, h := range hashes[1:] {
			if h < minHash {
				minHash = h
			}
		}
		return []uint32{minHash}
	}

	// --- Optimization using a Deque for Sliding Window Minimum ---
	// The deque stores indices of hashes in the current window.
	// It's kept sorted by hash value, so the front of the deque is always
	// the index of the minimum hash in the window.
	deque := make([]int, 0, windowSize)
	fingerprint := make([]uint32, 0, len(hashes)/windowSize) // Pre-allocate approximate size.

	lastMinHash := uint32(0) // Used to avoid adding duplicate consecutive hashes.

	for i, h := range hashes {
		// Maintain the deque: remove elements from the back that are greater than
		// the current element, as they can no longer be the minimum.
		for len(deque) > 0 && hashes[deque[len(deque)-1]] >= h {
			deque = deque[:len(deque)-1]
		}

		// Add the current hash's index to the deque.
		deque = append(deque, i)

		// Remove elements from the front of the deque that are no longer in the window.
		if deque[0] == i-windowSize {
			deque = deque[1:]
		}

		// The window is full once we've processed `windowSize-1` elements.
		// After this point, we select the minimum hash for each window.
		if i >= windowSize-1 {
			minHash := hashes[deque[0]]
			// To avoid adding the same hash multiple times if it remains the minimum
			// across several windows, we only add it if it's different from the last one added.
			if minHash != lastMinHash {
				fingerprint = append(fingerprint, minHash)
				lastMinHash = minHash
			}
		}
	}

	return fingerprint
}

// CalculateSimilarity computes the Jaccard index between two fingerprints.
// The Jaccard similarity is calculated as |Intersection| / |Union|.
func CalculateSimilarity(fp1, fp2 []uint32) float64 {
	if len(fp1) == 0 || len(fp2) == 0 {
		return 0.0
	}

	// Use a map to create a set of hashes for the first fingerprint for efficient lookups.
	set1 := make(map[uint32]struct{}, len(fp1))
	for _, h := range fp1 {
		set1[h] = struct{}{}
	}

	// Calculate the size of the intersection by checking for common elements.
	intersectionSize := 0
	for _, h := range fp2 {
		if _, found := set1[h]; found {
			intersectionSize++
			// To be strictly correct and not double-count if fp2 has duplicates,
			// you could delete the entry from the map after finding it.
			// delete(set1, h)
		}
	}

	// The union size is calculated using the principle of inclusion-exclusion:
	// |A U B| = |A| + |B| - |A ∩ B|
	unionSize := len(fp1) + len(fp2) - intersectionSize
	if unionSize == 0 {
		return 0.0
	}

	return float64(intersectionSize) / float64(unionSize)
}

