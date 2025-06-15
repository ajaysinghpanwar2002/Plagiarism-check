package moss

import (
	"hash/fnv"
	"regexp"
	"strings"
	"unicode"

	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

func normalizeText(text string) string {
	t := transform.Chain(norm.NFKD, transform.RemoveFunc(func(r rune) bool {
		return unicode.Is(unicode.Mn, r)
	}))
	normalizedText, _, _ := transform.String(t, text)

	var sb strings.Builder
	for _, r := range normalizedText {
		if unicode.IsLetter(r) || unicode.IsNumber(r) || unicode.IsSpace(r) {
			sb.WriteRune(unicode.ToLower(r))
		} else {
			sb.WriteRune(' ')
		}
	}
	spaceRe := regexp.MustCompile(`\s+`)
	return strings.TrimSpace(spaceRe.ReplaceAllString(sb.String(), " "))
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

	if len(hashes) <= windowSize {
		if len(hashes) == 0 {
			return []uint32{}
		}
		minHash := hashes[0]
		for _, h := range hashes {
			if h < minHash {
				minHash = h
			}
		}
		return []uint32{minHash}
	}

	fingerprintMap := make(map[uint32]struct{})
	for i := 0; i <= len(hashes)-windowSize; i++ {
		window := hashes[i : i+windowSize]
		minHashInWindow := window[0]
		// We want the rightmost minimal hash to break ties, as per some MOSS implementations.
		// However, for simplicity here, any minimal hash in the window is fine.
		// A more robust winnowing might select the rightmost occurrence of the minimum.
		for j := 1; j < len(window); j++ {
			if window[j] <= minHashInWindow {
				minHashInWindow = window[j]
			}
		}
		fingerprintMap[minHashInWindow] = struct{}{}
	}
	
	fingerprint := make([]uint32, 0, len(fingerprintMap))
	for h := range fingerprintMap {
		fingerprint = append(fingerprint, h)
	}
	return fingerprint
}

// CalculateSimilarity computes the Jaccard index between two fingerprints.
// Similarity = |Intersection(fp1, fp2)| / |Union(fp1, fp2)|
func CalculateSimilarity(fp1, fp2 []uint32) float64 {
	if len(fp1) == 0 || len(fp2) == 0 {
		return 0.0
	}

	set1 := make(map[uint32]struct{}, len(fp1))
	for _, h := range fp1 {
		set1[h] = struct{}{}
	}

	intersectionSize := 0
	for _, h := range fp2 {
		if _, found := set1[h]; found {
			intersectionSize++
		}
	}

	unionSize := len(fp1) + len(fp2) - intersectionSize
	if unionSize == 0 {
		return 0.0
	}

	return float64(intersectionSize) / float64(unionSize)
}
