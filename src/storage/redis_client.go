package storage

import (
	"context"
	"fmt"
	"log"
	"strings"

	"plagiarism-detector/src/simhash"

	"github.com/cactus/go-statsd-client/v5/statsd"
	"github.com/redis/go-redis/v9"
	"plagiarism-detector/src/monitoring"
)

const (
	numBands                 = 8
	bandBitSize              = 16
	bandMask                 = uint64(1<<bandBitSize) - 1
	hammingDistanceThreshold = 3
)

type RedisClient struct {
	client       *redis.Client
	statsdClient statsd.Statter
}

func NewRedisClient(ctx context.Context, addr, password string, db int, statsdClient statsd.Statter) (*RedisClient, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	if _, err := rdb.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisClient{client: rdb, statsdClient: statsdClient}, nil
}

// It performs the actual storage of the simhash and its bands
func (rc *RedisClient) storeSimhashInternal(ctx context.Context, pratilipiID, language string, hash simhash.Simhash) error {
	pipe := rc.client.Pipeline()

	fullHashKey := fmt.Sprintf("simhashes:%s", strings.ToUpper(language))
	pipe.HSet(ctx, fullHashKey, pratilipiID, hash.String())

	for i := 0; i < numBands; i++ {
		var bandValue uint64
		if i < numBands/2 { // First 4 bands from Low
			shift := uint(i * bandBitSize)
			bandValue = (hash.Low >> shift) & bandMask
		} else { // Next 4 bands from High
			shift := uint((i - numBands/2) * bandBitSize)
			bandValue = (hash.High >> shift) & bandMask
		}

		bucketKey := fmt.Sprintf("%s:%d:%x", strings.ToUpper(language), i, bandValue)
		pipe.SAdd(ctx, bucketKey, pratilipiID)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute Redis pipeline for storing simhash for ID %s: %w", pratilipiID, err)
		monitoring.Increment("failed-store-simhash", rc.statsdClient)
	}
	log.Printf("Successfully stored SimHash for Pratilipi ID %s (lang: %s) in Redis", pratilipiID, language)
	monitoring.Increment("stored-simhash", rc.statsdClient)
	return nil
}

func (rc *RedisClient) CheckAndStoreSimhash(ctx context.Context, pratilipiID, language string, newHash simhash.Simhash) (bool, error) {
	bucketKeys := make([]string, numBands)
	if numBands > 0 {
		for i := 0; i < numBands; i++ {
			var bandValue uint64
			if i < numBands/2 {
				shift := uint(i * bandBitSize)
				bandValue = (newHash.Low >> shift) & bandMask
			} else {
				shift := uint((i - numBands/2) * bandBitSize)
				bandValue = (newHash.High >> shift) & bandMask
			}
			bucketKeys[i] = fmt.Sprintf("%s:%d:%x", strings.ToUpper(language), i, bandValue)
		}
	}

	var candidateIDs []string
	var err error
	if len(bucketKeys) > 0 {
		candidateIDs, err = rc.client.SUnion(ctx, bucketKeys...).Result()
		if err != nil {
			return false, fmt.Errorf("failed to get candidate IDs using SUnion for %s: %w", pratilipiID, err)
		}
	}

	fullHashKey := fmt.Sprintf("simhashes:%s", strings.ToUpper(language))

	for _, candidateID := range candidateIDs {
		if candidateID == pratilipiID {
			continue
		}

		candidateHashStr, err := rc.client.HGet(ctx, fullHashKey, candidateID).Result()
		if err == redis.Nil {
			log.Printf("WARN: Candidate ID %s found in LSH bucket but not in full hash map %s. Skipping.", candidateID, fullHashKey)
			monitoring.Increment("candidate-id-not-found-in-full-hash", rc.statsdClient)
			continue
		}
		if err != nil {
			log.Printf("WARN: Failed to get full hash for candidate ID %s from %s: %v. Skipping.", candidateID, fullHashKey, err)
			monitoring.Increment("failed-get-full-hash", rc.statsdClient)
			continue
		}

		candidateHash, err := simhash.ParseSimhashFromString(candidateHashStr)
		if err != nil {
			log.Printf("WARN: Failed to parse Simhash string '%s' for candidate ID %s: %v. Skipping.", candidateHashStr, candidateID, err)
			continue
		}

		distance := simhash.HammingDistance(newHash, candidateHash)

		if distance <= hammingDistanceThreshold {
			log.Printf("Potential plagiarism DETECTED for Pratilipi ID %s (lang: %s). Similar to %s. Hamming Distance: %d",
				pratilipiID, language, candidateID, distance)
			monitoring.Increment("potential-plagiarism-detected", rc.statsdClient)
			return true, nil // Plagiarism detected
		}
	}

	return false, rc.storeSimhashInternal(ctx, pratilipiID, language, newHash)
}

func (rc *RedisClient) Close() error {
	return rc.client.Close()
}
