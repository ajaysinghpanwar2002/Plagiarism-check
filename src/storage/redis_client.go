package storage

import (
	"context"
	"fmt"
	"strings"

	"plagiarism-detector/src/simhash"

	"github.com/redis/go-redis/v9"
)

const (
	numBands    = 8
	bandBitSize = 16 // 128 bits / 8 bands = 16 bits per band
	bandMask    = uint64(1<<bandBitSize) - 1
)

// RedisClient handles interactions with Redis for storing and retrieving SimHashes.
type RedisClient struct {
	client *redis.Client
}

// NewRedisClient creates a new Redis client instance.
func NewRedisClient(ctx context.Context, addr, password string, db int) (*RedisClient, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	if _, err := rdb.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisClient{client: rdb}, nil
}

// StoreSimhash stores the given SimHash in Redis using LSH bucketing.
// It stores the pratilipiID in multiple sets (one for each band) and the full hash in a Redis Hash.
func (rc *RedisClient) StoreSimhash(ctx context.Context, pratilipiID, language string, hash simhash.Simhash) error {
	pipe := rc.client.Pipeline()

	// Store full hash: HSET simhashes:LANGUAGE pratilipiID full_hash_hex
	fullHashKey := fmt.Sprintf("simhashes:%s", strings.ToUpper(language))
	pipe.HSet(ctx, fullHashKey, pratilipiID, hash.String())

	// LSH Bucketing
	// Bands 0-3 from Low uint64, Bands 4-7 from High uint64
	for i := 0; i < numBands; i++ {
		var bandValue uint64
		if i < numBands/2 { // First 4 bands from Low
			shift := uint(i * bandBitSize)
			bandValue = (hash.Low >> shift) & bandMask
		} else { // Next 4 bands from High
			shift := uint((i - numBands/2) * bandBitSize)
			bandValue = (hash.High >> shift) & bandMask
		}

		// Bucket key: LANGUAGE:BAND_INDEX:BAND_VALUE_HEX
		bucketKey := fmt.Sprintf("%s:%d:%x", strings.ToUpper(language), i, bandValue)
		pipe.SAdd(ctx, bucketKey, pratilipiID)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute Redis pipeline for storing simhash for ID %s: %w", pratilipiID, err)
	}
	return nil
}

// Close closes the Redis client connection.
func (rc *RedisClient) Close() error {
	return rc.client.Close()
}
