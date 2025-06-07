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
	bandBitSize = 16
	bandMask    = uint64(1<<bandBitSize) - 1
)

type RedisClient struct {
	client *redis.Client
}

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

func (rc *RedisClient) StoreSimhash(ctx context.Context, pratilipiID, language string, hash simhash.Simhash) error {
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
	}
	return nil
}

func (rc *RedisClient) Close() error {
	return rc.client.Close()
}
