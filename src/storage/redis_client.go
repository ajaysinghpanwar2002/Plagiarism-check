package storage

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"plagiarism-detector/src/simhash"

	"github.com/cactus/go-statsd-client/v5/statsd"
	"github.com/redis/go-redis/v9"
	"plagiarism-detector/src/monitoring"
)

const (
	numBands                   = 8
	bandBitSize                = 16
	bandMask                   = uint64(1<<bandBitSize) - 1
	hammingDistanceThreshold   = 3
	checkpointKeyFormat        = "plagiarism_detector:checkpoint:athena_fetch_all:%s:offset"
	checkpointTTL              = 7 * 24 * time.Hour
	defaultSimhashBatchSize    = 500
	defaultSimhashBatchTimeout = 5 * time.Second
	simhashChannelCapacity     = 1000
)

type SimhashData struct {
	PratilipiID string
	Language    string
	Hash        simhash.Simhash
}

type RedisClient struct {
	client           *redis.Client
	statsdClient     statsd.Statter
	simhashBatchChan chan SimhashData
	storerWg         sync.WaitGroup
	storerCancelFunc context.CancelFunc
	batchSize        int
	batchTimeout     time.Duration
}

func NewRedisClient(parentCtx context.Context, addr, password string, db int, statsdClient statsd.Statter) (*RedisClient, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	if _, err := rdb.Ping(parentCtx).Result(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	storerCtx, storerCancel := context.WithCancel(parentCtx)

	rc := &RedisClient{
		client:           rdb,
		statsdClient:     statsdClient,
		simhashBatchChan: make(chan SimhashData, simhashChannelCapacity),
		storerCancelFunc: storerCancel,
		batchSize:        defaultSimhashBatchSize,
		batchTimeout:     defaultSimhashBatchTimeout,
	}

	rc.storerWg.Add(1)
	go rc.runSimhashStorer(storerCtx)

	return rc, nil
}

func (rc *RedisClient) runSimhashStorer(ctx context.Context) {
	defer rc.storerWg.Done()
	batch := make([]SimhashData, 0, rc.batchSize)

	for {
		select {
		case <-ctx.Done():
			log.Println("Simhash storer: context cancelled, flushing remaining items and exiting.")
			if len(batch) > 0 {
				flushCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				rc.flushSimhashBatch(flushCtx, batch)
				cancel()
			}
			return
		case data, ok := <-rc.simhashBatchChan:
			if !ok {
				log.Println("Simhash storer: channel closed, flushing remaining items and exiting.")
				if len(batch) > 0 {
					flushCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					rc.flushSimhashBatch(flushCtx, batch)
					cancel()
				}
				return
			}
			batch = append(batch, data)
			if len(batch) >= rc.batchSize {
				log.Printf("Simhash storer: batch size reached (%d), flushing.", len(batch))
				flushCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
				rc.flushSimhashBatch(flushCtx, batch)
				cancel()
				batch = make([]SimhashData, 0, rc.batchSize)
			}
		}
	}
}

func (rc *RedisClient) flushSimhashBatch(ctx context.Context, batch []SimhashData) error {
	if len(batch) == 0 {
		return nil
	}
	pipe := rc.client.Pipeline()
	processedIDs := make([]string, 0, len(batch))

	for _, data := range batch {
		fullHashKey := fmt.Sprintf("simhashes:%s", strings.ToUpper(data.Language))
		pipe.HSet(ctx, fullHashKey, data.PratilipiID, data.Hash.String())

		for i := 0; i < numBands; i++ {
			var bandValue uint64
			if i < numBands/2 { // First 4 bands from Low
				shift := uint(i * bandBitSize)
				bandValue = (data.Hash.Low >> shift) & bandMask
			} else { // Next 4 bands from High
				shift := uint((i - numBands/2) * bandBitSize)
				bandValue = (data.Hash.High >> shift) & bandMask
			}
			bucketKey := fmt.Sprintf("%s:%d:%x", strings.ToUpper(data.Language), i, bandValue)
			pipe.SAdd(ctx, bucketKey, data.PratilipiID)
		}
		processedIDs = append(processedIDs, data.PratilipiID)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		log.Printf("ERROR: Failed to execute Redis pipeline for storing simhash batch (count: %d): %v. IDs: %v", len(batch), err, processedIDs)
		monitoring.Increment("failed-store-simhash-batch", rc.statsdClient)
		return fmt.Errorf("failed to execute Redis pipeline for storing simhash batch: %w", err)
	}

	log.Printf("Successfully stored batch of %d SimHashes in Redis. First ID: %s", len(batch), batch[0].PratilipiID)
	monitoring.Increment("stored-simhash-batch", rc.statsdClient)
	return nil
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
			_, err = rc.client.SAdd(ctx, fmt.Sprintf("potential_plagiarism:%s", strings.ToUpper(language)), pratilipiID, candidateID).Result()
			return true, nil // Plagiarism detected
		}
	}

	dataToStore := SimhashData{PratilipiID: pratilipiID, Language: language, Hash: newHash}
	select {
	case rc.simhashBatchChan <- dataToStore:
	case <-ctx.Done():
		log.Printf("ERROR: Context cancelled before queuing SimHash for Pratilipi ID %s (lang: %s): %v", pratilipiID, language, ctx.Err())
		return false, fmt.Errorf("context cancelled before queuing simhash for ID %s: %w", pratilipiID, ctx.Err())
	}
	return false, nil
}

func (rc *RedisClient) Close() error {
	log.Println("Closing RedisClient: signalling simhash storer to stop...")
	if rc.storerCancelFunc != nil {
		rc.storerCancelFunc()
	}

	rc.storerWg.Wait()
	log.Println("Simhash storer finished.")

	log.Println("Closing Redis connection.")
	return rc.client.Close()
}

func (rc *RedisClient) GetCheckpointOffset(ctx context.Context, language string) (int64, error) {
	key := fmt.Sprintf(checkpointKeyFormat, strings.ToUpper(language))
	val, err := rc.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to get checkpoint offset for language %s from Redis: %w", language, err)
	}
	offset, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		log.Printf("WARN: Failed to parse checkpoint offset '%s' for language %s: %v. Defaulting to 0.", val, language, err)
		return 0, nil
	}
	return offset, nil
}

func (rc *RedisClient) SetCheckpointOffset(ctx context.Context, language string, offset int64) error {
	key := fmt.Sprintf(checkpointKeyFormat, strings.ToUpper(language))
	err := rc.client.Set(ctx, key, offset, checkpointTTL).Err()
	if err != nil {
		return fmt.Errorf("failed to set checkpoint offset for language %s to %d in Redis: %w", language, offset, err)
	}
	return nil
}

func (rc *RedisClient) DeleteCheckpointOffset(ctx context.Context, language string) error {
	key := fmt.Sprintf(checkpointKeyFormat, strings.ToUpper(language))
	err := rc.client.Del(ctx, key).Err()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("failed to delete checkpoint offset for language %s from Redis: %w", language, err)
	}
	log.Printf("Deleted checkpoint for language %s (if existed)", strings.ToUpper(language))
	return nil
}
