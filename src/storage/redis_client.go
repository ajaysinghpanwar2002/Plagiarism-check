package storage

import (
	"context"
	"fmt"
	"log"
	"sort"
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
	defaultSimhashBatchSize    = 250
	defaultSimhashBatchTimeout = 5 * time.Second
	simhashChannelCapacity     = 500
	processingDateKeyFormat    = "athena_processing_date:%s"
	redisDateFormat            = "2006-01-02" // stores dates in YYYY-MM-DD format
	maxCandidatesToCheck       = 25000
	sscanChunkSize             = 500
)

type SimhashData struct {
	PratilipiID string
	Language    string
	Hash        simhash.Simhash
}

type bucketInfo struct {
	key  string
	size int64
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
				flushCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

	if len(batch) > 1 {
		// All items in a batch are expected to have the same language.
		// If batch is not empty, batch[0].Language can be used.
		// Ensure batch is not empty before accessing batch[0] if this check is moved earlier.
		batchLanguage := strings.ToUpper(batch[0].Language)
		bandMap := make(map[string][]SimhashData) // Maps band identifier to list of items in that band

		// 1. Populate bandMap: Group items by their bands
		for _, item := range batch {
			for bandIdx := 0; bandIdx < numBands; bandIdx++ {
				var bandValue uint64
				if bandIdx < numBands/2 { // First numBands/2 bands from Low part of hash
					shift := uint(bandIdx * bandBitSize)
					bandValue = (item.Hash.Low >> shift) & bandMask
				} else { // Remaining bands from High part of hash
					shift := uint((bandIdx - numBands/2) * bandBitSize)
					bandValue = (item.Hash.High >> shift) & bandMask
				}
				// Create a unique key for each band bucket (language:band_index:band_value_hex)
				bucketKey := fmt.Sprintf("%s:%d:%x", batchLanguage, bandIdx, bandValue)
				bandMap[bucketKey] = append(bandMap[bucketKey], item)
			}
		}

		// 2. Compare items within the same LSH buckets
		comparedPairs := make(map[string]struct{}) // To avoid re-comparing/re-logging identical pairs

		for _, bucketItems := range bandMap {
			if len(bucketItems) < 2 { // Need at least two items in a bucket to find a pair
				continue
			}
			for i, item1 := range bucketItems {
				for j := i + 1; j < len(bucketItems); j++ {
					item2 := bucketItems[j]
					if item1.PratilipiID == item2.PratilipiID {
						continue
					}

					// Create a canonical key for the pair to ensure a pair is processed only once
					var pairKey string
					if item1.PratilipiID < item2.PratilipiID {
						pairKey = item1.PratilipiID + ":" + item2.PratilipiID
					} else {
						// item1.PratilipiID should not be equal to item2.PratilipiID if items are distinct
						pairKey = item2.PratilipiID + ":" + item1.PratilipiID
					}

					if _, exists := comparedPairs[pairKey]; exists {
						continue // This pair has already been compared (possibly via another shared band)
					}

					distance := simhash.HammingDistance(item1.Hash, item2.Hash)
					if distance <= hammingDistanceThreshold {
						log.Printf("IN-BATCH Plagiarism DETECTED for Pratilipi ID %s and %s (lang: %s). Hamming Distance: %d",
							item1.PratilipiID, item2.PratilipiID, item1.Language, distance)
						monitoring.Increment("potential-plagiarism-detected", rc.statsdClient)

						plagiarismRedisKey := fmt.Sprintf("potential_plagiarism:%s:%s", batchLanguage, item1.PratilipiID)
						pipe.SAdd(ctx, plagiarismRedisKey, item2.PratilipiID)

						plagiarismRedisKeyReverse := fmt.Sprintf("potential_plagiarism:%s:%s", batchLanguage, item2.PratilipiID)
						pipe.SAdd(ctx, plagiarismRedisKeyReverse, item1.PratilipiID)
					}
					comparedPairs[pairKey] = struct{}{}
				}
			}
		}
	}

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

func (rc *RedisClient) CheckPotentialSimhashMatches(ctx context.Context, pratilipiID, language string, newHash simhash.Simhash) ([]string, error) {
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

	allCandidateIDs := make(map[string]struct{})
	if len(bucketKeys) == 0 {
		log.Printf("WARN: No bucket keys generated for language %s. Skipping potential match check for Pratilipi ID %s.", language, pratilipiID)
		return nil, nil
	}

	var bucketInfos []bucketInfo

	for _, bk := range bucketKeys {
		sz, err := rc.client.SCard(ctx, bk).Result()
		if err != nil {
			log.Printf("WARN: Failed to get size for bucket %s: %v", bk, err)
			continue
		}
		bucketInfos = append(bucketInfos, bucketInfo{key: bk, size: sz})
	}

	sort.Slice(bucketInfos, func(i, j int) bool {
		return bucketInfos[i].size < bucketInfos[j].size
	})

	totalKeys := 0 
	for _, binfo := range bucketInfos {
		totalKeys += int(binfo.size)
	}

	// only process the smallest 4 buckets.
	// As Our hamming distance threshold is 3, we can safely ignore larger buckets.
	// This is a heuristic to reduce the number of candidates we check.
	if len(bucketInfos) > 4 && totalKeys > maxCandidatesToCheck {
		bucketInfos = bucketInfos[:4]
	}

	for _, binfo := range bucketInfos {
		if len(allCandidateIDs) >= maxCandidatesToCheck {
			break
		}
		var cursor uint64
		for {
			members, newCursor, err := rc.client.SScan(ctx, binfo.key, cursor, "", sscanChunkSize).Result()
			if err != nil {
				log.Printf("WARN: Failed to scan members for bucket %s: %v", binfo.key, err)
				break
			}
			for _, id := range members {
				allCandidateIDs[id] = struct{}{}
				if len(allCandidateIDs) >= maxCandidatesToCheck {
					break
				}
			}
			if newCursor == 0 || len(allCandidateIDs) >= maxCandidatesToCheck {
				break
			}
			cursor = newCursor
		}
	}

	fullHashKey := fmt.Sprintf("simhashes:%s", strings.ToUpper(language))
	potentialMatchIDs := []string{}

	for candidateID := range allCandidateIDs {
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
			log.Printf("Potential Simhash match for Pratilipi ID %s (lang: %s) with %s. Hamming Distance: %d.",
				pratilipiID, language, candidateID, distance)
			monitoring.IncrementWithTags("simhash-potential-match-found-language", rc.statsdClient, language)

			lang := strings.ToUpper(language)
			plagiarismRedisKey := fmt.Sprintf("potential_plagiarism:%s:%s", lang, pratilipiID)
			rc.client.SAdd(ctx, plagiarismRedisKey, candidateID)

			potentialMatchIDs = append(potentialMatchIDs, candidateID)
		}
	}

	// if len(potentialMatchIDs) > 0 {
	// 	return potentialMatchIDs, nil
	// }

	dataToStore := SimhashData{PratilipiID: pratilipiID, Language: language, Hash: newHash}
	select {
	case rc.simhashBatchChan <- dataToStore:
	case <-ctx.Done():
		log.Printf("ERROR: Context cancelled before queuing SimHash for Pratilipi ID %s (lang: %s): %v", pratilipiID, language, ctx.Err())
		return nil, fmt.Errorf("context cancelled before queuing simhash for ID %s: %w", pratilipiID, ctx.Err())
	}
	return potentialMatchIDs, nil
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

func (rc *RedisClient) StoreConfirmedPlagiarism(ctx context.Context, language, originalPratilipiID, plagiarizedPratilipiID string) error {
	processingDate, exists, err := rc.GetProcessingDate(ctx, language)
	if err != nil {
		log.Printf("ERROR: Failed to get processing date for language %s: %v. Using previous day as default.", language, err)
		processingDate = time.Now().AddDate(0, 0, -1)
	} else if !exists {
		processingDate = time.Now().AddDate(0, 0, -1)
	}
	dateStr := processingDate.In(time.UTC).Format(redisDateFormat)
	confirmedPlagiarismKey := fmt.Sprintf("confirmed_plagiarism:%s:%s", strings.ToUpper(language), dateStr)
	_, err = rc.client.HSet(ctx, confirmedPlagiarismKey, originalPratilipiID, plagiarizedPratilipiID).Result()
	if err != nil {
		log.Printf("ERROR: Failed to store confirmed plagiarism for %s against %s: %v", originalPratilipiID, plagiarizedPratilipiID, err)
		monitoring.Increment("failed-store-confirmed-plagiarism", rc.statsdClient)
		return fmt.Errorf("failed to store confirmed plagiarism for %s to %s: %w", originalPratilipiID, plagiarizedPratilipiID, err)
	}
	log.Printf("Stored confirmed plagiarism: %s plagiarized by %s (lang: %s)", originalPratilipiID, plagiarizedPratilipiID, language)
	monitoring.Increment("stored-confirmed-plagiarism", rc.statsdClient)
	return nil
}

func (rc *RedisClient) GetProcessingDate(ctx context.Context, language string) (time.Time, bool, error) {
	key := fmt.Sprintf(processingDateKeyFormat, language)
	dateStr, err := rc.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("redis GetProcessingDate for %s: %w", language, err)
	}

	// Dates are stored and read as UTC.
	parsedDate, err := time.ParseInLocation(redisDateFormat, dateStr, time.UTC)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("redis ParseProcessingDate '%s' for %s: %w", dateStr, language, err)
	}
	return parsedDate, true, nil
}

func (rc *RedisClient) SetProcessingDate(ctx context.Context, language string, dateToSet time.Time) error {
	key := fmt.Sprintf(processingDateKeyFormat, language)
	dateStr := dateToSet.In(time.UTC).Format(redisDateFormat)

	err := rc.client.Set(ctx, key, dateStr, 0).Err()
	if err != nil {
		return fmt.Errorf("redis SetProcessingDate for %s to %s: %w", language, dateStr, err)
	}
	return nil
}
