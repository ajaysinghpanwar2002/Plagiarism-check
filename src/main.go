package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"plagiarism-detector/src/config"
	"plagiarism-detector/src/simhash"
	"plagiarism-detector/src/sources"
	"plagiarism-detector/src/storage"
)

func main() {
	config, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	ctx := context.Background()

	processor, err := sources.NewAthenaProcessor(
		ctx,
		config.AWSRegion,
		config.AthenaResultsBucket,
		config.AthenaOutputPrefix,
		config.AthenaDatabase,
	)
	if err != nil {
		log.Fatalf("Failed to create Athena processor: %v", err)
	}

	s3Downloader, err := sources.NewS3Downloader(ctx, config.AWSRegion, config.StoryS3Bucket)
	if err != nil {
		log.Fatalf("Failed to create S3 downloader: %v", err)
	}

	redisClient, err := storage.NewRedisClient(ctx, config.RedisAddr, config.RedisPassword, config.RedisDB)
	if err != nil {
		log.Fatalf("Failed to create Redis client: %v", err)
	}
	defer redisClient.Close()

	type PratilipiTask struct {
		ID            string
		Language      string
		CheckpointKey string
	}
	pratilipiTaskChannel := make(chan PratilipiTask, 100)
	var wg sync.WaitGroup

	for i := 0; i < config.NumWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for task := range pratilipiTaskChannel {
				log.Printf("Worker %d: Processing Pratilipi ID %s for language %s", workerID, task.ID, task.Language)
				content, err := s3Downloader.DownloadStoryContent(ctx, task.ID)
				if err != nil {
					log.Printf("Worker %d: ERROR downloading and parsing content for Pratilipi ID %s: %v", workerID, task.ID, err)
					continue
				}

				if content == "" {
					log.Printf("Worker %d: No content found for Pratilipi ID %s (or all chapters failed to download/parse)", workerID, task.ID)
					continue
				}

				hash := simhash.New(content)

				plagiarismDetected, err := redisClient.CheckAndStoreSimhash(ctx, task.ID, task.Language, hash)
				if err != nil {
					log.Printf("Worker %d: ERROR processing SimHash for Pratilipi ID %s (lang: %s): %v. Checkpoint NOT updated.", workerID, task.ID, task.Language, err)
				} else {
					if plagiarismDetected {
						log.Printf("Worker %d: Potential PLAGIARISM DETECTED for Pratilipi ID %s (lang: %s). Not stored.", workerID, task.ID, task.Language)
					}
					err = redisClient.SetLastProcessedID(ctx, task.CheckpointKey, task.ID)
					if err != nil {
						log.Printf("Worker %d: ERROR updating checkpoint for Pratilipi ID %s (key: %s): %v", workerID, task.ID, task.CheckpointKey, err)
					} else {
						log.Printf("Worker %d: Successfully processed and checkpointed Pratilipi ID %s (key: %s)", workerID, task.ID, task.CheckpointKey)
					}
				}
			}
		}(i)
	}

	go func() {
		defer close(pratilipiTaskChannel)

		now := time.Now()
		yesterday := now.AddDate(0, 0, -1)
		dateKeyPart := yesterday.Format("2006-01-02")

		for _, language := range config.Languages {
			checkpointKey := fmt.Sprintf("checkpoint:%s:%s", language, dateKeyPart)
			lastID, err := redisClient.GetLastProcessedID(ctx, checkpointKey)
			if err != nil {
				log.Printf("ERROR: Could not fetch last processed ID for %s (key: %s): %v. Skipping language.", language, checkpointKey, err)
				lastID = ""
				continue
			}
			log.Printf("Producer: Fetching Pratilipi IDs for %s", language)
			ids, err := processor.FetchPublishedPratilipiIDsForYesterday(ctx, language, lastID)
			if err != nil {
				log.Printf("ERROR: Could not fetch IDs for %s: %v", language, err)
				continue
			}

			if len(ids) == 0 {
				log.Printf("Producer: No new Pratilipi IDs found for %s (after ID %s)", language, lastID)
				continue
			}

			log.Printf("Producer: Found %d IDs for %s. Sending to workers.", len(ids), language)
			for _, id := range ids {
				pratilipiTaskChannel <- PratilipiTask{ID: id, Language: language, CheckpointKey: checkpointKey}
			}
		}
	}()

	wg.Wait()
	fmt.Println("All Pratilipi IDs have been processed. Application finished.")
}
