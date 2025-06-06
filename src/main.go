package main

import (
	"context"
	"fmt"
	"log"
	"sync"

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
		ID       string
		Language string
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

				err = redisClient.StoreSimhash(ctx, task.ID, task.Language, hash)
				if err != nil {
					log.Printf("Worker %d: ERROR storing SimHash for Pratilipi ID %s (lang: %s): %v", workerID, task.ID, task.Language, err)
				} else {
					log.Printf("Worker %d: Successfully stored SimHash for Pratilipi ID %s (lang: %s) in Redis", workerID, task.ID, task.Language)
				}
			}
		}(i)
	}

	go func() {
		defer close(pratilipiTaskChannel)
		for _, language := range config.Languages {
			log.Printf("Producer: Fetching Pratilipi IDs for %s", language)
			ids, err := processor.FetchPublishedPratilipiIDsForYesterday(ctx, language)
			if err != nil {
				log.Printf("ERROR: Could not fetch IDs for %s: %v", language, err)
				continue
			}

			if len(ids) == 0 {
				log.Printf("Producer: No new Pratilipi IDs found for %s", language)
				continue
			}

			log.Printf("Producer: Found %d IDs for %s. Sending to workers.", len(ids), language)
			for _, id := range ids {
				pratilipiTaskChannel <- PratilipiTask{ID: id, Language: language} // Send struct
			}
		}
	}()

	wg.Wait()
	fmt.Println("All Pratilipi IDs have been processed. Application finished.")
}
