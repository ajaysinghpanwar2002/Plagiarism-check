package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"plagiarism-detector/src/config"
	"plagiarism-detector/src/monitoring"
	"plagiarism-detector/src/simhash"
	"plagiarism-detector/src/sources"
	"plagiarism-detector/src/storage"
)

func main() {
	config, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	StatsDClient, err := monitoring.ConnectStatsd(config.StatsDHost, config.StatsDPort, config.StatsDPrefix)
	if err != nil {
		log.Println("Unable to connect to grafana", err)
	}
	defer StatsDClient.Close()

	s3Downloader, err := sources.NewS3Downloader(ctx, config.AWSRegion, config.StoryS3Bucket, StatsDClient)
	if err != nil {
		log.Fatalf("Failed to create S3 downloader: %v", err)
	}

	redisClient, err := storage.NewRedisClient(ctx, config.RedisAddr, config.RedisPassword, config.RedisDB, StatsDClient)
	if err != nil {
		log.Fatalf("Failed to create Redis client: %v", err)
	}
	defer redisClient.Close()

	pratilipiTaskChannel := make(chan sources.PratilipiData, config.WorkerChannelSize)

	processor, err := sources.NewAthenaProcessor(
		ctx,
		config.AWSRegion,
		config.AthenaResultsBucket,
		config.AthenaOutputPrefix,
		config.AthenaDatabase,
		config.AthenaFetchStartDate,
		StatsDClient,
		redisClient,
	)
	if err != nil {
		log.Fatalf("Failed to create Athena processor: %v", err)
	}

	var wg sync.WaitGroup

	for i := 0; i < config.NumWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for task := range pratilipiTaskChannel {
				content, err := s3Downloader.DownloadStoryContent(ctx, task.ID)
				if err != nil {
					log.Printf("Worker %d: ERROR downloading and parsing content for Pratilipi ID %s: %v", workerID, task.ID, err)
					continue
				}

				if content == "" {
					log.Printf("Worker %d: No content found for Pratilipi ID %s (or all chapters failed to download/parse)", workerID, task.ID)
					monitoring.Increment("no-content-found-text-empty", StatsDClient)
					continue
				}

				hash := simhash.New(content)

				plagiarismDetected, err := redisClient.CheckAndStoreSimhash(ctx, task.ID, task.Language, hash)
				if err != nil {
					log.Printf("Worker %d: ERROR processing SimHash for Pratilipi ID %s (lang: %s): %v", workerID, task.ID, task.Language, err)
					monitoring.Increment("failed-process-simhash", StatsDClient)
				} else if plagiarismDetected {
					monitoring.Increment("potential-plagiarism-detected", StatsDClient)
				}
			}
		}(i)
	}

	go func() {
		defer close(pratilipiTaskChannel)
		for _, language := range config.Languages {
			log.Printf("Producer: Starting to fetch Pratilipi IDs for %s (with checkpointing)", language)
			err := processor.FetchPublishedPratilipiIDs(ctx, language, pratilipiTaskChannel)
			if err != nil {
				if err == context.Canceled || err == context.DeadlineExceeded {
					log.Printf("Producer: Fetching for %s was cancelled or timed out: %v. Stopping producer.", language, err)
					monitoring.Increment("fetch-cancelled-or-timed-out", StatsDClient)
					return
				}
				log.Printf("ERROR: Could not complete fetching IDs for %s: %v. Continuing with next language.", language, err)
				select {
				case <-ctx.Done():
					log.Printf("Producer: Context is done, stopping further language processing.")
					return
				default:
				}
			} else {
				log.Printf("Producer: Finished fetching IDs for %s.", language)
			}
		}
		log.Printf("Producer: All languages processed for ID fetching.")
	}()

	wg.Wait()
	fmt.Println("All Pratilipi IDs have been processed. Application finished.")
}
