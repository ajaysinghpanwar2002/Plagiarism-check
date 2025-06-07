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

	pratilipiIDChannel := make(chan string, 100)
	var wg sync.WaitGroup

	for i := 0; i < config.NumWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for id := range pratilipiIDChannel {
				log.Printf("Worker %d: Processing Pratilipi ID %s", workerID, id)
				content, err := s3Downloader.DownloadStoryContent(ctx, id)
				if err != nil {
					log.Printf("Worker %d: ERROR downloading and parsing content for Pratilipi ID %s: %v", workerID, id, err)
					continue
				}

				if content == "" {
					log.Printf("Worker %d: No content found for Pratilipi ID %s (or all chapters failed to download/parse)", workerID, id)
					continue
				}

				hash := simhash.New(content)
				// The .String() method to get the hex representation
				log.Printf("Worker %d: Generated SimHash for Pratilipi ID %s: %s", workerID, id, hash.String())
				// Store the SimHash in Redis
				// For now, we assume the language is part of the ID or can be derived.
				// Let's use a placeholder "UNKNOWN" for language or extract it if possible.
				// The Athena query fetches by language, so we should pass that along.
				// This part needs refinement on how language is passed to the worker.
				// For now, let's assume we can get it.
				// We will need to modify the channel to send a struct with ID and Language.

				// For now, let's hardcode a language for testing.
				// This will be addressed when we refine the producer-consumer data flow.
				language := "UNKNOWN" // Placeholder - to be fixed
				err = redisClient.StoreSimhash(ctx, id, language, hash)
				if err != nil {
					log.Printf("Worker %d: ERROR storing SimHash for Pratilipi ID %s: %v", workerID, id, err)
				} else {
					log.Printf("Worker %d: Successfully stored SimHash for Pratilipi ID %s in Redis", workerID, id)
				}
				log.Printf("Worker %d: Successfully parsed content for Pratilipi ID %s. Clean text length: %d", workerID, id, len(content))
			}
		}(i)
	}

	go func() {
		defer close(pratilipiIDChannel)
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
				pratilipiIDChannel <- id
			}
		}
	}()

	wg.Wait()
	fmt.Println("All Pratilipi IDs have been processed. Application finished.")
}
