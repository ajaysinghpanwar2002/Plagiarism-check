package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"plagiarism-detector/src/config"
	"plagiarism-detector/src/sources"
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

				// The content is now clean text, ready for SimHash generation.
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
