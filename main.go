package main

import (
	"context"
	"fmt"
	"log"
	"plagiarism-detector/sources"
	"sync"
)

func main() {
	config, err := LoadConfig()
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

	pratilipiIDChannel := make(chan string, 100)
	var wg sync.WaitGroup

	for i := 0; i < config.NumWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for id := range pratilipiIDChannel {
				// For now, the worker just logs the ID.
				// Later, it will download from S3, generate a SimHash, etc.
				log.Printf("Worker %d: Processing Pratilipi ID %s", workerID, id)
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
