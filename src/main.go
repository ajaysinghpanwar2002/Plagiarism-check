package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"plagiarism-detector/src/config"
	"plagiarism-detector/src/monitoring"
	"plagiarism-detector/src/moss"
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

				if len(content) < config.MinContentLength {
					log.Printf("Worker %d: Skipping Pratilipi ID %s (lang: %s) due to short content length (%d < %d)", workerID, task.ID, task.Language, len(content), config.MinContentLength)
					monitoring.Increment("short-content-length", StatsDClient)
					continue
				}

				hash := simhash.New(content, task.Language)

				potentialMatchIDs, err := redisClient.CheckPotentialSimhashMatches(ctx, task.ID, task.Language, hash)
				if err != nil {
					log.Printf("Worker %d: ERROR checking potential SimHash matches for Pratilipi ID %s (lang: %s): %v", workerID, task.ID, task.Language, err)
					monitoring.Increment("failed-check-potential-simhash", StatsDClient)
				}
				if len(potentialMatchIDs) > 0 {
					log.Printf("Worker %d: Potential Simhash matches found for %s: %v. Verifying with MOSS...", workerID, task.ID, potentialMatchIDs)
					fp1 := moss.GenerateFingerprint(content, config.MossKGramSize, config.MossWindowSize)
					var mossConfirmed bool = false

					for _, candidateID := range potentialMatchIDs {
						if candidateID == task.ID {
							continue
						}
						candidateContent, err := s3Downloader.DownloadStoryContent(ctx, candidateID)
						if err != nil {
							log.Printf("Worker %d: MOSS Check: Failed to download content for candidate ID %s: %v. Skipping MOSS for this candidate.", workerID, candidateID, err)
							monitoring.Increment("moss-candidate-download-failed", StatsDClient)
							continue
						}
						if candidateContent == "" {
							log.Printf("Worker %d: MOSS Check: Empty content for candidate (%s). Skipping MOSS for this candidate.", workerID, candidateID)
							monitoring.Increment("moss-candidate-content-empty", StatsDClient)
							continue
						}

						fp2 := moss.GenerateFingerprint(candidateContent, config.MossKGramSize, config.MossWindowSize)
						similarity := moss.CalculateSimilarity(fp1, fp2)

						log.Printf("Worker %d: MOSS Check for %s vs %s: Similarity %.2f (Threshold: %.2f)", workerID, task.ID, candidateID, similarity, config.MossSimilarityThreshold)

						if similarity >= config.MossSimilarityThreshold {
							log.Printf("Worker %d: CONFIRMED PLAGIARISM via MOSS for Pratilipi ID %s (lang: %s). Similar to %s. MOSS Similarity: %.2f",
								workerID, task.ID, task.Language, candidateID, similarity)
							monitoring.Increment("moss-confirmed-plagiarism", StatsDClient)

							errStore := redisClient.StoreConfirmedPlagiarism(ctx, task.Language, task.ID, candidateID)
							if errStore != nil {
								log.Printf("Worker %d: ERROR storing confirmed plagiarism for %s to %s: %v", workerID, task.ID, candidateID, errStore)
							}
							mossConfirmed = true
							break
						}
					}

					if mossConfirmed {
						log.Printf("Worker %d: Confirmed plagiarism processed for Pratilipi ID %s (lang: %s)", workerID, task.ID, task.Language)
					} else {
						log.Printf("Worker %d: No MOSS confirmation for Pratilipi ID %s (lang: %s) despite potential Simhash matches.", workerID, task.ID, task.Language)
						monitoring.Increment("moss-no-confirmation-for-potential-simhash", StatsDClient)
					}
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
