package sources

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cactus/go-statsd-client/v5/statsd"
	"plagiarism-detector/src/monitoring"
	"plagiarism-detector/src/storage"
)

type PratilipiData struct {
	ID       string
	Language string
}

type AthenaProcessor struct {
	athenaClient   *athena.Client
	s3Client       *s3.Client
	s3Bucket       string
	database       string
	outputPrefix   string
	statsdClient   statsd.Statter
	redisClient    *storage.RedisClient
	fetchStartDate string
}

func NewAthenaProcessor(ctx context.Context, region, s3Bucket, outputPrefix, database, fetchStartDate string, statsdClient statsd.Statter, redisClient *storage.RedisClient) (*AthenaProcessor, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return &AthenaProcessor{
		athenaClient:   athena.NewFromConfig(cfg),
		s3Client:       s3.NewFromConfig(cfg),
		s3Bucket:       s3Bucket,
		outputPrefix:   outputPrefix,
		database:       database,
		statsdClient:   statsdClient,
		redisClient:    redisClient,
		fetchStartDate: fetchStartDate,
	}, nil
}

func (a *AthenaProcessor) executeQueryAndProcess(ctx context.Context, query string) ([]string, error) {
	outputLocation := fmt.Sprintf("s3://%s/%s", a.s3Bucket, a.outputPrefix)
	startInput := &athena.StartQueryExecutionInput{
		QueryString: aws.String(query),
		QueryExecutionContext: &types.QueryExecutionContext{
			Database: aws.String(a.database),
		},
		ResultConfiguration: &types.ResultConfiguration{
			OutputLocation: aws.String(outputLocation),
		},
	}

	startOutput, err := a.athenaClient.StartQueryExecution(ctx, startInput)
	if err != nil {
		return nil, fmt.Errorf("failed to start query execution: %w", err)
	}

	queryExecutionID := *startOutput.QueryExecutionId
	log.Printf("Query execution started with ID: %s", queryExecutionID)

	if err := a.waitForQueryCompletion(ctx, queryExecutionID); err != nil {
		return nil, err
	}

	return a.processQueryResults(ctx, queryExecutionID)
}

func (a *AthenaProcessor) waitForQueryCompletion(ctx context.Context, queryExecutionID string) error {
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		input := &athena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(queryExecutionID),
		}

		output, err := a.athenaClient.GetQueryExecution(ctx, input)
		if err != nil {
			return fmt.Errorf("failed to get query execution status: %w", err)
		}

		state := output.QueryExecution.Status.State
		log.Printf("Query execution status: %s", state)

		switch state {
		case types.QueryExecutionStateSucceeded:
			return nil
		case types.QueryExecutionStateFailed, types.QueryExecutionStateCancelled:
			reason := ""
			if output.QueryExecution.Status.StateChangeReason != nil {
				reason = *output.QueryExecution.Status.StateChangeReason
			}
			monitoring.Increment("athena-query-execution-failed", a.statsdClient)
			return fmt.Errorf("query execution failed or cancelled: %s", reason)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Second):
		}
	}

	return fmt.Errorf("query execution timed out after %d retries", maxRetries)
}

func (a *AthenaProcessor) processQueryResults(ctx context.Context, queryExecutionID string) ([]string, error) {
	getExecInput := &athena.GetQueryExecutionInput{
		QueryExecutionId: aws.String(queryExecutionID),
	}

	execOutput, err := a.athenaClient.GetQueryExecution(ctx, getExecInput)
	if err != nil {
		return nil, fmt.Errorf("failed to get query execution details: %w", err)
	}

	s3OutputLocation := *execOutput.QueryExecution.ResultConfiguration.OutputLocation
	s3Key := s3OutputLocation[len(fmt.Sprintf("s3://%s/", a.s3Bucket)):]

	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(a.s3Bucket),
		Key:    aws.String(s3Key),
	}

	s3Output, err := a.s3Client.GetObject(ctx, getObjectInput)
	if err != nil {
		return nil, fmt.Errorf("failed to get results from S3: %w", err)
	}
	defer s3Output.Body.Close()

	csvReader := csv.NewReader(s3Output.Body)
	if _, err := csvReader.Read(); err != nil {
		if err == io.EOF {
			return []string{}, nil // Empty result set
		}
		return nil, fmt.Errorf("failed to read CSV header: %w", err)
	}

	var pratilipiIDs []string
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading CSV record: %v", err)
			continue
		}
		if len(record) > 0 {
			pratilipiIDs = append(pratilipiIDs, record[0])
		}
	}

	log.Printf("Successfully processed %d records from query %s", len(pratilipiIDs), queryExecutionID)
	monitoring.Increment("athena-query-execution-success", a.statsdClient)
	return pratilipiIDs, nil
}

func (a *AthenaProcessor) FetchPublishedPratilipiIDs(
	ctx context.Context,
	language string,
	taskChannel chan<- PratilipiData,
) error {
	processingDateRedis, dateFoundInRedis, err := a.redisClient.GetProcessingDate(ctx, language)
	if err != nil {
		log.Printf("ERROR: Failed to get processing date for %s from Redis: %v. Aborting.", language, err)
		return fmt.Errorf("failed to get processing date for %s: %w", language, err)
	}

	var dateToProcess time.Time
	if dateFoundInRedis {
		dateToProcess = processingDateRedis
		log.Printf("Resuming processing for language %s for date %s (from Redis).", language, dateToProcess.Format("2006-01-02"))
	} else {
		dateToProcess = time.Now().In(time.UTC).AddDate(0, 0, -1)
		log.Printf("No processing date found in Redis for %s. Processing data for yesterday: %s.", language, dateToProcess.Format("2006-01-02"))
	}

	startOfDay := time.Date(dateToProcess.Year(), dateToProcess.Month(), dateToProcess.Day(), 0, 0, 0, 0, time.UTC)
	endOfDay := startOfDay.AddDate(0, 0, 1)
	dateFilterClause := fmt.Sprintf(
		" AND published_at >= to_unixtime(parse_datetime('%s','yyyy-MM-dd HH:mm:ss')) AND published_at < to_unixtime(parse_datetime('%s','yyyy-MM-dd HH:mm:ss'))",
		startOfDay.Format("2006-01-02 15:04:05"),
		endOfDay.Format("2006-01-02 15:04:05"),
	)
	log.Printf("Querying for %s for content published between %s and %s.",
		language,
		startOfDay.Format("2006-01-02 15:04:05"),
		endOfDay.Format("2006-01-02 15:04:05"),
	)

	initialOffset, err := a.redisClient.GetCheckpointOffset(ctx, language)
	if err != nil {
		log.Printf("WARN: Failed to get checkpoint for %s (date %s), starting from offset 0: %v", language, dateToProcess.Format("2006-01-02"), err)
		initialOffset = 0
	}
	log.Printf("Resuming FetchPublishedPratilipiIDs for %s (date %s) from offset %d", language, dateToProcess.Format("2006-01-02"), initialOffset)

	currentQueryOffset := initialOffset
	var itemsSentThisRun int64 = 0
	var itemsSentSinceLastSuccessfulCheckpoint int64 = 0
	var allBatchesForDayProcessed bool = false

	const batchSize = 50000
	const checkpointUpdateCountThreshold = 1000

	defer func() {
		if allBatchesForDayProcessed {
			// Defer should not save checkpoint for the completed day.
			log.Printf("Defer: Day %s for language %s completed. Checkpoint management handled by main logic.", dateToProcess.Format("2006-01-02"), language)
			return
		}

		if itemsSentSinceLastSuccessfulCheckpoint > 0 {
			finalCheckpointValue := initialOffset + itemsSentThisRun
			log.Printf("Defer: Attempting to set final checkpoint for %s (date %s) at offset %d on exit. (Items sent this run: %d, items since last checkpoint: %d)",
				language, dateToProcess.Format("2006-01-02"), finalCheckpointValue, itemsSentThisRun, itemsSentSinceLastSuccessfulCheckpoint)

			saveCtx, cancelSave := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancelSave()

			if err := a.redisClient.SetCheckpointOffset(saveCtx, language, finalCheckpointValue); err != nil {
				log.Printf("Defer ERROR: Failed to set final checkpoint for %s (date %s) at offset %d: %v", language, dateToProcess.Format("2006-01-02"), finalCheckpointValue, err)
			} else {
				log.Printf("Defer: Successfully set final checkpoint for %s (date %s) at offset %d on exit", language, dateToProcess.Format("2006-01-02"), finalCheckpointValue)
			}
		} else if itemsSentThisRun > 0 {
			log.Printf("Defer: No new items processed since last successful checkpoint for %s (date %s). Final checkpoint not updated. (Total items sent this run: %d)", language, dateToProcess.Format("2006-01-02"), itemsSentThisRun)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Context cancelled, stopping FetchPublishedPratilipiIDs for %s (date %s).", language, dateToProcess.Format("2006-01-02"))
			return ctx.Err()
		default:
			// Continue if context is not cancelled.
		}

		query := fmt.Sprintf(
			"SELECT id FROM pratilipi_pratilipi WHERE language='%s' AND content_type='PRATILIPI' AND state='PUBLISHED'%s ORDER BY id DESC OFFSET %d LIMIT %d",
			language,
			dateFilterClause,
			currentQueryOffset,
			batchSize,
		)

		log.Printf("Fetching batch for %s (date %s) with query: %s", language, dateToProcess.Format("2006-01-02"), query)
		batchIDs, err := a.executeQueryAndProcess(ctx, query)
		if err != nil {
			log.Printf("ERROR: Failed to execute query for batch (lang: %s, date: %s, offset: %d): %v. Retrying after delay.", language, dateToProcess.Format("2006-01-02"), currentQueryOffset, err)
			select {
			case <-time.After(10 * time.Second):
				continue
			case <-ctx.Done():
				log.Printf("Context cancelled while waiting to retry query for %s (date %s).", language, dateToProcess.Format("2006-01-02"))
				return ctx.Err()
			}
		}

		if len(batchIDs) == 0 {
			log.Printf("No more IDs found for %s for date %s at offset %d. Fetch completed for this date.", language, dateToProcess.Format("2006-01-02"), currentQueryOffset)
			allBatchesForDayProcessed = true
			break
		}

		log.Printf("Fetched %d IDs for %s (date: %s, offset: %d)", len(batchIDs), language, dateToProcess.Format("2006-01-02"), currentQueryOffset)

		for _, id := range batchIDs {
			task := PratilipiData{ID: id, Language: language}
			select {
			case taskChannel <- task:
				itemsSentThisRun++
				itemsSentSinceLastSuccessfulCheckpoint++
				if itemsSentSinceLastSuccessfulCheckpoint >= checkpointUpdateCountThreshold {
					checkpointValue := initialOffset + itemsSentThisRun
					log.Printf("Threshold reached: saving checkpoint for %s (date %s) at %d", language, dateToProcess.Format("2006-01-02"), checkpointValue)

					checkpointCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					err := a.redisClient.SetCheckpointOffset(checkpointCtx, language, checkpointValue)
					cancel() // Call cancel regardless of error

					if err != nil {
						log.Printf("ERROR: failed to set checkpoint for %s (date %s) at %d: %v", language, dateToProcess.Format("2006-01-02"), checkpointValue, err)
					} else {
						log.Printf("Saved checkpoint for %s (date %s) at %d", language, dateToProcess.Format("2006-01-02"), checkpointValue)
						itemsSentSinceLastSuccessfulCheckpoint = 0
					}
				}
			case <-ctx.Done():
				log.Printf("Context cancelled while sending task for ID %s, language %s (date %s).", id, language, dateToProcess.Format("2006-01-02"))
				return ctx.Err()
			}
		}

		currentQueryOffset += int64(len(batchIDs))
	}

	if allBatchesForDayProcessed {
		nextDateToProcess := dateToProcess.AddDate(0, 0, 1)
		log.Printf("All items for %s on %s processed. Total items sent this run: %d. Attempting to set next processing date to %s and reset offset.",
			language, dateToProcess.Format("2006-01-02"), itemsSentThisRun, nextDateToProcess.Format("2006-01-02"))

		updateCtx, updateCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer updateCancel()

		if err := a.redisClient.SetCheckpointOffset(updateCtx, language, 0); err != nil {
			log.Printf("ERROR: Failed to reset checkpoint offset to 0 for %s (for next date %s): %v. Next run might re-process data if offset isn't 0.",
				language, nextDateToProcess.Format("2006-01-02"), err)
			return fmt.Errorf("failed to reset checkpoint offset for %s for date %s: %w", language, nextDateToProcess.Format("2006-01-02"), err)
		}
		log.Printf("Successfully reset checkpoint offset to 0 for %s for next date %s.", language, nextDateToProcess.Format("2006-01-02"))

		if err := a.redisClient.SetProcessingDate(updateCtx, language, nextDateToProcess); err != nil {
			log.Printf("ERROR: Failed to set next processing date for %s to %s: %v. Checkpoint offset NOT reset.", language, nextDateToProcess.Format("2006-01-02"), err)
			return fmt.Errorf("failed to set next processing date for %s to %s: %w", language, nextDateToProcess.Format("2006-01-02"), err)
		}
		log.Printf("Successfully set next processing date for %s to %s.", language, nextDateToProcess.Format("2006-01-02"))

		itemsSentSinceLastSuccessfulCheckpoint = 0 // Ensure defer doesn't save a stale checkpoint

		log.Printf("Finished fetching all IDs for language %s for date %s. Next processing date set to %s, offset reset to 0.",
			language, dateToProcess.Format("2006-01-02"), nextDateToProcess.Format("2006-01-02"))
		return nil
	}

	// If the loop was exited due to context cancellation or other errors before all batches were processed
	log.Printf("Fetching IDs for language %s for date %s was interrupted or failed. Total items sent in this run: %d. Effective next offset for resumption (for this date): %d.",
		language, dateToProcess.Format("2006-01-02"), itemsSentThisRun, initialOffset+itemsSentThisRun)

	if ctx.Err() != nil {
		return ctx.Err() // Propagate context error
	}

	return fmt.Errorf("processing for %s on %s did not complete successfully", language, dateToProcess.Format("2006-01-02"))
}
