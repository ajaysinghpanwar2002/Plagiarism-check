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
	athenaClient *athena.Client
	s3Client     *s3.Client
	s3Bucket     string
	database     string
	outputPrefix string
	statsdClient statsd.Statter
	redisClient  *storage.RedisClient
}

func NewAthenaProcessor(ctx context.Context, region, s3Bucket, outputPrefix, database string, statsdClient statsd.Statter, redisClient *storage.RedisClient) (*AthenaProcessor, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return &AthenaProcessor{
		athenaClient: athena.NewFromConfig(cfg),
		s3Client:     s3.NewFromConfig(cfg),
		s3Bucket:     s3Bucket,
		outputPrefix: outputPrefix,
		database:     database,
		statsdClient: statsdClient,
		redisClient:  redisClient,
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
			return fmt.Errorf("query execution failed or cancelled: %s", reason)
			monitoring.Increment("athena-query-execution-failed", a.statsdClient)
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

func (a *AthenaProcessor) FetchPublishedPratilipiIDsForYesterday(ctx context.Context, language string) ([]string, error) {
	now := time.Now()
	loc := now.Location()

	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)
	yesterday := today.AddDate(0, 0, -1)

	yesterdayStartStr := yesterday.Format("2006-01-02 15:04:05")
	todayStartStr := today.Format("2006-01-02 15:04:05")

	queryFormat := "SELECT id FROM pratilipi_pratilipi WHERE language='%s' AND content_type='PRATILIPI' AND state='PUBLISHED' AND published_at >= to_unixtime(parse_datetime('%s','yyyy-MM-dd HH:mm:ss')) AND published_at < to_unixtime(parse_datetime('%s','yyyy-MM-dd HH:mm:ss'))"
	query := fmt.Sprintf(queryFormat, language, yesterdayStartStr, todayStartStr)

	log.Printf("Executing query: %s", query)
	return a.executeQueryAndProcess(ctx, query)
}

func (a *AthenaProcessor) FetchPublishedPratilipiIDs(
	ctx context.Context,
	language string,
	taskChannel chan<- PratilipiData,
) error {
	initialOffset, err := a.redisClient.GetCheckpointOffset(ctx, language)
	if err != nil {
		log.Printf("WARN: Failed to get checkpoint for %s, starting from offset 0: %v", language, err)
		initialOffset = 0
	}
	log.Printf("Resuming FetchPublishedPratilipiIDs for %s from offset %d", language, initialOffset)

	currentQueryOffset := initialOffset
	var itemsSentThisRun int64 = 0
	var itemsSentSinceLastSuccessfulCheckpoint int64 = 0

	const batchSize = 50000
	const checkpointUpdateCountThreshold = 10000

	defer func() {
		if itemsSentSinceLastSuccessfulCheckpoint > 0 {
			finalCheckpointValue := initialOffset + itemsSentThisRun
			log.Printf("Attempting to set final checkpoint for %s at offset %d on exit. (Items sent this run: %d, items since last checkpoint: %d)",
				language, finalCheckpointValue, itemsSentThisRun, itemsSentSinceLastSuccessfulCheckpoint)

			saveCtx, cancelSave := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancelSave()

			if err := a.redisClient.SetCheckpointOffset(saveCtx, language, finalCheckpointValue); err != nil {
				log.Printf("ERROR: Failed to set final checkpoint for %s at offset %d: %v", language, finalCheckpointValue, err)
			} else {
				log.Printf("Successfully set final checkpoint for %s at offset %d on exit", language, finalCheckpointValue)
			}
		} else if itemsSentThisRun > 0 {
			log.Printf("No new items processed since last successful checkpoint for %s. Final checkpoint not updated. (Total items sent this run: %d)", language, itemsSentThisRun)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Context cancelled, stopping FetchPublishedPratilipiIDs for %s.", language)
			return ctx.Err()
		default:
			// Continue if context is not cancelled.
		}

		query := fmt.Sprintf(
			"SELECT id FROM pratilipi_pratilipi WHERE language='%s' AND content_type='PRATILIPI' AND state='PUBLISHED' ORDER BY id DESC LIMIT %d OFFSET %d",
			language,
			batchSize,
			currentQueryOffset,
		)

		log.Printf("Fetching batch for %s with query: %s", language, query)
		batchIDs, err := a.executeQueryAndProcess(ctx, query)
		if err != nil {
			log.Printf("ERROR: Failed to execute query for batch (lang: %s, offset: %d): %v. Retrying after delay.", language, currentQueryOffset, err)
			select {
			case <-time.After(10 * time.Second):
				continue
			case <-ctx.Done():
				log.Printf("Context cancelled while waiting to retry query for %s.", language)
				return ctx.Err()
			}
		}

		if len(batchIDs) == 0 {
			log.Printf("No more IDs found for %s at offset %d. Fetch completed.", language, currentQueryOffset)
			break
		}

		log.Printf("Fetched %d IDs for %s (offset: %d)", len(batchIDs), language, currentQueryOffset)

		for _, id := range batchIDs {
			task := PratilipiData{ID: id, Language: language}
			select {
			case taskChannel <- task:
				itemsSentThisRun++
				itemsSentSinceLastSuccessfulCheckpoint++
			case <-ctx.Done():
				log.Printf("Context cancelled while sending task for ID %s, language %s.", id, language)
				return ctx.Err()
			}
		}

		currentQueryOffset += int64(len(batchIDs))

		checkpointValueToStore := initialOffset + itemsSentThisRun
		if itemsSentSinceLastSuccessfulCheckpoint >= checkpointUpdateCountThreshold {
			log.Printf("Attempting to set checkpoint for %s at offset %d. (Items since last checkpoint: %d)",
				language, checkpointValueToStore, itemsSentSinceLastSuccessfulCheckpoint)

			checkpointCtx, checkpointCancel := context.WithTimeout(context.Background(), 15*time.Second)
			err := a.redisClient.SetCheckpointOffset(checkpointCtx, language, checkpointValueToStore)
			checkpointCancel()

			if err != nil {
				log.Printf("ERROR: Failed to set checkpoint for %s at offset %d: %v", language, checkpointValueToStore, err)
			} else {
				log.Printf("Successfully set checkpoint for %s at offset %d", language, checkpointValueToStore)
				itemsSentSinceLastSuccessfulCheckpoint = 0 // Reset counter on successful save.
			}
		}
	}

	log.Printf("Finished fetching all IDs for language %s. Total items sent in this run: %d. Effective next offset for resumption: %d.", language, itemsSentThisRun, initialOffset+itemsSentThisRun)
	return nil
}
