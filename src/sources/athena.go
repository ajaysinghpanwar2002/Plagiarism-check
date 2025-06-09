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
)

type AthenaProcessor struct {
	athenaClient *athena.Client
	s3Client     *s3.Client
	s3Bucket     string
	database     string
	outputPrefix string
	statsdClient statsd.Statter
}

func NewAthenaProcessor(ctx context.Context, region, s3Bucket, outputPrefix, database string, statsdClient statsd.Statter) (*AthenaProcessor, error) {
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
