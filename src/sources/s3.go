package sources

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"path/filepath"

	"plagiarism-detector/src/monitoring"
	"plagiarism-detector/src/parser"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cactus/go-statsd-client/v5/statsd"
)

type IndexFile struct {
	Chapters         []string `json:"chapters"`
	DeletedChapters  []string `json:"deletedChapters"`
	NotFoundChapters []string `json:"notFoundChapters"`
	DocVersion       string   `json:"docVersion"`
	ID               int64    `json:"_id"`
	LastUpdated      string   `json:"lastUpdated"`
	CreatedAt        string   `json:"createdAt"`
	V                int      `json:"__v"`
	LastBackupAt     string   `json:"lastBackupAt"`
}

type S3Downloader struct {
	s3Client      *s3.Client
	storyS3Bucket string
	statsdClient  statsd.Statter
}

func NewS3Downloader(ctx context.Context, region, storyS3Bucket string, statsdClient statsd.Statter) (*S3Downloader, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return &S3Downloader{
		s3Client:      s3.NewFromConfig(cfg),
		storyS3Bucket: storyS3Bucket,
		statsdClient:  statsdClient,
	}, nil
}

func (s *S3Downloader) DownloadStoryContent(ctx context.Context, pratilipiID string) (string, error) {
	indexPath := filepath.Join(pratilipiID, "index")

	indexObj, err := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.storyS3Bucket),
		Key:    aws.String(indexPath),
	})
	if err != nil {
		monitoring.Increment("failed-download-index", s.statsdClient)
		return "", fmt.Errorf("failed to download index file %s for pratilipi %s: %w", indexPath, pratilipiID, err)
	}
	defer indexObj.Body.Close()

	indexBody, err := io.ReadAll(indexObj.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read index file body for pratilipi %s: %w", pratilipiID, err)
	}

	var indexData IndexFile
	if err := json.Unmarshal(indexBody, &indexData); err != nil {
		return "", fmt.Errorf("failed to unmarshal index data for pratilipi %s: %w", pratilipiID, err)
	}

	if len(indexData.Chapters) == 0 {
		log.Printf("No chapters found for pratilipi ID %s", pratilipiID)
		monitoring.Increment("no-chapters-found", s.statsdClient)
		return "", nil
	}

	var contentBuffer bytes.Buffer
	for _, chapterID := range indexData.Chapters {
		chapterPath := filepath.Join(pratilipiID, "chapters", chapterID)
		chapterObj, err := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.storyS3Bucket),
			Key:    aws.String(chapterPath),
		})
		if err != nil {
			log.Printf("WARN: Failed to download chapter %s for pratilipi %s: %v. Skipping chapter.", chapterPath, pratilipiID, err)
			monitoring.Increment("failed-download-chapter", s.statsdClient)
			continue
		}
		defer chapterObj.Body.Close()

		chapterBody, err := io.ReadAll(chapterObj.Body)
		if err != nil {
			log.Printf("WARN: Failed to read chapter body %s for pratilipi %s: %v. Skipping chapter.", chapterPath, pratilipiID, err)
			continue
		}

		cleanedText, err := parser.Parse(chapterBody)
		if err != nil {
			log.Printf("WARN: Failed to parse chapter %s for pratilipi %s: %v. Skipping chapter.", chapterPath, pratilipiID, err)
			continue
		}

		if cleanedText != "" {
			contentBuffer.WriteString(cleanedText)
			contentBuffer.WriteString("\n")
		}
	}

	return contentBuffer.String(), nil
}
