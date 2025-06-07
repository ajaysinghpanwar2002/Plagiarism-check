package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	AthenaDatabase      string
	AthenaResultsBucket string
	AthenaOutputPrefix  string
	AWSRegion           string
	Languages           []string
	NumWorkers          int
	StoryS3Bucket       string
}

func LoadConfig() (*Config, error) {
	if err := godotenv.Load(); err != nil {
		return nil, fmt.Errorf("loading .env file: %w", err)
	}

	config := &Config{
		AthenaDatabase:      os.Getenv("ATHENA_DATABASE"),
		AthenaResultsBucket: os.Getenv("ATHENA_RESULTS_BUCKET"),
		AthenaOutputPrefix:  os.Getenv("ATHENA_OUTPUT_PREFIX"),
		AWSRegion:           os.Getenv("AWS_REGION"),
		StoryS3Bucket:       os.Getenv("STORY_S3_BUCKET"),
		Languages:           []string{"ENGLISH"},
		NumWorkers:          10,
	}

	if config.AthenaDatabase == "" || config.AthenaResultsBucket == "" ||
		config.AthenaOutputPrefix == "" || config.AWSRegion == "" {
		return nil, fmt.Errorf("missing required environment variables")
	}

	return config, nil
}
