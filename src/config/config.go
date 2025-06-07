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
	RedisAddr           string
	RedisPassword       string
	RedisDB             string
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
		Languages:           []string{"HINDI", "ENGLISH", "TAMIL", "TELUGU", "KANNADA", "MALAYALAM", "BENGALI", "MARATHI", "GUJARATI", "ODIA", "PUNJABI"},
		NumWorkers:          10,
		RedisAddr:           os.Getenv("REDIS_ADDR"),
		RedisPassword:       os.Getenv("REDIS_PASSWORD"),
		RedisDB:             os.Getenv("REDIS_DB"),
	}

	if config.AthenaDatabase == "" || config.AthenaResultsBucket == "" ||
		config.AthenaOutputPrefix == "" || config.AWSRegion == "" || config.RedisAddr == "" {
		return nil, fmt.Errorf("missing required environment variables for AWS or Redis")
	}

	return config, nil
}
