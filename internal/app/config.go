package app

import (
	"fmt"
	"os"
)

type Config struct {
	HTTPAddr           string
	DatabaseURL        string
	OIDCIssuerURL      string
	OIDCAudience       string
	AWSRegion          string
	AWSEndpointURL     string
	SQSRequestQueueURL string
	SQSEventQueueURL   string
	SQSDLQQueueURL     string
}

func NewConfig() (Config, error) {
	cfg := Config{
		HTTPAddr:           env("HTTP_ADDR", ":8080"),
		DatabaseURL:        env("DATABASE_URL", "postgres://wager:wager@postgres:5432/wager?sslmode=disable"),
		OIDCIssuerURL:      env("OIDC_ISSUER_URL", "http://keycloak:8080/realms/wager"),
		OIDCAudience:       env("OIDC_AUDIENCE", "wager-api"),
		AWSRegion:          env("AWS_REGION", "us-east-1"),
		AWSEndpointURL:     os.Getenv("AWS_ENDPOINT_URL"),
		SQSRequestQueueURL: env("SQS_REQUEST_QUEUE_URL", "http://localstack:4566/000000000000/wager-transactions.fifo"),
		SQSEventQueueURL:   env("SQS_EVENT_QUEUE_URL", "http://localstack:4566/000000000000/wager-events"),
		SQSDLQQueueURL:     env("SQS_DLQ_QUEUE_URL", "http://localstack:4566/000000000000/wager-transactions-dlq.fifo"),
	}
	if cfg.DatabaseURL == "" || cfg.OIDCIssuerURL == "" || cfg.OIDCAudience == "" || cfg.AWSRegion == "" || cfg.SQSRequestQueueURL == "" || cfg.SQSEventQueueURL == "" || cfg.SQSDLQQueueURL == "" {
		return Config{}, fmt.Errorf("database, OIDC, AWS region, and SQS queue settings are required")
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
