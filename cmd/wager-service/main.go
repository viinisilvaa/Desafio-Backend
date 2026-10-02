package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/desafio/wager-service/internal/app"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

func main() {
	fx.New(
		fx.Module("wager-service",
			fx.Provide(
				app.NewConfig,
				app.NewMetrics,
				newLogger,
				newPool,
				newSQSClient,
				app.NewAuthenticator,
				app.NewProcessor,
				app.NewHTTPServer,
				app.NewSQSConsumer,
				app.NewOutboxPublisher,
				app.NewReferenceWorker,
				app.NewDLQAuditor,
			),
			fx.Invoke(validateDependencies, registerLifecycle),
		),
	).Run()
}

func newLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

func newPool(lifecycle fx.Lifecycle, cfg app.Config) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	lifecycle.Append(fx.Hook{OnStop: func(context.Context) error {
		pool.Close()
		return nil
	}})
	return pool, nil
}

func newSQSClient(cfg app.Config) (*sqs.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	options := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(cfg.AWSRegion)}
	loaded, err := awsconfig.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, err
	}
	return sqs.NewFromConfig(loaded, func(clientOptions *sqs.Options) {
		if cfg.AWSEndpointURL != "" {
			clientOptions.BaseEndpoint = aws.String(cfg.AWSEndpointURL)
		}
	}), nil
}

func validateDependencies(cfg app.Config, pool *pgxpool.Pool, client *sqs.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		return err
	}
	for _, queueURL := range []string{cfg.SQSRequestQueueURL, cfg.SQSEventQueueURL, cfg.SQSDLQQueueURL} {
		if _, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(queueURL)}); err != nil {
			return err
		}
	}
	return nil
}

func registerLifecycle(lifecycle fx.Lifecycle, cfg app.Config, pool *pgxpool.Pool, server *app.HTTPServer, consumer *app.SQSConsumer, publisher *app.OutboxPublisher, references *app.ReferenceWorker, dlq *app.DLQAuditor, logger *slog.Logger) {
	var workers *app.WorkerGroup
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := server.Start(ctx); err != nil {
				return err
			}
			workers = app.NewWorkerGroup(consumer.Run, publisher.Run, references.Run, dlq.Run)
			if err := workers.Start(); err != nil {
				_ = server.Shutdown(ctx)
				return err
			}
			logger.Info("service started", "httpAddr", cfg.HTTPAddr)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			serverErr := server.Shutdown(ctx)
			if workers != nil {
				if err := workers.Stop(ctx); err != nil {
					logger.Warn("worker shutdown deadline exceeded", "error", err)
				}
			}
			logger.Info("service stopped")
			return serverErr
		},
	})
}
