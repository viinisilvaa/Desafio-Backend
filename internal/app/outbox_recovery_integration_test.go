package app

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/desafio/wager-service/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestOutboxRecoversAfterPublishBeforeDatabaseAck(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	endpointURL := os.Getenv("TEST_SQS_ENDPOINT_URL")
	if databaseURL == "" || endpointURL == "" {
		t.Skip("set TEST_DATABASE_URL and TEST_SQS_ENDPOINT_URL to run LocalStack outbox recovery test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := migrations.Run(ctx, databaseURL, "up"); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(env("AWS_REGION", "us-east-1")),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(env("AWS_ACCESS_KEY_ID", "test"), env("AWS_SECRET_ACCESS_KEY", "test"), "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := sqs.NewFromConfig(awsCfg, func(options *sqs.Options) { options.BaseEndpoint = aws.String(endpointURL) })
	queueName := "wager-outbox-recovery-" + strings.ReplaceAll(uuid.NewString(), "-", "")
	queueOutput, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(queueName)})
	if err != nil {
		t.Fatal(err)
	}
	queueURL := aws.ToString(queueOutput.QueueUrl)
	defer func() {
		_, _ = client.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: aws.String(queueURL)})
	}()

	eventID := uuid.NewString()
	walletID := uuid.NewString()
	event := newProcessedEvent(eventID, walletID, eventID, uuid.NewString(), "provider-test", "external-test", "BET", 1)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = insertEvent(ctx, tx, event); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `UPDATE outbox SET created_at='1970-01-01T00:00:00Z',next_attempt_at=now() WHERE event_id=$1`, eventID); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = pool.Exec(context.Background(), `DELETE FROM outbox WHERE event_id=$1`, eventID) }()

	publisher := NewOutboxPublisher(pool, client, Config{SQSEventQueueURL: queueURL}, testLogger(), NewMetrics())
	claimed, err := publisher.claim(ctx, 1)
	if err != nil || len(claimed) != 1 || claimed[0].ID != eventID {
		t.Fatalf("first claim did not select fixture: events=%+v err=%v", claimed, err)
	}
	if _, err = client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(queueURL), MessageBody: aws.String(string(claimed[0].Payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE outbox SET locked_until=now()-interval '1 second' WHERE event_id=$1 AND published_at IS NULL`, eventID); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := publisher.claim(ctx, 1)
	if err != nil || len(reclaimed) != 1 || reclaimed[0].ID != eventID {
		t.Fatalf("expired outbox lease was not recovered: events=%+v err=%v", reclaimed, err)
	}
	publisher.publish(ctx, reclaimed[0])
	var isPublished bool
	if err = pool.QueryRow(ctx, `SELECT published_at IS NOT NULL FROM outbox WHERE event_id=$1`, eventID).Scan(&isPublished); err != nil {
		t.Fatal(err)
	}
	if !isPublished {
		t.Fatal("recovered event was not marked published")
	}
	seen := make([]string, 0, 2)
	for len(seen) < 2 {
		response, receiveErr := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 10, WaitTimeSeconds: 1})
		if receiveErr != nil {
			t.Fatal(receiveErr)
		}
		for _, message := range response.Messages {
			var received eventEnvelope
			if err = json.Unmarshal([]byte(aws.ToString(message.Body)), &received); err != nil {
				t.Fatal(err)
			}
			seen = append(seen, received.EventID)
			if _, err = client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(queueURL), ReceiptHandle: message.ReceiptHandle}); err != nil {
				t.Fatal(err)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("did not receive both copies of recovered event: %v", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	if len(seen) != 2 || seen[0] != eventID || seen[1] != eventID {
		t.Fatalf("event identity changed across at-least-once publication: %v", seen)
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
