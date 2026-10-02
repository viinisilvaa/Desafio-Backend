package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/desafio/wager-service/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const inboxConsumerName = "wager-transactions-v1"

type requestEnvelope struct {
	MessageID  string              `json:"messageId"`
	Type       string              `json:"type"`
	OccurredAt string              `json:"occurredAt"`
	Data       domain.WagerRequest `json:"data"`
}

type SQSConsumer struct {
	client    *sqs.Client
	queueURL  string
	processor *Processor
	logger    *slog.Logger
	metrics   *Metrics
}

func NewSQSConsumer(client *sqs.Client, cfg Config, processor *Processor, logger *slog.Logger, metrics *Metrics) *SQSConsumer {
	return &SQSConsumer{client: client, queueURL: cfg.SQSRequestQueueURL, processor: processor, logger: logger, metrics: metrics}
}

func (c *SQSConsumer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		response, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(c.queueURL), MaxNumberOfMessages: 5, WaitTimeSeconds: 20,
			VisibilityTimeout:           45,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameMessageGroupId},
		})
		if err != nil {
			if ctx.Err() == nil {
				c.metrics.retries.Add(1)
				c.logger.Error("sqs receive failed", "error", err)
				waitContext(ctx, time.Second)
			}
			continue
		}
		c.handleBatch(ctx, response.Messages)
	}
}

func (c *SQSConsumer) handleBatch(ctx context.Context, messages []types.Message) {
	var workers sync.WaitGroup
	for _, group := range groupFIFOMessageBatch(messages) {
		workers.Add(1)
		go func(group []types.Message) {
			defer workers.Done()
			for _, message := range group {
				if ctx.Err() != nil {
					return
				}
				c.handle(ctx, message)
			}
		}(group)
	}
	workers.Wait()
}

func groupFIFOMessageBatch(messages []types.Message) [][]types.Message {
	groupIndexes := make(map[string]int)
	groups := make([][]types.Message, 0)
	for _, message := range messages {
		groupID := message.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
		index, exists := groupIndexes[groupID]
		if !exists {
			index = len(groups)
			groupIndexes[groupID] = index
			groups = append(groups, nil)
		}
		groups[index] = append(groups[index], message)
	}
	return groups
}

func (c *SQSConsumer) handle(parent context.Context, message types.Message) {
	if parent.Err() != nil {
		return
	}
	var envelope requestEnvelope
	if message.Body == nil || json.Unmarshal([]byte(aws.ToString(message.Body)), &envelope) != nil || envelope.Type != "WagerTransactionRequested" || envelope.MessageID == "" {
		c.recordFailedDelivery(message)
		c.logger.Warn("invalid wager message; leaving for redrive", "messageId", aws.ToString(message.MessageId))
		return
	}
	if _, err := time.Parse(time.RFC3339Nano, envelope.OccurredAt); err != nil {
		c.recordFailedDelivery(message)
		c.logger.Warn("invalid wager event timestamp; leaving for redrive", "messageId", envelope.MessageID)
		return
	}
	digest := sha256.Sum256([]byte(aws.ToString(message.Body)))
	messageHash := hex.EncodeToString(digest[:])
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	result, err := c.processor.Process(ctx, envelope.Data, inboxConsumerName, envelope.MessageID, messageHash)
	if err != nil {
		c.recordFailedDelivery(message)
		c.logger.Warn("wager message processing will retry", "messageId", envelope.MessageID, "providerId", envelope.Data.ProviderID, "error", err)
		return
	}
	if _, err = c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(c.queueURL), ReceiptHandle: message.ReceiptHandle}); err != nil {
		c.metrics.retries.Add(1)
		c.logger.Error("committed wager could not be acknowledged", "messageId", envelope.MessageID, "transactionId", result.TransactionID, "error", err)
	}
}

func (c *SQSConsumer) recordFailedDelivery(message types.Message) {
	if c.metrics != nil {
		c.metrics.retries.Add(1)
	}
}

type OutboxPublisher struct {
	pool     *pgxpool.Pool
	client   *sqs.Client
	queueURL string
	logger   *slog.Logger
	metrics  *Metrics
}

func NewOutboxPublisher(pool *pgxpool.Pool, client *sqs.Client, cfg Config, logger *slog.Logger, metrics *Metrics) *OutboxPublisher {
	return &OutboxPublisher{pool: pool, client: client, queueURL: cfg.SQSEventQueueURL, logger: logger, metrics: metrics}
}

type pendingEvent struct {
	ID      string
	Payload []byte
	Attempt int
}

func (p *OutboxPublisher) Run(ctx context.Context) {
	for ctx.Err() == nil {
		events, err := p.claim(ctx, 10)
		if err != nil {
			if ctx.Err() == nil {
				p.logger.Error("outbox claim failed", "error", err)
				waitContext(ctx, time.Second)
			}
			continue
		}
		if len(events) == 0 {
			p.refreshLag(ctx)
			waitContext(ctx, 500*time.Millisecond)
			continue
		}
		p.refreshLag(ctx)
		for _, event := range events {
			if ctx.Err() != nil {
				return
			}
			p.publish(ctx, event)
		}
	}
}

func (p *OutboxPublisher) claim(ctx context.Context, limit int) ([]pendingEvent, error) {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `WITH candidates AS (
		SELECT event_id FROM outbox WHERE published_at IS NULL AND next_attempt_at<=now() AND (locked_until IS NULL OR locked_until<now())
		ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED
	) UPDATE outbox o SET locked_until=now()+interval '60 seconds', attempts=o.attempts+1 FROM candidates c
	WHERE o.event_id=c.event_id RETURNING o.event_id::text,o.payload,o.attempts`, limit)
	if err != nil {
		return nil, err
	}
	events := make([]pendingEvent, 0, limit)
	for rows.Next() {
		var event pendingEvent
		if err = rows.Scan(&event.ID, &event.Payload, &event.Attempt); err != nil {
			rows.Close()
			return nil, err
		}
		events = append(events, event)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return events, nil
}

func (p *OutboxPublisher) refreshLag(ctx context.Context) {
	var lagMillis int64
	err := p.pool.QueryRow(ctx, `SELECT COALESCE((extract(epoch FROM now()-min(created_at))*1000)::bigint,0) FROM outbox WHERE published_at IS NULL`).Scan(&lagMillis)
	if err != nil {
		p.logger.Warn("outbox lag metric refresh failed", "error", err)
		return
	}
	p.metrics.outboxLagMillis.Store(lagMillis)
}

func (p *OutboxPublisher) publish(ctx context.Context, event pendingEvent) {
	_, err := p.client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(p.queueURL), MessageBody: aws.String(string(event.Payload))})
	if err != nil {
		attempt := min(event.Attempt, 10)
		p.metrics.retries.Add(1)
		delay := time.Second * time.Duration(math.Pow(2, float64(attempt)))
		if delay > 5*time.Minute {
			delay = 5 * time.Minute
		}
		_, dbErr := p.pool.Exec(ctx, `UPDATE outbox SET locked_until=NULL,next_attempt_at=now()+$1::interval,last_error=$2 WHERE event_id=$3 AND published_at IS NULL`, delay.String(), truncateError(err.Error()), event.ID)
		if dbErr != nil {
			p.logger.Error("outbox retry scheduling failed", "eventId", event.ID, "error", dbErr)
		}
		p.logger.Warn("outbox send failed", "eventId", event.ID, "attempt", event.Attempt, "error", err)
		return
	}
	if _, err = p.pool.Exec(ctx, `UPDATE outbox SET published_at=now(),locked_until=NULL,last_error=NULL WHERE event_id=$1 AND published_at IS NULL`, event.ID); err != nil {
		p.logger.Error("outbox event sent but acknowledgement failed", "eventId", event.ID, "error", err)
	}
}

func waitContext(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func truncateError(value string) string {
	if len(value) > 1000 {
		return value[:1000]
	}
	return value
}
