package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/desafio/wager-service/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DLQAuditor struct {
	pool     *pgxpool.Pool
	client   *sqs.Client
	queueURL string
	logger   *slog.Logger
	metrics  *Metrics
}

func NewDLQAuditor(pool *pgxpool.Pool, client *sqs.Client, cfg Config, logger *slog.Logger, metrics *Metrics) *DLQAuditor {
	return &DLQAuditor{pool: pool, client: client, queueURL: cfg.SQSDLQQueueURL, logger: logger, metrics: metrics}
}

func (a *DLQAuditor) Run(ctx context.Context) {
	for ctx.Err() == nil {
		response, err := a.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(a.queueURL), MaxNumberOfMessages: 5, WaitTimeSeconds: 20, VisibilityTimeout: 60,
		})
		if err != nil {
			if ctx.Err() == nil {
				a.metrics.retries.Add(1)
				a.logger.Error("dlq receive failed", "error", err)
				waitContext(ctx, time.Second)
			}
			continue
		}
		var group sync.WaitGroup
		for _, message := range response.Messages {
			group.Add(1)
			go func(message types.Message) {
				defer group.Done()
				a.handle(ctx, message)
			}(message)
		}
		group.Wait()
	}
}

func (a *DLQAuditor) handle(parent context.Context, message types.Message) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	newRecord, disposition, err := a.audit(ctx, message)
	if err != nil {
		if parent.Err() == nil {
			a.metrics.retries.Add(1)
			a.logger.Error("dlq audit failed", "messageId", aws.ToString(message.MessageId), "error", err)
		}
		return
	}
	if newRecord {
		a.metrics.recordDeadLetter(disposition)
	}
	if _, err = a.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(a.queueURL), ReceiptHandle: message.ReceiptHandle}); err != nil {
		a.metrics.retries.Add(1)
		a.logger.Error("audited dlq message could not be acknowledged", "messageId", aws.ToString(message.MessageId), "error", err)
	}
}

func (a *DLQAuditor) audit(ctx context.Context, message types.Message) (bool, string, error) {
	messageID := aws.ToString(message.MessageId)
	if messageID == "" {
		return false, "", errors.New("dead-letter message has no broker message id")
	}
	body := aws.ToString(message.Body)
	digest := sha256.Sum256([]byte(body))
	messageHash := hex.EncodeToString(digest[:])
	queueName := path.Base("/" + strings.TrimSpace(queueNameFromURL(a.queueURL)))
	if queueName == "." || queueName == "/" {
		queueName = "wager-transactions-dlq.fifo"
	}

	var request domain.WagerRequest
	var providerID, externalID string
	disposition := "INVALID_MESSAGE"
	failureCode := FailureInvalidDeadLetterMessage
	var transactionID string
	var payloadHash string
	validRequest := false
	var envelope requestEnvelope
	if json.Unmarshal([]byte(body), &envelope) == nil {
		request = envelope.Data
		providerID, externalID = request.ProviderID, request.ExternalTransactionID
		_, timestampErr := time.Parse(time.RFC3339Nano, envelope.OccurredAt)
		if envelope.Type == "WagerTransactionRequested" && envelope.MessageID != "" && timestampErr == nil && request.Validate() == nil {
			var err error
			payloadHash, err = hashRequest(request)
			if err != nil {
				return false, "", err
			}
			validRequest = true
			disposition = "NO_TRANSACTION"
			failureCode = FailureMaxRetriesExceeded
		}
	}

	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, "", err
	}
	defer tx.Rollback(ctx)

	var previousHash string
	var previousDisposition string
	err = tx.QueryRow(ctx, `SELECT payload_hash,disposition FROM dead_letter_messages WHERE queue_name=$1 AND message_id=$2 FOR UPDATE`, queueName, messageID).Scan(&previousHash, &previousDisposition)
	if err == nil {
		if previousHash != messageHash {
			failureCode = FailureDeadLetterMessageConflict
			disposition = "CONFLICTING_PAYLOAD"
			var priorTransactionID *string
			if err = tx.QueryRow(ctx, `SELECT transaction_id::text FROM dead_letter_messages WHERE queue_name=$1 AND message_id=$2`, queueName, messageID).Scan(&priorTransactionID); err != nil {
				return false, "", err
			}
			if _, err = tx.Exec(ctx, `UPDATE dead_letter_messages SET payload_hash=$1,transaction_id=$2,failure_code=$3,disposition=$4,received_at=now() WHERE queue_name=$5 AND message_id=$6`, messageHash, priorTransactionID, failureCode, disposition, queueName, messageID); err != nil {
				return false, "", err
			}
		}
		if err = tx.Commit(ctx); err != nil {
			return false, "", err
		}
		if previousHash == messageHash {
			return false, previousDisposition, nil
		}
		return false, disposition, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, "", err
	}

	if validRequest {
		var status string
		var storedHash string
		var walletID, playerID, currency string
		err = tx.QueryRow(ctx, `SELECT id::text,status,payload_hash,wallet_id::text,player_id::text,currency
			FROM wager_transactions WHERE origin='EXTERNAL' AND provider_id=$1 AND external_transaction_id=$2 FOR UPDATE`,
			providerID, externalID).Scan(&transactionID, &status, &storedHash, &walletID, &playerID, &currency)
		if errors.Is(err, pgx.ErrNoRows) {
			err = nil
		} else if err != nil {
			return false, "", err
		} else if storedHash != payloadHash {
			failureCode = FailureIdempotencyConflict
			disposition = "CONFLICTING_PAYLOAD"
			transactionID = ""
		} else if status == string(domain.StatusPending) || status == string(domain.StatusPendingReference) {
			var balance, version int64
			var walletCurrency string
			if err = tx.QueryRow(ctx, `SELECT balance_minor,version,currency FROM wallets WHERE id=$1 AND player_id=$2 FOR UPDATE`, walletID, playerID).Scan(&balance, &version, &walletCurrency); err != nil {
				return false, "", err
			}
			if walletCurrency != currency || walletCurrency != request.Money.Currency() {
				return false, "", domain.ErrCurrencyMismatch
			}
			transitioned, transitionErr := transitionWagerTransaction(ctx, tx, transactionID, request, domain.StatusFailed, FailureMaxRetriesExceeded)
			if transitionErr != nil {
				return false, "", transitionErr
			}
			if _, err = tx.Exec(ctx, `UPDATE wager_transactions SET status=$1,failure_code=$2,result_balance_minor=$3,wallet_version=$4,updated_at=$5 WHERE id=$6`, transitioned.Status(), FailureMaxRetriesExceeded, balance, version, transitioned.UpdatedAt(), transactionID); err != nil {
				return false, "", err
			}
			disposition = "FAILED"
			failureCode = FailureMaxRetriesExceeded
		} else {
			disposition = "TERMINAL"
			transactionID = ""
		}
	}

	inserted := false
	err = tx.QueryRow(ctx, `INSERT INTO dead_letter_messages(queue_name,message_id,payload_hash,provider_id,external_transaction_id,transaction_id,failure_code,disposition)
		VALUES ($1,$2,$3,NULLIF($4,''),NULLIF($5,''),NULLIF($6,'')::uuid,$7,$8) ON CONFLICT DO NOTHING RETURNING message_id`,
		queueName, messageID, messageHash, providerID, externalID, transactionID, failureCode, disposition).Scan(new(string))
	if err == nil {
		inserted = true
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return false, "", err
	} else {
		var existingHash, existingDisposition string
		if err = tx.QueryRow(ctx, `SELECT payload_hash,disposition FROM dead_letter_messages WHERE queue_name=$1 AND message_id=$2`, queueName, messageID).Scan(&existingHash, &existingDisposition); err != nil {
			return false, "", err
		}
		if existingHash != messageHash {
			failureCode = FailureDeadLetterMessageConflict
			disposition = "CONFLICTING_PAYLOAD"
			if _, err = tx.Exec(ctx, `UPDATE dead_letter_messages SET failure_code=$1,disposition=$2,received_at=now() WHERE queue_name=$3 AND message_id=$4`, failureCode, disposition, queueName, messageID); err != nil {
				return false, "", err
			}
		} else {
			disposition = existingDisposition
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return false, "", err
	}
	return inserted, disposition, nil
}

func queueNameFromURL(queueURL string) string {
	parsed, err := url.Parse(queueURL)
	if err != nil {
		return ""
	}
	return path.Base(parsed.Path)
}
