package app

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/desafio/wager-service/internal/domain"
	"github.com/desafio/wager-service/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestDLQAuditFailsOnlyMatchingPendingWager(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set TEST_DATABASE_URL to run DLQ audit integration test")
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
	processor := NewProcessor(pool, NewMetrics())
	playerID := uuid.NewString()
	initial, _ := domain.ParseMoney("100.00", "BRL")
	wallet, err := processor.CreateWallet(ctx, playerID, initial)
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := domain.ParseMoney("20.00", "BRL")
	request := domain.WagerRequest{ProviderID: "dlq-provider", ExternalTransactionID: uuid.NewString(), IdempotencyKey: uuid.NewString(), PlayerID: playerID, WalletID: wallet.ID, RoundID: "round", GameID: "game", Kind: domain.KindBet, Money: amount}
	payloadHash, err := hashRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	initialTransaction, err := domain.NewWagerTransaction(uuid.NewString(), request, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	transactionID, inserted, err := insertExternalTransaction(ctx, tx, request, payloadHash, initialTransaction)
	if err != nil || !inserted {
		_ = tx.Rollback(ctx)
		t.Fatalf("insert pending fixture: inserted=%v err=%v", inserted, err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	envelope := requestEnvelope{MessageID: uuid.NewString(), Type: "WagerTransactionRequested", OccurredAt: time.Now().UTC().Format(time.RFC3339Nano), Data: request}
	body, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	message := types.Message{MessageId: aws.String("dlq-broker-message-1"), Body: aws.String(string(body))}
	auditor := &DLQAuditor{pool: pool, queueURL: "http://localstack:4566/000000000000/wager-transactions-dlq.fifo"}
	created, disposition, err := auditor.audit(ctx, message)
	if err != nil || !created || disposition != "FAILED" {
		t.Fatalf("DLQ audit failed: created=%v disposition=%s err=%v", created, disposition, err)
	}
	var status, failureCode string
	var resultBalance, walletVersion int64
	if err = pool.QueryRow(ctx, `SELECT status,failure_code,result_balance_minor,wallet_version FROM wager_transactions WHERE id=$1`, transactionID).Scan(&status, &failureCode, &resultBalance, &walletVersion); err != nil {
		t.Fatal(err)
	}
	if status != string(domain.StatusFailed) || failureCode != FailureMaxRetriesExceeded || resultBalance != initial.MinorUnits() || walletVersion != 1 {
		t.Fatalf("unexpected terminal wager: status=%s failure=%s balance=%d version=%d", status, failureCode, resultBalance, walletVersion)
	}
	finalWallet, err := processor.GetWallet(ctx, wallet.ID)
	if err != nil {
		t.Fatal(err)
	}
	if finalWallet.Balance.MinorUnits() != initial.MinorUnits() || finalWallet.Version != 1 {
		t.Fatalf("DLQ audit changed wallet: balance=%d version=%d", finalWallet.Balance.MinorUnits(), finalWallet.Version)
	}
	var deadLetterCount, ledgerCount int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM dead_letter_messages WHERE queue_name=$1 AND message_id=$2`, "wager-transactions-dlq.fifo", "dlq-broker-message-1").Scan(&deadLetterCount); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM wallet_ledger WHERE wallet_id=$1`, wallet.ID).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if deadLetterCount != 1 || ledgerCount != 1 {
		t.Fatalf("audit rows=%d ledger rows=%d, want 1/1 (opening only)", deadLetterCount, ledgerCount)
	}
	if created, disposition, err = auditor.audit(ctx, message); err != nil || created || disposition != "FAILED" {
		t.Fatalf("DLQ replay was not idempotent: created=%v disposition=%s err=%v", created, disposition, err)
	}
	message.Body = aws.String(`{"broken":true}`)
	if created, disposition, err = auditor.audit(ctx, message); err != nil || created || disposition != "CONFLICTING_PAYLOAD" {
		t.Fatalf("messageId/body conflict was not recorded: created=%v disposition=%s err=%v", created, disposition, err)
	}
	unknownRequest := request
	unknownRequest.ExternalTransactionID = uuid.NewString()
	unknownRequest.IdempotencyKey = uuid.NewString()
	unknownEnvelope := requestEnvelope{MessageID: uuid.NewString(), Type: "WagerTransactionRequested", OccurredAt: time.Now().UTC().Format(time.RFC3339Nano), Data: unknownRequest}
	unknownBody, err := json.Marshal(unknownEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	message.MessageId = aws.String("dlq-broker-message-unknown")
	message.Body = aws.String(string(unknownBody))
	if created, disposition, err = auditor.audit(ctx, message); err != nil || !created || disposition != "NO_TRANSACTION" {
		t.Fatalf("DLQ record with no wager should be quarantined: created=%v disposition=%s err=%v", created, disposition, err)
	}
	if finalWallet, err = processor.GetWallet(ctx, wallet.ID); err != nil || finalWallet.Balance.MinorUnits() != initial.MinorUnits() || finalWallet.Version != 1 {
		t.Fatalf("unknown DLQ message changed wallet: wallet=%+v err=%v", finalWallet, err)
	}
}
