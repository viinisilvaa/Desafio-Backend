package app

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/desafio/wager-service/internal/domain"
	"github.com/desafio/wager-service/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestIndependentProcessorsPreserveWalletInvariants(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL concurrency integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := migrations.Run(ctx, databaseURL, "up"); err != nil {
		t.Fatal(err)
	}
	processors := make([]*Processor, 3)
	pools := make([]*pgxpool.Pool, 3)
	for index := range pools {
		pool, err := pgxpool.New(ctx, databaseURL)
		if err != nil {
			t.Fatal(err)
		}
		pools[index] = pool
		processors[index] = NewProcessor(pool, NewMetrics())
	}
	defer func() {
		for _, pool := range pools {
			pool.Close()
		}
	}()

	playerID := uuid.NewString()
	initial, _ := domain.ParseMoney("100.00", "BRL")
	wallet, err := processors[0].CreateWallet(ctx, playerID, initial)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pools[0].Exec(ctx, `INSERT INTO wager_transactions(origin,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount_minor,currency,status)
		VALUES ('EXTERNAL','schema-owner-check',$1,$2,repeat('a',64),$3,$4,'round','game','BET',1,'BRL','PENDING')`, uuid.NewString(), uuid.NewString(), wallet.ID, uuid.NewString())
	if err == nil {
		t.Fatal("database accepted a wager whose player does not own its wallet")
	}
	amount, _ := domain.ParseMoney("25.00", "BRL")
	request := domain.WagerRequest{ProviderID: "integration-provider", ExternalTransactionID: "same-" + uuid.NewString(), IdempotencyKey: "same-key-" + uuid.NewString(), PlayerID: playerID, WalletID: wallet.ID, RoundID: "round", GameID: "game", Kind: domain.KindBet, Money: amount}
	var start sync.WaitGroup
	start.Add(1)
	results := make(chan Result, 50)
	errorsFound := make(chan error, 50)
	var calls sync.WaitGroup
	for index := range 50 {
		calls.Add(1)
		go func(index int) {
			defer calls.Done()
			start.Wait()
			result, processErr := processors[index%len(processors)].Process(ctx, request, "", "", "")
			if processErr != nil {
				errorsFound <- processErr
				return
			}
			results <- result
		}(index)
	}
	start.Done()
	calls.Wait()
	close(results)
	close(errorsFound)
	for processErr := range errorsFound {
		t.Errorf("idempotent replay failed: %v", processErr)
	}
	firstExecution := 0
	for result := range results {
		if result.Status != "PROCESSED" {
			t.Errorf("unexpected status %s", result.Status)
		}
		if !result.IdempotentReplay {
			firstExecution++
		}
	}
	if firstExecution != 1 {
		t.Fatalf("got %d first executions, want exactly one", firstExecution)
	}
	changedPayload := request
	changedPayload.GameID = "different-game"
	if _, err := processors[0].Process(ctx, changedPayload, "", "", ""); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed payload with same idempotency key should conflict, got %v", err)
	}
	messageID := uuid.NewString()
	messageHash := strings.Repeat("a", 64)
	messageReplay, err := processors[1].Process(ctx, request, "integration-inbox", messageID, messageHash)
	if err != nil || !messageReplay.IdempotentReplay {
		t.Fatalf("inbox delivery of existing transaction should replay: result=%+v err=%v", messageReplay, err)
	}
	if _, err = processors[2].Process(ctx, request, "integration-inbox", messageID, strings.Repeat("b", 64)); !errors.Is(err, ErrMessageConflict) {
		t.Fatalf("same inbox ID with changed message hash should conflict, got %v", err)
	}
	var ledgerCount int
	if err := pools[0].QueryRow(ctx, `SELECT count(*) FROM wallet_ledger WHERE wallet_id=$1`, wallet.ID).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if ledgerCount != 2 {
		t.Fatalf("got %d ledger rows after 50 replays, want opening plus one debit", ledgerCount)
	}

	playerID = uuid.NewString()
	wallet, err = processors[0].CreateWallet(ctx, playerID, initial)
	if err != nil {
		t.Fatal(err)
	}
	largeBet, _ := domain.ParseMoney("80.00", "BRL")
	var twoCalls sync.WaitGroup
	start = sync.WaitGroup{}
	start.Add(1)
	resultsTwo := make(chan Result, 2)
	errorsTwo := make(chan error, 2)
	for index := range 2 {
		twoCalls.Add(1)
		go func(index int) {
			defer twoCalls.Done()
			start.Wait()
			request := domain.WagerRequest{ProviderID: "integration-provider", ExternalTransactionID: uuid.NewString(), IdempotencyKey: uuid.NewString(), PlayerID: playerID, WalletID: wallet.ID, RoundID: "round", GameID: "game", Kind: domain.KindBet, Money: largeBet}
			result, processErr := processors[index].Process(ctx, request, "", "", "")
			if processErr != nil {
				errorsTwo <- processErr
				return
			}
			resultsTwo <- result
		}(index)
	}
	start.Done()
	twoCalls.Wait()
	close(resultsTwo)
	close(errorsTwo)
	for processErr := range errorsTwo {
		t.Errorf("concurrent wager failed: %v", processErr)
	}
	processed, rejected := 0, 0
	for result := range resultsTwo {
		switch result.Status {
		case "PROCESSED":
			processed++
		case "REJECTED":
			if result.FailureCode != FailureInsufficientFunds {
				t.Errorf("rejection code = %q, want %s", result.FailureCode, FailureInsufficientFunds)
			}
			rejected++
		default:
			t.Errorf("unexpected concurrent status %s", result.Status)
		}
	}
	if processed != 1 || rejected != 1 {
		t.Fatalf("processed=%d rejected=%d, want one each", processed, rejected)
	}
	finalWallet, err := processors[1].GetWallet(ctx, wallet.ID)
	if err != nil {
		t.Fatal(err)
	}
	if finalWallet.Balance.MinorUnits() != 2000 || finalWallet.Version != 2 {
		t.Fatalf("final balance/version=%d/%d, want 2000/2", finalWallet.Balance.MinorUnits(), finalWallet.Version)
	}
	if err = pools[2].QueryRow(ctx, `SELECT count(*) FROM wallet_ledger WHERE wallet_id=$1`, wallet.ID).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if ledgerCount != 2 {
		t.Fatalf("got %d ledger rows including opening, want 2", ledgerCount)
	}
	reconciliation, err := processors[1].Reconcile(ctx, wallet.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reconciliation.Consistent || reconciliation.CheckedEntries != 2 || reconciliation.Difference.MinorUnits() != 0 {
		t.Fatalf("unexpected wallet reconciliation: %+v", reconciliation)
	}
	if _, err = pools[2].Exec(ctx, `UPDATE wallet_ledger SET amount_minor=amount_minor WHERE wallet_id=$1`, wallet.ID); err == nil {
		t.Fatal("ledger UPDATE unexpectedly succeeded")
	}

	playerID = uuid.NewString()
	wallet, err = processors[0].CreateWallet(ctx, playerID, initial)
	if err != nil {
		t.Fatal(err)
	}
	ten, _ := domain.ParseMoney("10.00", "BRL")
	provider := "reference-" + uuid.NewString()
	betExternalID := "bet-" + uuid.NewString()
	refund := domain.WagerRequest{ProviderID: provider, ExternalTransactionID: "refund-" + uuid.NewString(), IdempotencyKey: uuid.NewString(), PlayerID: playerID, WalletID: wallet.ID, RoundID: "round", GameID: "game", Kind: domain.KindRefund, Money: ten, ReferenceExternalTransactionID: betExternalID}
	pendingResult, err := processors[1].Process(ctx, refund, "", "", "")
	if err != nil || pendingResult.Status != "PENDING_REFERENCE" {
		t.Fatalf("refund before BET should be pending, result=%+v err=%v", pendingResult, err)
	}
	bet := domain.WagerRequest{ProviderID: provider, ExternalTransactionID: betExternalID, IdempotencyKey: uuid.NewString(), PlayerID: playerID, WalletID: wallet.ID, RoundID: "round", GameID: "game", Kind: domain.KindBet, Money: ten}
	if result, processErr := processors[2].Process(ctx, bet, "", "", ""); processErr != nil || result.Status != "PROCESSED" {
		t.Fatalf("referenced BET failed: result=%+v err=%v", result, processErr)
	}
	referenceWorker := &ReferenceWorker{pool: pools[0], processor: processors[0], metrics: NewMetrics()}
	if worked, processErr := referenceWorker.processOne(ctx); processErr != nil || !worked {
		t.Fatalf("pending refund worker did not process: worked=%v err=%v", worked, processErr)
	}
	resolved, err := processors[1].GetTransaction(ctx, provider, refund.ExternalTransactionID)
	if err != nil || resolved.Status != "PROCESSED" || resolved.Balance.MinorUnits() != 10000 {
		t.Fatalf("pending refund was not resolved: result=%+v err=%v", resolved, err)
	}

	playerID = uuid.NewString()
	wallet, err = processors[0].CreateWallet(ctx, playerID, initial)
	if err != nil {
		t.Fatal(err)
	}
	provider = "reversal-" + uuid.NewString()
	betExternalID = "bet-" + uuid.NewString()
	bet = domain.WagerRequest{ProviderID: provider, ExternalTransactionID: betExternalID, IdempotencyKey: uuid.NewString(), PlayerID: playerID, WalletID: wallet.ID, RoundID: "round", GameID: "game", Kind: domain.KindBet, Money: largeBet}
	if result, processErr := processors[0].Process(ctx, bet, "", "", ""); processErr != nil || result.Status != "PROCESSED" {
		t.Fatalf("reversal fixture BET failed: result=%+v err=%v", result, processErr)
	}
	refund = domain.WagerRequest{ProviderID: provider, ExternalTransactionID: "refund-" + uuid.NewString(), IdempotencyKey: uuid.NewString(), PlayerID: playerID, WalletID: wallet.ID, RoundID: "round", GameID: "game", Kind: domain.KindRefund, Money: largeBet, ReferenceExternalTransactionID: betExternalID}
	if result, processErr := processors[1].Process(ctx, refund, "", "", ""); processErr != nil || result.Status != "PROCESSED" {
		t.Fatalf("fixture REFUND failed: result=%+v err=%v", result, processErr)
	}
	duplicateRefund := refund
	duplicateRefund.ExternalTransactionID = "refund-duplicate-" + uuid.NewString()
	duplicateRefund.IdempotencyKey = uuid.NewString()
	if result, processErr := processors[2].Process(ctx, duplicateRefund, "", "", ""); processErr != nil || result.Status != "REJECTED" || result.FailureCode != FailureReferenceAlreadyReversed {
		t.Fatalf("second reversal should be rejected: result=%+v err=%v", result, processErr)
	}
	rollback := domain.WagerRequest{ProviderID: provider, ExternalTransactionID: "rollback-" + uuid.NewString(), IdempotencyKey: uuid.NewString(), PlayerID: playerID, WalletID: wallet.ID, RoundID: "round", GameID: "game", Kind: domain.KindRollback, Money: largeBet, ReferenceExternalTransactionID: refund.ExternalTransactionID}
	if result, processErr := processors[0].Process(ctx, rollback, "", "", ""); processErr != nil || result.Status != "PROCESSED" || result.Balance.MinorUnits() != 2000 {
		t.Fatalf("rollback of REFUND should debit back to 20.00: result=%+v err=%v", result, processErr)
	}
}
