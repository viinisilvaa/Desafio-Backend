package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/desafio/wager-service/internal/domain"
	"github.com/desafio/wager-service/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestThreeIndependentProcessesCompeteForWallet(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set TEST_DATABASE_URL to run multi-process PostgreSQL concurrency test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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
	amount, _ := domain.ParseMoney("80.00", "BRL")

	type childProcess struct {
		command *exec.Cmd
		stdout  bytes.Buffer
		stderr  bytes.Buffer
	}
	children := make([]*childProcess, 3)
	for index := range children {
		request := domain.WagerRequest{
			ProviderID:            "three-process-provider",
			ExternalTransactionID: uuid.NewString(),
			IdempotencyKey:        uuid.NewString(),
			PlayerID:              playerID,
			WalletID:              wallet.ID,
			RoundID:               "round",
			GameID:                "game",
			Kind:                  domain.KindBet,
			Money:                 amount,
		}
		encoded, marshalErr := json.Marshal(request)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		child := &childProcess{command: exec.CommandContext(ctx, os.Args[0], "-test.run=^TestIndependentWagerProcessHelper$", "-test.v")}
		child.command.Stdout = &child.stdout
		child.command.Stderr = &child.stderr
		child.command.Env = append(os.Environ(),
			"WAGER_PROCESS_HELPER=1",
			"WAGER_PROCESS_DATABASE_URL="+databaseURL,
			"WAGER_PROCESS_REQUEST="+string(encoded),
		)
		if err = child.command.Start(); err != nil {
			t.Fatal(err)
		}
		children[index] = child
	}

	processed, rejected := 0, 0
	for _, child := range children {
		if err = child.command.Wait(); err != nil {
			t.Fatalf("worker process failed: %v\nstdout: %s\nstderr: %s", err, child.stdout.String(), child.stderr.String())
		}
		var result Result
		found := false
		for _, line := range strings.Split(child.stdout.String(), "\n") {
			if strings.HasPrefix(line, "WAGER_RESULT:") {
				if err = json.Unmarshal([]byte(strings.TrimPrefix(line, "WAGER_RESULT:")), &result); err != nil {
					t.Fatalf("decode child result: %v; stdout: %s", err, child.stdout.String())
				}
				found = true
			}
		}
		if !found {
			t.Fatalf("worker process did not return a result: %s", child.stdout.String())
		}
		switch result.Status {
		case "PROCESSED":
			processed++
		case "REJECTED":
			if result.FailureCode != FailureInsufficientFunds {
				t.Errorf("worker rejection code=%q, want %q", result.FailureCode, FailureInsufficientFunds)
			}
			rejected++
		default:
			t.Errorf("unexpected worker status %q", result.Status)
		}
	}
	if processed != 1 || rejected != 2 {
		t.Fatalf("three OS processes produced processed=%d rejected=%d, want 1/2", processed, rejected)
	}
	finalWallet, err := processor.GetWallet(ctx, wallet.ID)
	if err != nil {
		t.Fatal(err)
	}
	if finalWallet.Balance.MinorUnits() != 2000 || finalWallet.Version != 2 {
		t.Fatalf("final balance/version=%d/%d, want 2000/2", finalWallet.Balance.MinorUnits(), finalWallet.Version)
	}
	var entries int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM wallet_ledger WHERE wallet_id=$1`, wallet.ID).Scan(&entries); err != nil {
		t.Fatal(err)
	}
	if entries != 2 {
		t.Fatalf("ledger rows=%d, want OPENING plus one debit", entries)
	}
}

func TestIndependentWagerProcessHelper(t *testing.T) {
	if os.Getenv("WAGER_PROCESS_HELPER") != "1" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv("WAGER_PROCESS_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var request domain.WagerRequest
	if err = json.Unmarshal([]byte(os.Getenv("WAGER_PROCESS_REQUEST")), &request); err != nil {
		t.Fatal(err)
	}
	result, err := NewProcessor(pool, NewMetrics()).Process(ctx, request, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("WAGER_RESULT:%s\n", encoded)
}
