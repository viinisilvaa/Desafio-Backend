package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/desafio/wager-service/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrIdempotencyConflict = errors.New("idempotency key or external transaction reused with different content")
	ErrMessageConflict     = errors.New("message id reused with different content")
	ErrWalletNotFound      = errors.New("wallet not found or does not belong to player")
)

type Processor struct {
	pool    *pgxpool.Pool
	metrics *Metrics
}

func NewProcessor(pool *pgxpool.Pool, metrics *Metrics) *Processor {
	return &Processor{pool: pool, metrics: metrics}
}

type Result struct {
	TransactionID    string       `json:"transactionId"`
	Status           string       `json:"status"`
	Balance          domain.Money `json:"balance"`
	WalletVersion    int64        `json:"walletVersion"`
	FailureCode      string       `json:"failureCode,omitempty"`
	IdempotentReplay bool         `json:"idempotentReplay"`
}

func (p *Processor) Process(ctx context.Context, request domain.WagerRequest, consumerName, messageID, messageHash string) (finalResult Result, finalErr error) {
	started := time.Now()
	defer func() {
		if p.metrics != nil {
			p.metrics.recordProcess(finalResult, finalErr, uint64(time.Since(started)))
		}
	}()
	initialTransaction, err := domain.NewWagerTransaction(uuid.NewString(), request, time.Now().UTC())
	if err != nil {
		return Result{}, err
	}
	if messageID == "" && (consumerName != "" || messageHash != "") {
		return Result{}, fmt.Errorf("message identity is incomplete")
	}
	if messageID != "" && (consumerName == "" || messageHash == "") {
		return Result{}, fmt.Errorf("message identity is incomplete")
	}
	payloadHash, err := hashRequest(request)
	if err != nil {
		return Result{}, err
	}
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback(ctx)

	if messageID != "" {
		var inserted string
		err = tx.QueryRow(ctx, `
			INSERT INTO inbox(consumer_name, message_id, payload_hash)
			VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING message_id`,
			consumerName, messageID, messageHash,
		).Scan(&inserted)
		if errors.Is(err, pgx.ErrNoRows) {
			var priorHash string
			if err = tx.QueryRow(ctx, `SELECT payload_hash FROM inbox WHERE consumer_name=$1 AND message_id=$2`, consumerName, messageID).Scan(&priorHash); err != nil {
				return Result{}, err
			}
			if priorHash != messageHash {
				return Result{}, ErrMessageConflict
			}
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return Result{}, err
		}
	}

	transactionID, inserted, err := insertExternalTransaction(ctx, tx, request, payloadHash, initialTransaction)
	if err != nil {
		return Result{}, err
	}
	if !inserted {
		result, loadErr := loadResult(ctx, tx, transactionID, true)
		if loadErr != nil {
			return Result{}, loadErr
		}
		if messageID != "" {
			if _, err = tx.Exec(ctx, `UPDATE inbox SET completed_at=now() WHERE consumer_name=$1 AND message_id=$2`, consumerName, messageID); err != nil {
				return Result{}, err
			}
		}
		if err = tx.Commit(ctx); err != nil {
			return Result{}, err
		}
		return result, nil
	}

	var balanceBefore int64
	var walletVersion int64
	var walletCurrency string
	err = tx.QueryRow(ctx, `
		SELECT balance_minor, version, currency FROM wallets
		WHERE id=$1 AND player_id=$2 FOR UPDATE`, request.WalletID, request.PlayerID,
	).Scan(&balanceBefore, &walletVersion, &walletCurrency)
	if errors.Is(err, pgx.ErrNoRows) {
		return Result{}, ErrWalletNotFound
	}
	if err != nil {
		return Result{}, err
	}
	if walletCurrency != request.Money.Currency() {
		return Result{}, domain.ErrCurrencyMismatch
	}
	if messageID != "" {
		// The message receipt and all financial effects commit together.
	}

	if request.Kind == domain.KindRefund || request.Kind == domain.KindRollback || (request.Kind == domain.KindWin && request.ReferenceExternalTransactionID != "") {
		ref, pending, failureCode, refErr := resolveReference(ctx, tx, request)
		if refErr != nil {
			return Result{}, refErr
		}
		if pending {
			transitioned, transitionErr := transitionWagerTransaction(ctx, tx, transactionID, request, domain.StatusPendingReference, "")
			if transitionErr != nil {
				return Result{}, transitionErr
			}
			if _, err = tx.Exec(ctx, `UPDATE wager_transactions SET status=$1, result_balance_minor=$2, wallet_version=$3, updated_at=$4 WHERE id=$5`, transitioned.Status(), balanceBefore, walletVersion, transitioned.UpdatedAt(), transactionID); err != nil {
				return Result{}, err
			}
			if err = insertEvent(ctx, tx, newPendingReferenceEvent(uuid.NewString(), request.WalletID, request.ExternalTransactionID, transactionID, request.ProviderID, request.ReferenceExternalTransactionID, walletVersion)); err != nil {
				return Result{}, err
			}
			if messageID != "" {
				_, err = tx.Exec(ctx, `UPDATE inbox SET completed_at=now() WHERE consumer_name=$1 AND message_id=$2`, consumerName, messageID)
				if err != nil {
					return Result{}, err
				}
			}
			if err = tx.Commit(ctx); err != nil {
				return Result{}, err
			}
			balance, _ := domain.ParseMoney(formatMinor(balanceBefore), walletCurrency)
			return Result{TransactionID: transactionID, Status: "PENDING_REFERENCE", Balance: balance, WalletVersion: walletVersion}, nil
		}
		if failureCode != "" {
			result, rejectErr := p.reject(ctx, tx, request, transactionID, balanceBefore, walletVersion, walletCurrency, failureCode, consumerName, messageID)
			if rejectErr != nil {
				return Result{}, rejectErr
			}
			if err = tx.Commit(ctx); err != nil {
				return Result{}, err
			}
			return result, nil
		}
		if err = storeReference(ctx, tx, transactionID, ref); err != nil {
			return Result{}, err
		}
	}

	result, err := p.apply(ctx, tx, request, transactionID, balanceBefore, walletVersion, walletCurrency, consumerName, messageID)
	if err != nil {
		return Result{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Result{}, err
	}
	return result, nil
}

func hashRequest(request domain.WagerRequest) (string, error) {
	amount, err := request.Money.String()
	if err != nil {
		return "", err
	}
	canonical := struct {
		ProviderID            string           `json:"providerId"`
		ExternalTransactionID string           `json:"externalTransactionId"`
		PlayerID              string           `json:"playerId"`
		WalletID              string           `json:"walletId"`
		RoundID               string           `json:"roundId"`
		GameID                string           `json:"gameId"`
		Kind                  domain.WagerKind `json:"kind"`
		Amount                string           `json:"amount"`
		Currency              string           `json:"currency"`
		Reference             string           `json:"referenceExternalTransactionId,omitempty"`
	}{request.ProviderID, request.ExternalTransactionID, request.PlayerID, request.WalletID, request.RoundID, request.GameID, request.Kind, amount, request.Money.Currency(), request.ReferenceExternalTransactionID}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func insertExternalTransaction(ctx context.Context, tx pgx.Tx, request domain.WagerRequest, payloadHash string, initial domain.WagerTransaction) (string, bool, error) {
	var id string
	err := tx.QueryRow(ctx, `
		INSERT INTO wager_transactions(id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash,
			wallet_id, player_id, round_id, game_id, kind, amount_minor, currency, reference_external_transaction_id, status)
		VALUES ($1,'EXTERNAL',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NULLIF($13,''),$14)
		ON CONFLICT DO NOTHING RETURNING id::text`, initial.ID(), request.ProviderID, request.ExternalTransactionID,
		request.IdempotencyKey, payloadHash, request.WalletID, request.PlayerID, request.RoundID, request.GameID,
		request.Kind, request.Money.MinorUnits(), request.Money.Currency(), request.ReferenceExternalTransactionID, initial.Status(),
	).Scan(&id)
	if err == nil {
		return id, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", false, err
	}
	var existing struct {
		ID   string
		Key  string
		Hash string
	}
	err = tx.QueryRow(ctx, `SELECT id::text, idempotency_key, payload_hash FROM wager_transactions
		WHERE origin='EXTERNAL' AND provider_id=$1 AND (idempotency_key=$2 OR external_transaction_id=$3)
		ORDER BY id LIMIT 1`, request.ProviderID, request.IdempotencyKey, request.ExternalTransactionID).Scan(&existing.ID, &existing.Key, &existing.Hash)
	if err != nil {
		return "", false, err
	}
	if existing.Key != request.IdempotencyKey || existing.Hash != payloadHash {
		return "", false, ErrIdempotencyConflict
	}
	return existing.ID, false, nil
}

func (p *Processor) apply(ctx context.Context, tx pgx.Tx, request domain.WagerRequest, transactionID string, before, version int64, currency, consumer, messageID string) (Result, error) {
	after := before
	direction := ""
	financialKind := request.Kind
	switch request.Kind {
	case domain.KindBet:
		direction = "DEBIT"
	case domain.KindWin, domain.KindRefund:
		direction = "CREDIT"
	case domain.KindRollback:
		var referenceKind string
		if err := tx.QueryRow(ctx, `SELECT kind FROM wager_transactions WHERE id=(SELECT reference_transaction_id FROM wager_transactions WHERE id=$1)`, transactionID).Scan(&referenceKind); err != nil {
			return Result{}, err
		}
		if referenceKind == string(domain.KindBet) {
			direction = "CREDIT"
			financialKind = domain.KindWin
		} else {
			direction = "DEBIT"
			financialKind = domain.KindBet
		}
	}
	if direction != "" {
		current, err := domain.ParseMoney(formatMinor(before), currency)
		if err != nil {
			return Result{}, err
		}
		// Rehydrate through the domain so the in-memory transition mirrors the SQL lock.
		wallet, err := domain.RehydrateWallet(request.WalletID, request.PlayerID, current, version)
		if err != nil {
			return Result{}, err
		}
		updatedWallet, next, err := wallet.Apply(financialKind, request.Money)
		if err != nil {
			if errors.Is(err, domain.ErrInsufficientFunds) {
				code := FailureInsufficientFunds
				if request.Kind == domain.KindRollback {
					code = FailureReversalInsufficientFunds
				}
				return p.reject(ctx, tx, request, transactionID, before, version, currency, code, consumer, messageID)
			}
			return Result{}, err
		}
		after = next.MinorUnits()
		if _, err = tx.Exec(ctx, `UPDATE wallets SET balance_minor=$1, version=$2, updated_at=now() WHERE id=$3`, after, updatedWallet.Version(), request.WalletID); err != nil {
			return Result{}, err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO wallet_ledger(wallet_id, transaction_id, direction, amount_minor, currency, wallet_version, balance_before_minor, balance_after_minor) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, request.WalletID, transactionID, direction, request.Money.MinorUnits(), currency, updatedWallet.Version(), before, after); err != nil {
			return Result{}, err
		}
		version = updatedWallet.Version()
	}
	transitioned, err := transitionWagerTransaction(ctx, tx, transactionID, request, domain.StatusProcessed, "")
	if err != nil {
		return Result{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE wager_transactions SET status=$1, result_balance_minor=$2, wallet_version=$3, updated_at=$4 WHERE id=$5`, transitioned.Status(), after, version, transitioned.UpdatedAt(), transactionID); err != nil {
		return Result{}, err
	}
	if messageID != "" {
		if _, err := tx.Exec(ctx, `UPDATE inbox SET completed_at=now() WHERE consumer_name=$1 AND message_id=$2`, consumer, messageID); err != nil {
			return Result{}, err
		}
	}
	if err := insertEvent(ctx, tx, newProcessedEvent(uuid.NewString(), request.WalletID, request.ExternalTransactionID, transactionID, request.ProviderID, request.ExternalTransactionID, string(request.Kind), version)); err != nil {
		return Result{}, err
	}
	if direction != "" {
		if err := insertEvent(ctx, tx, newBalanceChangedEvent(uuid.NewString(), request.WalletID, request.ExternalTransactionID, transactionID, direction, request.Money, moneyValue(before, currency), moneyValue(after, currency), version)); err != nil {
			return Result{}, err
		}
	}
	return Result{TransactionID: transactionID, Status: "PROCESSED", Balance: moneyValue(after, currency), WalletVersion: version}, nil
}

func (p *Processor) reject(ctx context.Context, tx pgx.Tx, request domain.WagerRequest, transactionID string, balance, version int64, currency, code, consumer, messageID string) (Result, error) {
	transitioned, err := transitionWagerTransaction(ctx, tx, transactionID, request, domain.StatusRejected, code)
	if err != nil {
		return Result{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE wager_transactions SET status=$1, failure_code=$2, result_balance_minor=$3, wallet_version=$4, updated_at=$5 WHERE id=$6`, transitioned.Status(), code, balance, version, transitioned.UpdatedAt(), transactionID); err != nil {
		return Result{}, err
	}
	if messageID != "" {
		if _, err := tx.Exec(ctx, `UPDATE inbox SET completed_at=now() WHERE consumer_name=$1 AND message_id=$2`, consumer, messageID); err != nil {
			return Result{}, err
		}
	}
	if err := insertEvent(ctx, tx, newRejectedEvent(uuid.NewString(), request.WalletID, request.ExternalTransactionID, transactionID, request.ProviderID, request.ExternalTransactionID, code, version)); err != nil {
		return Result{}, err
	}
	return Result{TransactionID: transactionID, Status: "REJECTED", Balance: moneyValue(balance, currency), WalletVersion: version, FailureCode: code}, nil
}

func transitionWagerTransaction(ctx context.Context, tx pgx.Tx, id string, request domain.WagerRequest, next domain.TransactionStatus, failureCode string) (domain.WagerTransaction, error) {
	var current domain.TransactionStatus
	var previousFailure string
	var createdAt, updatedAt, now time.Time
	err := tx.QueryRow(ctx, `SELECT status,COALESCE(failure_code,''),created_at,updated_at,clock_timestamp() FROM wager_transactions WHERE id=$1 FOR UPDATE`, id).Scan(&current, &previousFailure, &createdAt, &updatedAt, &now)
	if err != nil {
		return domain.WagerTransaction{}, err
	}
	transaction, err := domain.RehydrateWagerTransaction(id, request, current, previousFailure, createdAt, updatedAt)
	if err != nil {
		return domain.WagerTransaction{}, err
	}
	switch next {
	case domain.StatusPendingReference:
		return transaction.MarkPendingReference(now)
	case domain.StatusProcessed:
		return transaction.MarkProcessed(now)
	case domain.StatusRejected:
		return transaction.Reject(failureCode, now)
	case domain.StatusFailed:
		return transaction.Fail(failureCode, now)
	default:
		return domain.WagerTransaction{}, domain.ErrInvalidWager
	}
}

type reference struct {
	ID, Kind, Status, PlayerID, WalletID, RoundID, Currency string
	Amount                                                  int64
	Reversed                                                bool
}

func resolveReference(ctx context.Context, tx pgx.Tx, request domain.WagerRequest) (reference, bool, string, error) {
	var ref reference
	err := tx.QueryRow(ctx, `SELECT t.id::text, t.kind, t.status, t.player_id::text, t.wallet_id::text, t.round_id, t.currency, t.amount_minor,
		EXISTS(SELECT 1 FROM wager_transactions reversal WHERE reversal.reference_transaction_id=t.id AND reversal.kind IN ('REFUND','ROLLBACK') AND reversal.status='PROCESSED')
		FROM wager_transactions t WHERE t.origin='EXTERNAL' AND t.provider_id=$1 AND t.external_transaction_id=$2`,
		request.ProviderID, request.ReferenceExternalTransactionID).Scan(&ref.ID, &ref.Kind, &ref.Status, &ref.PlayerID, &ref.WalletID, &ref.RoundID, &ref.Currency, &ref.Amount, &ref.Reversed)
	if errors.Is(err, pgx.ErrNoRows) {
		return reference{}, true, "", nil
	}
	if err != nil {
		return reference{}, false, "", err
	}
	if ref.Status == "PENDING" || ref.Status == "PENDING_REFERENCE" {
		return reference{}, true, FailureReferenceNotProcessed, nil
	}
	if ref.Status != "PROCESSED" {
		return reference{}, false, FailureReferenceNotProcessed, nil
	}
	if ref.Reversed {
		return reference{}, false, FailureReferenceAlreadyReversed, nil
	}
	if ref.PlayerID != request.PlayerID || ref.WalletID != request.WalletID || ref.RoundID != request.RoundID || ref.Currency != request.Money.Currency() {
		return reference{}, false, FailureReferenceMismatch, nil
	}
	if request.Kind == domain.KindWin {
		if ref.Kind != string(domain.KindBet) {
			return reference{}, false, FailureReferenceKindInvalid, nil
		}
		return ref, false, "", nil
	}
	if ref.Amount != request.Money.MinorUnits() {
		return reference{}, false, FailureReferenceMismatch, nil
	}
	if request.Kind == domain.KindRefund && ref.Kind != string(domain.KindBet) {
		return reference{}, false, FailureReferenceKindInvalid, nil
	}
	if request.Kind == domain.KindRollback && ref.Kind != string(domain.KindBet) && ref.Kind != string(domain.KindWin) && ref.Kind != string(domain.KindRefund) {
		return reference{}, false, FailureReferenceKindInvalid, nil
	}
	return ref, false, "", nil
}

func storeReference(ctx context.Context, tx pgx.Tx, transactionID string, ref reference) error {
	_, err := tx.Exec(ctx, `UPDATE wager_transactions SET reference_transaction_id=$1 WHERE id=$2`, ref.ID, transactionID)
	return err
}

func loadResult(ctx context.Context, tx pgx.Tx, transactionID string, replay bool) (Result, error) {
	var result Result
	var balance *int64
	var currency string
	err := tx.QueryRow(ctx, `SELECT id::text,status,result_balance_minor,wallet_version,failure_code,currency FROM wager_transactions WHERE id=$1`, transactionID).Scan(&result.TransactionID, &result.Status, &balance, &result.WalletVersion, &result.FailureCode, &currency)
	if err != nil {
		return Result{}, err
	}
	if balance != nil {
		result.Balance = moneyValue(*balance, currency)
	} else {
		result.Balance, _ = domain.ZeroMoney(currency)
	}
	result.IdempotentReplay = replay
	return result, nil
}

type eventEnvelope struct {
	EventID       string `json:"eventId"`
	EventType     string `json:"eventType"`
	AggregateID   string `json:"aggregateId"`
	CorrelationID string `json:"correlationId"`
	CausationID   string `json:"causationId,omitempty"`
	OccurredAt    string `json:"occurredAt"`
	Version       int64  `json:"version"`
	Data          any    `json:"data"`
}

func insertEvent(ctx context.Context, tx pgx.Tx, event eventEnvelope) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox(event_id, aggregate_id, event_type, correlation_id, causation_id, occurred_at, version, payload) VALUES ($1,$2,$3,$4,NULLIF($5,''),$6,$7,$8)`, event.EventID, event.AggregateID, event.EventType, event.CorrelationID, event.CausationID, event.OccurredAt, event.Version, payload)
	return err
}

func moneyValue(minor int64, currency string) domain.Money {
	amount, err := domain.ParseMoney(formatMinor(minor), currency)
	if err != nil {
		return domain.Money{}
	}
	return amount
}

func formatMinor(minor int64) string {
	negative := minor < 0
	var magnitude uint64
	if negative {
		magnitude = uint64(-(minor + 1)) + 1
	} else {
		magnitude = uint64(minor)
	}
	value := fmt.Sprintf("%d.%02d", magnitude/100, magnitude%100)
	if negative {
		return "-" + value
	}
	return value
}

func IsInfrastructureError(err error) bool {
	return err != nil && !errors.Is(err, domain.ErrInvalidWager) && !errors.Is(err, domain.ErrCurrencyMismatch) &&
		!errors.Is(err, domain.ErrInsufficientFunds) && !errors.Is(err, ErrIdempotencyConflict) &&
		!errors.Is(err, ErrWalletNotFound)
}
