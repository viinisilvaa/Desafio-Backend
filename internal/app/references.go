package app

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/desafio/wager-service/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const maxReferenceAttempts = 20

type ReferenceWorker struct {
	pool      *pgxpool.Pool
	processor *Processor
	logger    *slog.Logger
	metrics   *Metrics
}

func NewReferenceWorker(pool *pgxpool.Pool, processor *Processor, logger *slog.Logger, metrics *Metrics) *ReferenceWorker {
	return &ReferenceWorker{pool: pool, processor: processor, logger: logger, metrics: metrics}
}

func (w *ReferenceWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		worked := false
		for range 20 {
			if ctx.Err() != nil {
				return
			}
			processed, err := w.processOne(ctx)
			if err != nil {
				w.metrics.retries.Add(1)
				w.logger.Error("pending reference retry failed", "error", err)
				break
			}
			if !processed {
				break
			}
			worked = true
		}
		if !worked {
			select {
			case <-ctx.Done():
			case <-ticker.C:
			}
		}
	}
}

func (w *ReferenceWorker) processOne(ctx context.Context) (bool, error) {
	tx, err := w.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var request domain.WagerRequest
	var id string
	var attempts int
	var createdAt time.Time
	var currency string
	var amount int64
	err = tx.QueryRow(ctx, `SELECT id::text,provider_id,external_transaction_id,idempotency_key,player_id::text,wallet_id::text,round_id,game_id,kind,amount_minor,currency,COALESCE(reference_external_transaction_id,''),reference_attempts,created_at
		FROM wager_transactions WHERE status='PENDING_REFERENCE' AND reference_next_attempt_at<=now()
		ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&id, &request.ProviderID, &request.ExternalTransactionID, &request.IdempotencyKey, &request.PlayerID, &request.WalletID, &request.RoundID, &request.GameID, &request.Kind, &amount, &currency, &request.ReferenceExternalTransactionID, &attempts, &createdAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	request.Money, err = domain.MoneyFromMinorUnits(amount, currency)
	if err != nil {
		return false, err
	}
	reference, pending, failureCode, err := resolveReference(ctx, tx, request)
	if err != nil {
		return false, err
	}
	if pending && (attempts+1 >= maxReferenceAttempts || time.Since(createdAt) >= 24*time.Hour) {
		pending = false
		if failureCode == "" {
			failureCode = FailureReferenceNotFound
		}
	}
	if pending {
		attempts++
		_, err = tx.Exec(ctx, `UPDATE wager_transactions SET reference_attempts=$1,reference_next_attempt_at=now()+make_interval(secs => LEAST(300,power(2,$1)::integer)),updated_at=now() WHERE id=$2`, attempts, id)
		if err != nil {
			return false, err
		}
		return true, tx.Commit(ctx)
	}
	if failureCode != "" {
		var before, version int64
		if err = tx.QueryRow(ctx, `SELECT balance_minor,version FROM wallets WHERE id=$1 FOR UPDATE`, request.WalletID).Scan(&before, &version); err != nil {
			return false, err
		}
		_, err = w.processor.reject(ctx, tx, request, id, before, version, currency, failureCode, "", "")
		if err != nil {
			return false, err
		}
		return true, tx.Commit(ctx)
	}
	if err = storeReference(ctx, tx, id, reference); err != nil {
		return false, err
	}
	var before, version int64
	var walletCurrency string
	if err = tx.QueryRow(ctx, `SELECT balance_minor,version,currency FROM wallets WHERE id=$1 AND player_id=$2 FOR UPDATE`, request.WalletID, request.PlayerID).Scan(&before, &version, &walletCurrency); err != nil {
		return false, err
	}
	if walletCurrency != currency {
		return false, domain.ErrCurrencyMismatch
	}
	if _, err = w.processor.apply(ctx, tx, request, id, before, version, currency, "", ""); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}
