package app

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/desafio/wager-service/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrWalletConflict = errors.New("wallet already exists for player and currency")
	ErrInvalidCursor  = errors.New("invalid ledger cursor")
)

type WalletView struct {
	ID        string       `json:"id"`
	PlayerID  string       `json:"playerId"`
	Balance   domain.Money `json:"balance"`
	Version   int64        `json:"version"`
	CreatedAt time.Time    `json:"createdAt,omitempty"`
	UpdatedAt time.Time    `json:"updatedAt,omitempty"`
}

type LedgerView struct {
	ID            string       `json:"id"`
	TransactionID string       `json:"transactionId"`
	Direction     string       `json:"direction"`
	Money         domain.Money `json:"money"`
	BalanceBefore domain.Money `json:"balanceBefore"`
	BalanceAfter  domain.Money `json:"balanceAfter"`
	CreatedAt     time.Time    `json:"createdAt"`
}

type LedgerPage struct {
	Entries    []LedgerView `json:"entries"`
	NextCursor string       `json:"nextCursor,omitempty"`
}

type Reconciliation struct {
	WalletID          string       `json:"walletId"`
	StoredBalance     domain.Money `json:"storedBalance"`
	CalculatedBalance domain.Money `json:"calculatedBalance"`
	Difference        domain.Money `json:"difference"`
	Consistent        bool         `json:"consistent"`
	CheckedEntries    int64        `json:"checkedEntries"`
}

func (p *Processor) CreateWallet(ctx context.Context, playerID string, initial domain.Money) (WalletView, error) {
	if playerID == "" || !initial.Valid() || initial.MinorUnits() < 0 {
		return WalletView{}, domain.ErrInvalidWager
	}
	if _, err := domain.NewWallet("new", playerID, initial); err != nil {
		return WalletView{}, err
	}
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return WalletView{}, err
	}
	defer tx.Rollback(ctx)
	var wallet WalletView
	var minor int64
	var currency string
	err = tx.QueryRow(ctx, `INSERT INTO wallets(player_id,currency,balance_minor) VALUES ($1,$2,$3) RETURNING id::text,player_id::text,balance_minor,currency,version,created_at,updated_at`, playerID, initial.Currency(), initial.MinorUnits()).Scan(&wallet.ID, &wallet.PlayerID, &minor, &currency, &wallet.Version, &wallet.CreatedAt, &wallet.UpdatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return WalletView{}, ErrWalletConflict
		}
		return WalletView{}, err
	}
	wallet.Balance = moneyValue(minor, currency)
	if initial.IsPositive() {
		var transactionID string
		err = tx.QueryRow(ctx, `INSERT INTO wager_transactions(origin,wallet_id,player_id,kind,amount_minor,currency,status,result_balance_minor,wallet_version)
			VALUES ('INTERNAL',$1,$2,'OPENING',$3,$4,'PROCESSED',$3,1) RETURNING id::text`, wallet.ID, playerID, initial.MinorUnits(), initial.Currency()).Scan(&transactionID)
		if err != nil {
			return WalletView{}, err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO wallet_ledger(wallet_id,transaction_id,direction,amount_minor,currency,wallet_version,balance_before_minor,balance_after_minor) VALUES ($1,$2,'CREDIT',$3,$4,1,0,$3)`, wallet.ID, transactionID, initial.MinorUnits(), initial.Currency()); err != nil {
			return WalletView{}, err
		}
		if err = insertEvent(ctx, tx, newProcessedEvent(uuid.NewString(), wallet.ID, transactionID, transactionID, "", "", "OPENING", 1)); err != nil {
			return WalletView{}, err
		}
		if err = insertEvent(ctx, tx, newBalanceChangedEvent(uuid.NewString(), wallet.ID, transactionID, transactionID, "CREDIT", initial, moneyValue(0, currency), initial, 1)); err != nil {
			return WalletView{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return WalletView{}, err
	}
	return wallet, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func (p *Processor) GetWallet(ctx context.Context, walletID string) (WalletView, error) {
	var wallet WalletView
	var minor int64
	var currency string
	err := p.pool.QueryRow(ctx, `SELECT id::text,player_id::text,balance_minor,currency,version,created_at,updated_at FROM wallets WHERE id=$1`, walletID).Scan(&wallet.ID, &wallet.PlayerID, &minor, &currency, &wallet.Version, &wallet.CreatedAt, &wallet.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return WalletView{}, ErrWalletNotFound
	}
	if err != nil {
		return WalletView{}, err
	}
	wallet.Balance = moneyValue(minor, currency)
	return wallet, nil
}

func (p *Processor) GetTransaction(ctx context.Context, providerID, externalID string) (Result, error) {
	var result Result
	var minor *int64
	var currency string
	err := p.pool.QueryRow(ctx, `SELECT id::text,status,result_balance_minor,wallet_version,failure_code,currency FROM wager_transactions WHERE origin='EXTERNAL' AND provider_id=$1 AND external_transaction_id=$2`, providerID, externalID).Scan(&result.TransactionID, &result.Status, &minor, &result.WalletVersion, &result.FailureCode, &currency)
	if errors.Is(err, pgx.ErrNoRows) {
		return Result{}, pgx.ErrNoRows
	}
	if err != nil {
		return Result{}, err
	}
	if minor != nil {
		result.Balance = moneyValue(*minor, currency)
	} else {
		result.Balance, _ = domain.ZeroMoney(currency)
	}
	return result, nil
}

func (p *Processor) GetTransactionByID(ctx context.Context, providerID, transactionID string) (Result, error) {
	var result Result
	var minor *int64
	var currency string
	err := p.pool.QueryRow(ctx, `SELECT id::text,status,result_balance_minor,wallet_version,failure_code,currency FROM wager_transactions WHERE origin='EXTERNAL' AND provider_id=$1 AND id=$2`, providerID, transactionID).Scan(&result.TransactionID, &result.Status, &minor, &result.WalletVersion, &result.FailureCode, &currency)
	if err != nil {
		return Result{}, err
	}
	if minor != nil {
		result.Balance = moneyValue(*minor, currency)
	} else {
		result.Balance, _ = domain.ZeroMoney(currency)
	}
	return result, nil
}

func (p *Processor) GetTransactionByAnyID(ctx context.Context, transactionID string) (Result, error) {
	var result Result
	var minor *int64
	var currency string
	err := p.pool.QueryRow(ctx, `SELECT id::text,status,result_balance_minor,wallet_version,failure_code,currency FROM wager_transactions WHERE id=$1`, transactionID).Scan(&result.TransactionID, &result.Status, &minor, &result.WalletVersion, &result.FailureCode, &currency)
	if errors.Is(err, pgx.ErrNoRows) {
		return Result{}, pgx.ErrNoRows
	}
	if err != nil {
		return Result{}, err
	}
	if minor != nil {
		result.Balance = moneyValue(*minor, currency)
	} else {
		result.Balance, _ = domain.ZeroMoney(currency)
	}
	return result, nil
}

func (p *Processor) Ledger(ctx context.Context, walletID, cursor string, limit int) (LedgerPage, error) {
	if limit < 1 || limit > 200 {
		limit = 50
	}
	var rows pgx.Rows
	var err error
	if cursor == "" {
		rows, err = p.pool.Query(ctx, `SELECT id::text,transaction_id::text,direction,amount_minor,currency,balance_before_minor,balance_after_minor,created_at FROM wallet_ledger WHERE wallet_id=$1 ORDER BY created_at DESC,id DESC LIMIT $2`, walletID, limit+1)
	} else {
		decoded, decodeErr := base64.RawURLEncoding.DecodeString(cursor)
		if decodeErr != nil {
			return LedgerPage{}, ErrInvalidCursor
		}
		parts := strings.SplitN(string(decoded), "|", 2)
		if len(parts) != 2 {
			return LedgerPage{}, ErrInvalidCursor
		}
		createdAt, parseErr := time.Parse(time.RFC3339Nano, parts[0])
		if parseErr != nil || uuid.Validate(parts[1]) != nil {
			return LedgerPage{}, ErrInvalidCursor
		}
		rows, err = p.pool.Query(ctx, `SELECT id::text,transaction_id::text,direction,amount_minor,currency,balance_before_minor,balance_after_minor,created_at FROM wallet_ledger WHERE wallet_id=$1 AND (created_at,id)<($2,$3) ORDER BY created_at DESC,id DESC LIMIT $4`, walletID, createdAt, parts[1], limit+1)
	}
	if err != nil {
		return LedgerPage{}, err
	}
	defer rows.Close()
	entries := make([]LedgerView, 0, limit+1)
	for rows.Next() {
		var item LedgerView
		var amount, before, after int64
		var currency string
		if err := rows.Scan(&item.ID, &item.TransactionID, &item.Direction, &amount, &currency, &before, &after, &item.CreatedAt); err != nil {
			return LedgerPage{}, err
		}
		item.Money, item.BalanceBefore, item.BalanceAfter = moneyValue(amount, currency), moneyValue(before, currency), moneyValue(after, currency)
		entries = append(entries, item)
	}
	if err := rows.Err(); err != nil {
		return LedgerPage{}, err
	}
	page := LedgerPage{Entries: entries}
	if len(entries) > limit {
		last := entries[limit-1]
		page.Entries = entries[:limit]
		page.NextCursor = base64.RawURLEncoding.EncodeToString([]byte(last.CreatedAt.UTC().Format(time.RFC3339Nano) + "|" + last.ID))
	}
	return page, nil
}

func (p *Processor) Reconcile(ctx context.Context, walletID string) (Reconciliation, error) {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Reconciliation{}, err
	}
	defer tx.Rollback(ctx)
	var stored, credits, debits, count int64
	var currency string
	err = tx.QueryRow(ctx, `SELECT w.balance_minor,w.currency,
		COALESCE(SUM(CASE WHEN l.direction='CREDIT' THEN l.amount_minor ELSE 0 END),0)::bigint,
		COALESCE(SUM(CASE WHEN l.direction='DEBIT' THEN l.amount_minor ELSE 0 END),0)::bigint,
		COUNT(l.id)
		FROM wallets w LEFT JOIN wallet_ledger l ON l.wallet_id=w.id WHERE w.id=$1 GROUP BY w.id`, walletID).Scan(&stored, &currency, &credits, &debits, &count)
	if errors.Is(err, pgx.ErrNoRows) {
		return Reconciliation{}, ErrWalletNotFound
	}
	if err != nil {
		return Reconciliation{}, err
	}
	calculated := credits - debits
	if calculated < 0 && stored > math.MaxInt64+calculated {
		return Reconciliation{}, fmt.Errorf("reconciliation difference overflow")
	}
	difference := stored - calculated
	calculatedMoney, err := domain.MoneyFromMinorUnits(calculated, currency)
	if err != nil {
		return Reconciliation{}, err
	}
	differenceMoney, err := domain.MoneyFromMinorUnits(difference, currency)
	if err != nil {
		return Reconciliation{}, err
	}
	result := Reconciliation{WalletID: walletID, StoredBalance: moneyValue(stored, currency), CalculatedBalance: calculatedMoney, Difference: differenceMoney, Consistent: stored == calculated, CheckedEntries: count}
	if err = tx.Commit(ctx); err != nil {
		return Reconciliation{}, err
	}
	return result, nil
}
