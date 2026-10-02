package domain

import (
	"errors"
	"fmt"
)

type WagerKind string

const (
	KindBet      WagerKind = "BET"
	KindWin      WagerKind = "WIN"
	KindLoss     WagerKind = "LOSS"
	KindRefund   WagerKind = "REFUND"
	KindRollback WagerKind = "ROLLBACK"
)

var (
	ErrInvalidWager      = errors.New("invalid wager")
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrReferenceRequired = errors.New("reference transaction required")
)

type WagerRequest struct {
	ProviderID                     string    `json:"providerId"`
	ExternalTransactionID          string    `json:"externalTransactionId"`
	IdempotencyKey                 string    `json:"idempotencyKey"`
	PlayerID                       string    `json:"playerId"`
	WalletID                       string    `json:"walletId"`
	RoundID                        string    `json:"roundId"`
	GameID                         string    `json:"gameId"`
	Kind                           WagerKind `json:"kind"`
	Money                          Money     `json:"money"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId,omitempty"`
}

func (r WagerRequest) Validate() error {
	if r.ProviderID == "" || r.ExternalTransactionID == "" || r.IdempotencyKey == "" ||
		r.PlayerID == "" || r.WalletID == "" || r.RoundID == "" || r.GameID == "" || !r.Money.Valid() {
		return fmt.Errorf("%w: required field missing", ErrInvalidWager)
	}
	if r.Kind == "OPENING" {
		return fmt.Errorf("%w: OPENING is internal-only", ErrInvalidWager)
	}
	switch r.Kind {
	case KindBet, KindWin, KindRefund, KindRollback:
		if !r.Money.IsPositive() {
			return fmt.Errorf("%w: %s requires a positive amount", ErrInvalidWager, r.Kind)
		}
	case KindLoss:
		if !r.Money.IsZero() {
			return fmt.Errorf("%w: LOSS requires 0.00", ErrInvalidWager)
		}
	default:
		return fmt.Errorf("%w: unsupported kind %q", ErrInvalidWager, r.Kind)
	}
	if (r.Kind == KindRefund || r.Kind == KindRollback) && r.ReferenceExternalTransactionID == "" {
		return ErrReferenceRequired
	}
	if r.Kind != KindRefund && r.Kind != KindRollback && r.Kind != KindWin && r.ReferenceExternalTransactionID != "" {
		return fmt.Errorf("%w: reference is not supported for %s", ErrInvalidWager, r.Kind)
	}
	return nil
}

type Wallet struct {
	id       string
	playerID string
	balance  Money
	version  int64
}

func NewWallet(id, playerID string, initial Money) (Wallet, error) {
	if id == "" || playerID == "" || !initial.Valid() || initial.MinorUnits() < 0 {
		return Wallet{}, fmt.Errorf("%w: invalid wallet identity or initial balance", ErrInvalidWager)
	}
	return Wallet{id: id, playerID: playerID, balance: initial, version: 1}, nil
}

func RehydrateWallet(id, playerID string, balance Money, version int64) (Wallet, error) {
	if id == "" || playerID == "" || !balance.Valid() || balance.MinorUnits() < 0 || version < 1 {
		return Wallet{}, fmt.Errorf("%w: invalid persisted wallet", ErrInvalidWager)
	}
	return Wallet{id: id, playerID: playerID, balance: balance, version: version}, nil
}

func (w Wallet) ID() string       { return w.id }
func (w Wallet) PlayerID() string { return w.playerID }
func (w Wallet) Balance() Money   { return w.balance }
func (w Wallet) Version() int64   { return w.version }

func (w Wallet) Apply(kind WagerKind, amount Money) (Wallet, Money, error) {
	if w.id == "" || !w.balance.Valid() || !amount.Valid() {
		return Wallet{}, Money{}, ErrInvalidWager
	}
	if w.balance.Currency() != amount.Currency() {
		return Wallet{}, Money{}, ErrCurrencyMismatch
	}
	if kind != KindLoss && !amount.IsPositive() {
		return Wallet{}, Money{}, ErrInvalidWager
	}
	next := w.balance
	switch kind {
	case KindBet, KindRollback:
		var err error
		next, err = w.balance.Subtract(amount)
		if err != nil {
			return Wallet{}, Money{}, err
		}
	case KindWin, KindRefund:
		var err error
		next, err = w.balance.Add(amount)
		if err != nil {
			return Wallet{}, Money{}, err
		}
	case KindLoss:
		if !amount.IsZero() {
			return Wallet{}, Money{}, ErrInvalidWager
		}
		return w, w.balance, nil
	default:
		return Wallet{}, Money{}, ErrInvalidWager
	}
	if next.MinorUnits() < 0 {
		return Wallet{}, Money{}, ErrInsufficientFunds
	}
	w.balance = next
	w.version++
	return w, next, nil
}
