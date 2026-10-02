package domain

import (
	"errors"
	"fmt"
	"time"
)

type TransactionStatus string

const (
	StatusPending          TransactionStatus = "PENDING"
	StatusPendingReference TransactionStatus = "PENDING_REFERENCE"
	StatusProcessed        TransactionStatus = "PROCESSED"
	StatusRejected         TransactionStatus = "REJECTED"
	StatusFailed           TransactionStatus = "FAILED"
)

var ErrTerminalTransaction = errors.New("transaction is terminal")

type WagerTransaction struct {
	id        string
	request   WagerRequest
	status    TransactionStatus
	failure   string
	createdAt time.Time
	updatedAt time.Time
}

func NewWagerTransaction(id string, request WagerRequest, now time.Time) (WagerTransaction, error) {
	if id == "" {
		return WagerTransaction{}, fmt.Errorf("%w: transaction id is required", ErrInvalidWager)
	}
	if err := request.Validate(); err != nil {
		return WagerTransaction{}, err
	}
	if now.IsZero() {
		return WagerTransaction{}, fmt.Errorf("%w: transaction timestamp is required", ErrInvalidWager)
	}
	return WagerTransaction{id: id, request: request, status: StatusPending, createdAt: now.UTC(), updatedAt: now.UTC()}, nil
}

func RehydrateWagerTransaction(id string, request WagerRequest, status TransactionStatus, failure string, createdAt, updatedAt time.Time) (WagerTransaction, error) {
	if id == "" || !request.Money.Valid() || !validStatus(status) || createdAt.IsZero() || updatedAt.IsZero() || updatedAt.Before(createdAt) {
		return WagerTransaction{}, fmt.Errorf("%w: invalid persisted transaction", ErrInvalidWager)
	}
	if err := request.Validate(); err != nil {
		return WagerTransaction{}, err
	}
	if status == StatusRejected || status == StatusFailed {
		if failure == "" {
			return WagerTransaction{}, fmt.Errorf("%w: terminal error requires a failure code", ErrInvalidWager)
		}
	} else if failure != "" {
		return WagerTransaction{}, fmt.Errorf("%w: non-error status cannot have a failure code", ErrInvalidWager)
	}
	return WagerTransaction{id: id, request: request, status: status, failure: failure, createdAt: createdAt.UTC(), updatedAt: updatedAt.UTC()}, nil
}

func (t WagerTransaction) ID() string                { return t.id }
func (t WagerTransaction) Request() WagerRequest     { return t.request }
func (t WagerTransaction) Status() TransactionStatus { return t.status }
func (t WagerTransaction) FailureCode() string       { return t.failure }
func (t WagerTransaction) CreatedAt() time.Time      { return t.createdAt }
func (t WagerTransaction) UpdatedAt() time.Time      { return t.updatedAt }

func (t WagerTransaction) MarkPendingReference(now time.Time) (WagerTransaction, error) {
	return t.transition(StatusPendingReference, "", now)
}

func (t WagerTransaction) MarkProcessed(now time.Time) (WagerTransaction, error) {
	return t.transition(StatusProcessed, "", now)
}

func (t WagerTransaction) Reject(failureCode string, now time.Time) (WagerTransaction, error) {
	if failureCode == "" {
		return WagerTransaction{}, fmt.Errorf("%w: rejection code is required", ErrInvalidWager)
	}
	return t.transition(StatusRejected, failureCode, now)
}

func (t WagerTransaction) Fail(failureCode string, now time.Time) (WagerTransaction, error) {
	if failureCode == "" {
		return WagerTransaction{}, fmt.Errorf("%w: failure code is required", ErrInvalidWager)
	}
	return t.transition(StatusFailed, failureCode, now)
}

func (t WagerTransaction) transition(status TransactionStatus, failure string, now time.Time) (WagerTransaction, error) {
	if t.id == "" || !validStatus(t.status) || now.IsZero() {
		return WagerTransaction{}, ErrInvalidWager
	}
	if t.status == StatusProcessed || t.status == StatusRejected || t.status == StatusFailed {
		return WagerTransaction{}, ErrTerminalTransaction
	}
	if status == StatusPendingReference && t.status != StatusPending {
		return WagerTransaction{}, ErrInvalidWager
	}
	if now.Before(t.updatedAt) {
		return WagerTransaction{}, fmt.Errorf("%w: transition timestamp moved backwards", ErrInvalidWager)
	}
	t.status = status
	t.failure = failure
	t.updatedAt = now.UTC()
	return t, nil
}

func validStatus(status TransactionStatus) bool {
	switch status {
	case StatusPending, StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
		return true
	default:
		return false
	}
}
