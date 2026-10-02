package app

import (
	"time"

	"github.com/desafio/wager-service/internal/domain"
)

type WagerTransactionProcessedData struct {
	TransactionID         string `json:"transactionId"`
	ProviderID            string `json:"providerId,omitempty"`
	ExternalTransactionID string `json:"externalTransactionId,omitempty"`
	Kind                  string `json:"kind"`
	Status                string `json:"status"`
}

type WagerTransactionRejectedData struct {
	TransactionID         string `json:"transactionId"`
	ProviderID            string `json:"providerId"`
	ExternalTransactionID string `json:"externalTransactionId"`
	Status                string `json:"status"`
	FailureCode           string `json:"failureCode"`
}

type WagerTransactionPendingReferenceData struct {
	TransactionID                  string `json:"transactionId"`
	ProviderID                     string `json:"providerId"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId"`
}

type WalletBalanceChangedData struct {
	WalletID      string       `json:"walletId"`
	TransactionID string       `json:"transactionId"`
	Direction     string       `json:"direction"`
	Money         domain.Money `json:"money"`
	BalanceBefore domain.Money `json:"balanceBefore"`
	BalanceAfter  domain.Money `json:"balanceAfter"`
	WalletVersion int64        `json:"walletVersion"`
}

func newProcessedEvent(id, walletID, correlationID, transactionID, providerID, externalID, kind string, version int64) eventEnvelope {
	return eventEnvelope{EventID: id, EventType: "WagerTransactionProcessed", AggregateID: walletID, CorrelationID: correlationID, OccurredAt: time.Now().UTC().Format(time.RFC3339Nano), Version: version, Data: WagerTransactionProcessedData{TransactionID: transactionID, ProviderID: providerID, ExternalTransactionID: externalID, Kind: kind, Status: "PROCESSED"}}
}

func newRejectedEvent(id, walletID, correlationID, transactionID, providerID, externalID, failureCode string, version int64) eventEnvelope {
	return eventEnvelope{EventID: id, EventType: "WagerTransactionRejected", AggregateID: walletID, CorrelationID: correlationID, OccurredAt: time.Now().UTC().Format(time.RFC3339Nano), Version: version, Data: WagerTransactionRejectedData{TransactionID: transactionID, ProviderID: providerID, ExternalTransactionID: externalID, Status: "REJECTED", FailureCode: failureCode}}
}

func newPendingReferenceEvent(id, walletID, correlationID, transactionID, providerID, referenceID string, version int64) eventEnvelope {
	return eventEnvelope{EventID: id, EventType: "WagerTransactionPendingReference", AggregateID: walletID, CorrelationID: correlationID, OccurredAt: time.Now().UTC().Format(time.RFC3339Nano), Version: version, Data: WagerTransactionPendingReferenceData{TransactionID: transactionID, ProviderID: providerID, ReferenceExternalTransactionID: referenceID}}
}

func newBalanceChangedEvent(id, walletID, correlationID, transactionID, direction string, money, before, after domain.Money, version int64) eventEnvelope {
	return eventEnvelope{EventID: id, EventType: "WalletBalanceChanged", AggregateID: walletID, CorrelationID: correlationID, OccurredAt: time.Now().UTC().Format(time.RFC3339Nano), Version: version, Data: WalletBalanceChangedData{WalletID: walletID, TransactionID: transactionID, Direction: direction, Money: money, BalanceBefore: before, BalanceAfter: after, WalletVersion: version}}
}
