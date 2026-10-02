package app

import (
	"testing"

	"github.com/desafio/wager-service/internal/domain"
)

func TestHashRequestNormalizesAmountAndExcludesIdempotencyKey(t *testing.T) {
	firstAmount, err := domain.ParseMoney("0001.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	secondAmount, err := domain.ParseMoney("1.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	first := domain.WagerRequest{ProviderID: "provider-a", ExternalTransactionID: "external-1", IdempotencyKey: "key-a", PlayerID: "player", WalletID: "wallet", RoundID: "round", GameID: "game", Kind: domain.KindBet, Money: firstAmount}
	second := first
	second.IdempotencyKey = "key-b"
	second.Money = secondAmount
	firstHash, err := hashRequest(first)
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := hashRequest(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != secondHash {
		t.Fatal("equivalent normalized business payloads must produce the same hash")
	}
	second.GameID = "other-game"
	changedHash, err := hashRequest(second)
	if err != nil {
		t.Fatal(err)
	}
	if changedHash == firstHash {
		t.Fatal("changing a business field must change the payload hash")
	}
}
