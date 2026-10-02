package app

import (
	"encoding/json"
	"testing"

	"github.com/desafio/wager-service/internal/domain"
)

func TestBalanceChangedEventUsesDecimalStringAndVersionedEnvelope(t *testing.T) {
	money, _ := domain.ParseMoney("2.50", "BRL")
	before, _ := domain.ParseMoney("10.00", "BRL")
	after, _ := domain.ParseMoney("7.50", "BRL")
	event := newBalanceChangedEvent("event-1", "wallet-1", "correlation-1", "transaction-1", "DEBIT", money, before, after, 2)
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err = json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["eventId"] != "event-1" || decoded["eventType"] != "WalletBalanceChanged" || decoded["version"] != float64(2) {
		t.Fatalf("unexpected event envelope: %s", encoded)
	}
	data := decoded["data"].(map[string]any)
	if data["direction"] != "DEBIT" || data["money"].(map[string]any)["amount"] != "2.50" {
		t.Fatalf("unexpected typed payload: %s", encoded)
	}
}
