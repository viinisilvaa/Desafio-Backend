package domain

import (
	"errors"
	"testing"
	"time"
)

func TestWagerRulesAndWalletTransitions(t *testing.T) {
	zero, _ := ParseMoney("0.00", "BRL")
	positive, _ := ParseMoney("80.00", "BRL")
	initial, _ := ParseMoney("100.00", "BRL")
	request := WagerRequest{
		ProviderID: "provider-a", ExternalTransactionID: "tx-1", IdempotencyKey: "key-1",
		PlayerID: "player-1", WalletID: "wallet-1", RoundID: "round-1", GameID: "game-1",
		Kind: KindLoss, Money: zero,
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("zero LOSS should be accepted: %v", err)
	}
	request.Kind = KindBet
	request.Money = zero
	if err := request.Validate(); !errors.Is(err, ErrInvalidWager) {
		t.Fatalf("zero BET should be rejected, got %v", err)
	}
	wallet, err := NewWallet("wallet-1", "player-1", initial)
	if err != nil {
		t.Fatal(err)
	}
	updated, _, err := wallet.Apply(KindBet, positive)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Balance().MinorUnits() != 2000 || updated.Version() != 2 {
		t.Fatalf("unexpected wallet after debit: balance=%d version=%d", updated.Balance().MinorUnits(), updated.Version())
	}
	if _, _, err := updated.Apply(KindBet, positive); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("expected insufficient funds, got %v", err)
	}
	negative, _ := MoneyFromMinorUnits(-100, "BRL")
	if _, _, err := wallet.Apply(KindBet, negative); !errors.Is(err, ErrInvalidWager) {
		t.Fatalf("negative internal amount must not invert a debit, got %v", err)
	}
	unchanged, lossBalance, err := updated.Apply(KindLoss, zero)
	if err != nil || unchanged.Version() != updated.Version() || lossBalance.MinorUnits() != updated.Balance().MinorUnits() {
		t.Fatalf("LOSS changed the wallet: version=%d balance=%d err=%v", unchanged.Version(), lossBalance.MinorUnits(), err)
	}
}

func TestWagerTransactionTransitionsAndRehydration(t *testing.T) {
	amount, _ := ParseMoney("1.00", "BRL")
	request := WagerRequest{ProviderID: "p", ExternalTransactionID: "e", IdempotencyKey: "k", PlayerID: "player", WalletID: "wallet", RoundID: "round", GameID: "game", Kind: KindBet, Money: amount}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.FixedZone("test", 3600))
	transaction, err := NewWagerTransaction("transaction-1", request, now)
	if err != nil {
		t.Fatal(err)
	}
	if transaction.Status() != StatusPending {
		t.Fatalf("initial status = %s, want PENDING", transaction.Status())
	}
	processed, err := transaction.MarkProcessed(now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = processed.Reject("LATE_REJECT", now.Add(2*time.Second)); !errors.Is(err, ErrTerminalTransaction) {
		t.Fatalf("terminal transition should fail, got %v", err)
	}
	rehydrated, err := RehydrateWagerTransaction(processed.ID(), request, processed.Status(), "", processed.CreatedAt(), processed.UpdatedAt())
	if err != nil || rehydrated.Status() != StatusProcessed {
		t.Fatalf("rehydration changed transaction state: %v", err)
	}
	if _, err = RehydrateWagerTransaction("x", request, StatusRejected, "", now, now); !errors.Is(err, ErrInvalidWager) {
		t.Fatalf("rejected rehydration without failure code should fail, got %v", err)
	}
	pending, err := transaction.MarkPendingReference(now.Add(time.Second))
	if err != nil || pending.Status() != StatusPendingReference {
		t.Fatalf("expected pending-reference state, got %s, %v", pending.Status(), err)
	}
}

func TestWagerReferenceAndInternalOpeningRules(t *testing.T) {
	amount, _ := ParseMoney("1.00", "BRL")
	request := WagerRequest{
		ProviderID: "p", ExternalTransactionID: "e", IdempotencyKey: "k", PlayerID: "player",
		WalletID: "wallet", RoundID: "round", GameID: "game", Kind: KindRefund, Money: amount,
	}
	if !errors.Is(request.Validate(), ErrReferenceRequired) {
		t.Fatal("REFUND without a reference must be rejected")
	}
	request.Kind = "OPENING"
	if err := request.Validate(); err == nil {
		t.Fatal("external OPENING must be rejected")
	}
}

func TestAllExternalOperationAmountPolicies(t *testing.T) {
	zero, _ := ParseMoney("0.00", "BRL")
	one, _ := ParseMoney("0.01", "BRL")
	valid := map[WagerKind]Money{KindBet: one, KindWin: one, KindRefund: one, KindRollback: one, KindLoss: zero}
	for kind, amount := range valid {
		request := WagerRequest{ProviderID: "p", ExternalTransactionID: "e", IdempotencyKey: "k", PlayerID: "player", WalletID: "wallet", RoundID: "round", GameID: "game", Kind: kind, Money: amount}
		if kind == KindRefund || kind == KindRollback {
			request.ReferenceExternalTransactionID = "reference"
		}
		if err := request.Validate(); err != nil {
			t.Errorf("%s with valid amount rejected: %v", kind, err)
		}
		request.Money = zero
		if kind != KindLoss {
			if err := request.Validate(); err == nil {
				t.Errorf("%s accepted zero amount", kind)
			}
		}
	}
}
