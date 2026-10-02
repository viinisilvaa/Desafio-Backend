package domain

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func TestParseMoneyAndArithmetic(t *testing.T) {
	first, err := ParseMoney("25.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	second, err := ParseMoney("4.35", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	total, err := first.Add(second)
	if err != nil {
		t.Fatal(err)
	}
	formatted, err := total.String()
	if err != nil || formatted != "29.35" {
		t.Fatalf("got %q, %v; want 29.35", formatted, err)
	}
}

func TestParseMoneyRejectsInvalidExternalAmounts(t *testing.T) {
	for _, amount := range []string{"", "1", "1.0", "1.000", "1e2", "NaN", "Infinity", "-0.01", "+1.00", " 1.00", ".25", "1.2.3"} {
		t.Run(amount, func(t *testing.T) {
			if _, err := ParseMoney(amount, "BRL"); err == nil {
				t.Fatalf("ParseMoney(%q) unexpectedly succeeded", amount)
			}
		})
	}
}

func TestMoneyOverflowAndCurrencyMismatch(t *testing.T) {
	maximum, err := ParseMoney("92233720368547758.07", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	one, _ := ParseMoney("0.01", "BRL")
	if _, err := maximum.Add(one); !errors.Is(err, ErrMoneyOverflow) {
		t.Fatalf("expected overflow, got %v", err)
	}
	eur, _ := ParseMoney("0.01", "EUR")
	if _, err := one.Add(eur); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("expected currency mismatch, got %v", err)
	}
	if _, err := (Money{minorUnits: math.MinInt64, currency: "BRL", valid: true}).Negate(); !errors.Is(err, ErrMoneyOverflow) {
		t.Fatalf("expected negate overflow, got %v", err)
	}
	if _, err := ParseMoney("92233720368547758.08", "BRL"); !errors.Is(err, ErrMoneyOverflow) {
		t.Fatalf("expected parse overflow, got %v", err)
	}
}

func TestMoneyJSONUsesDecimalString(t *testing.T) {
	value, err := ParseMoney("0.05", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"amount":"0.05","currency":"BRL"}` {
		t.Fatalf("unexpected JSON: %s", encoded)
	}
}

func TestMoneySignedInternalValuesAndSubtraction(t *testing.T) {
	negative, err := MoneyFromMinorUnits(-125, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	formatted, err := negative.String()
	if err != nil || formatted != "-1.25" {
		t.Fatalf("got %q, %v; want -1.25", formatted, err)
	}
	positive, _ := ParseMoney("2.00", "BRL")
	result, err := positive.Subtract(negative)
	if err != nil || result.MinorUnits() != 325 {
		t.Fatalf("unexpected subtraction result: %d, %v", result.MinorUnits(), err)
	}
	if _, err = (Money{}).Add(positive); !errors.Is(err, ErrInvalidMoney) {
		t.Fatalf("zero value Money should be invalid, got %v", err)
	}
	minimum, err := MoneyFromMinorUnits(math.MinInt64, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	formatted, err = minimum.String()
	if err != nil || formatted != "-92233720368547758.08" {
		t.Fatalf("MinInt64 formatted as %q, %v", formatted, err)
	}
}

func TestMoneyRejectsJSONNumberAndInvalidCurrency(t *testing.T) {
	var value Money
	if err := json.Unmarshal([]byte(`{"amount":1.25,"currency":"BRL"}`), &value); err == nil {
		t.Fatal("JSON number amount must be rejected")
	}
	if _, err := ParseMoney("1.00", "brl"); !errors.Is(err, ErrInvalidMoney) {
		t.Fatalf("lowercase currency should be rejected, got %v", err)
	}
	if _, err := ParseMoney("1.00", "ZZZ"); !errors.Is(err, ErrInvalidMoney) {
		t.Fatalf("unknown currency should be rejected, got %v", err)
	}
}
