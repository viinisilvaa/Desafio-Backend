package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"golang.org/x/text/currency"
)

var (
	ErrInvalidMoney       = errors.New("invalid money")
	ErrCurrencyMismatch   = errors.New("currency mismatch")
	ErrMoneyOverflow      = errors.New("money overflow")
	ErrNegativeMoneyInput = errors.New("negative external amount")
)

// Money stores minor units and an ISO 4217 currency. Its zero value is invalid.
type Money struct {
	minorUnits int64
	currency   string
	valid      bool
}

func ParseMoney(amount, currency string) (Money, error) {
	if !validCurrency(currency) {
		return Money{}, fmt.Errorf("%w: currency must be a three-letter uppercase code", ErrInvalidMoney)
	}
	if amount == "" || strings.ContainsAny(amount, "eE+ ") {
		return Money{}, fmt.Errorf("%w: amount must be a fixed-point decimal", ErrInvalidMoney)
	}
	if strings.HasPrefix(amount, "-") {
		return Money{}, ErrNegativeMoneyInput
	}
	if strings.HasPrefix(amount, "+") {
		return Money{}, fmt.Errorf("%w: leading plus is not accepted", ErrInvalidMoney)
	}
	parts := strings.Split(amount, ".")
	if len(parts) != 2 || parts[0] == "" || len(parts[1]) != 2 {
		return Money{}, fmt.Errorf("%w: expected exactly two fractional digits", ErrInvalidMoney)
	}
	for _, part := range parts {
		for _, digit := range part {
			if digit < '0' || digit > '9' {
				return Money{}, fmt.Errorf("%w: amount contains a non-digit", ErrInvalidMoney)
			}
		}
	}
	whole, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || whole > math.MaxInt64/100 {
		return Money{}, ErrMoneyOverflow
	}
	minor, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return Money{}, ErrInvalidMoney
	}
	base := whole * 100
	if base > math.MaxInt64-minor {
		return Money{}, ErrMoneyOverflow
	}
	return Money{minorUnits: base + minor, currency: currency, valid: true}, nil
}

func ZeroMoney(currency string) (Money, error) {
	return ParseMoney("0.00", currency)
}

func MoneyFromMinorUnits(minorUnits int64, currency string) (Money, error) {
	if !validCurrency(currency) {
		return Money{}, fmt.Errorf("%w: currency must be a three-letter uppercase code", ErrInvalidMoney)
	}
	return Money{minorUnits: minorUnits, currency: currency, valid: true}, nil
}

func validCurrency(code string) bool {
	if len(code) != 3 || code != strings.ToUpper(code) {
		return false
	}
	_, err := currency.ParseISO(code)
	return err == nil
}

func (m Money) Valid() bool { return m.valid }

func (m Money) Currency() string { return m.currency }

func (m Money) MinorUnits() int64 { return m.minorUnits }

func (m Money) IsZero() bool { return m.valid && m.minorUnits == 0 }

func (m Money) IsPositive() bool { return m.valid && m.minorUnits > 0 }

func (m Money) Add(other Money) (Money, error) {
	if err := m.checkCompatible(other); err != nil {
		return Money{}, err
	}
	if (other.minorUnits > 0 && m.minorUnits > math.MaxInt64-other.minorUnits) ||
		(other.minorUnits < 0 && m.minorUnits < math.MinInt64-other.minorUnits) {
		return Money{}, ErrMoneyOverflow
	}
	return Money{minorUnits: m.minorUnits + other.minorUnits, currency: m.currency, valid: true}, nil
}

func (m Money) Subtract(other Money) (Money, error) {
	if err := m.checkCompatible(other); err != nil {
		return Money{}, err
	}
	if other.minorUnits == math.MinInt64 {
		return Money{}, ErrMoneyOverflow
	}
	return m.Add(Money{minorUnits: -other.minorUnits, currency: other.currency, valid: true})
}

func (m Money) Negate() (Money, error) {
	if !m.valid {
		return Money{}, ErrInvalidMoney
	}
	if m.minorUnits == math.MinInt64 {
		return Money{}, ErrMoneyOverflow
	}
	return Money{minorUnits: -m.minorUnits, currency: m.currency, valid: true}, nil
}

func (m Money) Compare(other Money) (int, error) {
	if err := m.checkCompatible(other); err != nil {
		return 0, err
	}
	if m.minorUnits < other.minorUnits {
		return -1, nil
	}
	if m.minorUnits > other.minorUnits {
		return 1, nil
	}
	return 0, nil
}

func (m Money) String() (string, error) {
	if !m.valid {
		return "", ErrInvalidMoney
	}
	negative := m.minorUnits < 0
	var magnitude uint64
	if negative {
		magnitude = uint64(-(m.minorUnits + 1)) + 1
	} else {
		magnitude = uint64(m.minorUnits)
	}
	formatted := fmt.Sprintf("%d.%02d", magnitude/100, magnitude%100)
	if negative {
		formatted = "-" + formatted
	}
	return formatted, nil
}

func (m Money) MarshalJSON() ([]byte, error) {
	amount, err := m.String()
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	}{Amount: amount, Currency: m.currency})
}

func (m *Money) UnmarshalJSON(data []byte) error {
	var value struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	parsed, err := ParseMoney(value.Amount, value.Currency)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}

func (m Money) checkCompatible(other Money) error {
	if !m.valid || !other.valid {
		return ErrInvalidMoney
	}
	if m.currency != other.currency {
		return ErrCurrencyMismatch
	}
	return nil
}
