package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/desafio/wager-service/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type HTTPServer struct {
	cfg       Config
	processor *Processor
	auth      requestAuthenticator
	pool      *pgxpool.Pool
	sqs       *sqs.Client
	logger    *slog.Logger
	metrics   *Metrics
	server    *http.Server
	listener  net.Listener
}

type requestAuthenticator interface {
	Authenticate(*http.Request) (Principal, error)
}

func NewHTTPServer(cfg Config, processor *Processor, auth *Authenticator, pool *pgxpool.Pool, queue *sqs.Client, logger *slog.Logger, metrics *Metrics) *HTTPServer {
	return &HTTPServer{cfg: cfg, processor: processor, auth: auth, pool: pool, sqs: queue, logger: logger, metrics: metrics}
}

func (s *HTTPServer) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "live"})
	})
	mux.HandleFunc("GET /health/ready", s.ready)
	mux.Handle("GET /metrics", http.HandlerFunc(s.metrics.Handler))
	mux.HandleFunc("POST /wallets", s.createWallet)
	mux.HandleFunc("GET /wallets/{walletID}", s.getWallet)
	mux.HandleFunc("GET /wallets/{walletID}/ledger", s.ledger)
	mux.HandleFunc("POST /wallets/{walletID}/reconciliation", s.reconcile)
	mux.HandleFunc("POST /wagering/transactions", s.submitWager)
	mux.HandleFunc("GET /wagering/transactions/{transactionID}", s.getTransactionByID)
	mux.HandleFunc("GET /providers/{providerID}/wagering/transactions/{externalTransactionID}", s.getProviderTransaction)
	listener, err := net.Listen("tcp", s.cfg.HTTPAddr)
	if err != nil {
		return err
	}
	s.listener = listener
	s.server = &http.Server{Addr: s.cfg.HTTPAddr, Handler: s.recover(mux), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		if err := s.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("http server stopped", "error", err)
		}
	}()
	return nil
}

func (s *HTTPServer) Shutdown(ctx context.Context) error {
	if s.server == nil {
		return nil
	}
	err := s.server.Shutdown(ctx)
	if err != nil && s.listener != nil {
		_ = s.server.Close()
	}
	return err
}

func (s *HTTPServer) recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.logger.Error("http panic recovered", "path", r.URL.Path)
				writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *HTTPServer) principal(w http.ResponseWriter, r *http.Request) (Principal, bool) {
	if s.auth == nil {
		writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "valid bearer token required")
		return Principal{}, false
	}
	principal, err := s.auth.Authenticate(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "valid bearer token required")
		return Principal{}, false
	}
	return principal, true
}

func (s *HTTPServer) requireInternal(w http.ResponseWriter, r *http.Request) bool {
	principal, ok := s.principal(w, r)
	if !ok {
		return false
	}
	if !principal.Roles["wallet-admin"] {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "internal wallet permission required")
		return false
	}
	return true
}

func (s *HTTPServer) createWallet(w http.ResponseWriter, r *http.Request) {
	if !s.requireInternal(w, r) {
		return
	}
	var input struct {
		PlayerID       string       `json:"playerId"`
		InitialBalance domain.Money `json:"initialBalance"`
	}
	if !decodeJSON(w, r, &input) {
		return
	}
	if uuid.Validate(input.PlayerID) != nil || !input.InitialBalance.Valid() || input.InitialBalance.MinorUnits() < 0 {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid wallet creation request")
		return
	}
	wallet, err := s.processor.CreateWallet(r.Context(), input.PlayerID, input.InitialBalance)
	if err != nil {
		if errors.Is(err, ErrWalletConflict) {
			writeError(w, http.StatusConflict, "WALLET_EXISTS", "wallet already exists for player and currency")
			return
		}
		s.writeFailure(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, wallet)
}

func (s *HTTPServer) getWallet(w http.ResponseWriter, r *http.Request) {
	if !s.requireInternal(w, r) {
		return
	}
	walletID := r.PathValue("walletID")
	if uuid.Validate(walletID) != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid wallet id")
		return
	}
	wallet, err := s.processor.GetWallet(r.Context(), walletID)
	if err != nil {
		s.writeFailure(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, wallet)
}

func (s *HTTPServer) ledger(w http.ResponseWriter, r *http.Request) {
	if !s.requireInternal(w, r) {
		return
	}
	walletID := r.PathValue("walletID")
	if uuid.Validate(walletID) != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid wallet id")
		return
	}
	page, err := s.processor.Ledger(r.Context(), walletID, r.URL.Query().Get("cursor"), parsePageLimit(r.URL.Query().Get("limit")))
	if err != nil {
		s.writeFailure(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *HTTPServer) reconcile(w http.ResponseWriter, r *http.Request) {
	if !s.requireInternal(w, r) {
		return
	}
	walletID := r.PathValue("walletID")
	if uuid.Validate(walletID) != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid wallet id")
		return
	}
	result, err := s.processor.Reconcile(r.Context(), walletID)
	if err != nil {
		s.writeFailure(w, r, err)
		return
	}
	if !result.Consistent {
		s.metrics.reconciliation.Add(1)
		s.logger.Error("wallet ledger mismatch", "walletId", walletID, "checkedEntries", result.CheckedEntries)
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *HTTPServer) submitWager(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.principal(w, r)
	if !ok {
		return
	}
	if principal.ProviderID == "" {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "provider identity is required")
		return
	}
	if !principal.Roles["wager-provider"] || principal.Roles["wallet-admin"] {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "provider transaction permission required")
		return
	}
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" {
		writeError(w, http.StatusBadRequest, "IDEMPOTENCY_KEY_REQUIRED", "Idempotency-Key header is required")
		return
	}
	var input struct {
		ProviderID                     string           `json:"providerId"`
		ExternalTransactionID          string           `json:"externalTransactionId"`
		PlayerID                       string           `json:"playerId"`
		WalletID                       string           `json:"walletId"`
		RoundID                        string           `json:"roundId"`
		GameID                         string           `json:"gameId"`
		Kind                           domain.WagerKind `json:"kind"`
		Money                          domain.Money     `json:"money"`
		ReferenceExternalTransactionID string           `json:"referenceExternalTransactionId,omitempty"`
	}
	if !decodeJSON(w, r, &input) {
		return
	}
	if input.ProviderID != principal.ProviderID {
		writeError(w, http.StatusForbidden, "PROVIDER_MISMATCH", "provider identity does not match token")
		return
	}
	request := domain.WagerRequest{ProviderID: principal.ProviderID, ExternalTransactionID: input.ExternalTransactionID, IdempotencyKey: key, PlayerID: input.PlayerID, WalletID: input.WalletID, RoundID: input.RoundID, GameID: input.GameID, Kind: input.Kind, Money: input.Money, ReferenceExternalTransactionID: input.ReferenceExternalTransactionID}
	if uuid.Validate(request.PlayerID) != nil || uuid.Validate(request.WalletID) != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "playerId and walletId must be UUIDs")
		return
	}
	result, err := s.processor.Process(r.Context(), request, "", "", "")
	if err != nil {
		s.writeFailure(w, r, err)
		return
	}
	status := http.StatusOK
	switch result.Status {
	case "PENDING", "PENDING_REFERENCE":
		status = http.StatusAccepted
	case "REJECTED":
		status = http.StatusUnprocessableEntity
	}
	if !result.IdempotentReplay && status == http.StatusOK {
		status = http.StatusCreated
	}
	writeJSON(w, status, result)
}

func (s *HTTPServer) getProviderTransaction(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.principal(w, r)
	if !ok {
		return
	}
	providerID := r.PathValue("providerID")
	if !principal.Roles["wager-provider"] || principal.ProviderID == "" || providerID != principal.ProviderID {
		writeError(w, http.StatusForbidden, "PROVIDER_MISMATCH", "providers may access only their own transactions")
		return
	}
	result, err := s.processor.GetTransaction(r.Context(), principal.ProviderID, r.PathValue("externalTransactionID"))
	if err != nil {
		s.writeFailure(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *HTTPServer) getTransactionByID(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.principal(w, r)
	if !ok {
		return
	}
	transactionID := r.PathValue("transactionID")
	if uuid.Validate(transactionID) != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid transaction id")
		return
	}
	if principal.Roles["wallet-admin"] {
		result, err := s.processor.GetTransactionByAnyID(r.Context(), transactionID)
		if err != nil {
			s.writeFailure(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
		return
	}
	if !principal.Roles["wager-provider"] || principal.ProviderID == "" {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "provider identity or internal role required")
		return
	}
	result, err := s.processor.GetTransactionByID(r.Context(), principal.ProviderID, transactionID)
	if err != nil {
		s.writeFailure(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *HTTPServer) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if s.pool == nil || s.sqs == nil {
		writeError(w, http.StatusServiceUnavailable, "NOT_READY", "dependencies unavailable")
		return
	}
	if err := s.pool.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "NOT_READY", "postgres unavailable")
		return
	}
	for _, queueURL := range []string{s.cfg.SQSRequestQueueURL, s.cfg.SQSEventQueueURL, s.cfg.SQSDLQQueueURL} {
		if queueURL == "" {
			writeError(w, http.StatusServiceUnavailable, "NOT_READY", "sqs unavailable")
			return
		}
		if _, err := s.sqs.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(queueURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}}); err != nil {
			writeError(w, http.StatusServiceUnavailable, "NOT_READY", "sqs unavailable")
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *HTTPServer) writeFailure(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ErrIdempotencyConflict), errors.Is(err, ErrMessageConflict), errors.Is(err, ErrWalletConflict):
		writeError(w, http.StatusConflict, "CONFLICT", "idempotency or resource conflict")
	case errors.Is(err, domain.ErrInvalidWager), errors.Is(err, domain.ErrInvalidMoney), errors.Is(err, domain.ErrNegativeMoneyInput), errors.Is(err, domain.ErrCurrencyMismatch), errors.Is(err, domain.ErrReferenceRequired), errors.Is(err, domain.ErrMoneyOverflow), errors.Is(err, ErrInvalidCursor):
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "request violates the operation contract")
	case errors.Is(err, ErrWalletNotFound), errors.Is(err, pgx.ErrNoRows):
		writeError(w, http.StatusNotFound, "NOT_FOUND", "resource not found")
	case errors.Is(err, ErrUnauthorized):
		writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "valid bearer token required")
	default:
		if s.metrics != nil {
			s.metrics.retries.Add(1)
		}
		if s.logger != nil {
			s.logger.Error("request failed", "path", r.URL.Path, "error", err)
		}
		writeError(w, http.StatusServiceUnavailable, "TEMPORARILY_UNAVAILABLE", "retry the request with the same idempotency key")
	}
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", "request body is invalid")
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", "request must contain one JSON value")
		return false
	}
	return true
}

func parsePageLimit(raw string) int {
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 || value > 200 {
		return 50
	}
	return value
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]string{"code": code, "message": message})
}
