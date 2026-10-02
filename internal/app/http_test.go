package app

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type staticAuthenticator struct {
	principal Principal
	err       error
}

func (a staticAuthenticator) Authenticate(_ *http.Request) (Principal, error) {
	return a.principal, a.err
}

func TestProviderCannotSubmitAsAnotherProvider(t *testing.T) {
	server := &HTTPServer{
		auth:    staticAuthenticator{principal: Principal{ProviderID: "provider-a", Roles: map[string]bool{"wager-provider": true}}},
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		metrics: NewMetrics(),
	}
	requestBody := `{"providerId":"provider-b","externalTransactionId":"external-1","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37","roundId":"round-1","gameId":"game-1","kind":"BET","money":{"amount":"1.00","currency":"BRL"}}`
	request := httptest.NewRequest("POST", "/wagering/transactions", strings.NewReader(requestBody))
	request.Header.Set("Idempotency-Key", "key-1")
	response := httptest.NewRecorder()
	server.submitWager(response, request)
	if response.Code != 403 {
		t.Fatalf("status=%d, want 403", response.Code)
	}
}

func TestInternalClientCannotUseProviderEndpoint(t *testing.T) {
	server := &HTTPServer{
		auth:    staticAuthenticator{principal: Principal{ProviderID: "internal", Roles: map[string]bool{"wallet-admin": true}}},
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		metrics: NewMetrics(),
	}
	request := httptest.NewRequest("POST", "/wagering/transactions", strings.NewReader(`{}`))
	request.Header.Set("Idempotency-Key", "key-1")
	response := httptest.NewRecorder()
	server.submitWager(response, request)
	if response.Code != 403 {
		t.Fatalf("status=%d, want 403", response.Code)
	}
}

func TestProviderCannotQueryAnotherProvider(t *testing.T) {
	server := &HTTPServer{
		auth:    staticAuthenticator{principal: Principal{ProviderID: "provider-a", Roles: map[string]bool{"wager-provider": true}}},
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		metrics: NewMetrics(),
	}
	request := httptest.NewRequest("GET", "/providers/provider-b/wagering/transactions/external-1", nil)
	request.SetPathValue("providerID", "provider-b")
	request.SetPathValue("externalTransactionID", "external-1")
	response := httptest.NewRecorder()
	server.getProviderTransaction(response, request)
	if response.Code != 403 {
		t.Fatalf("status=%d, want 403", response.Code)
	}
}

func TestMissingAuthenticationIsRejected(t *testing.T) {
	server := &HTTPServer{auth: staticAuthenticator{err: ErrUnauthorized}}
	response := httptest.NewRecorder()
	_, ok := server.principal(response, httptest.NewRequest("GET", "/", nil))
	if ok || response.Code != 401 {
		t.Fatalf("authenticated=%v status=%d, want false/401", ok, response.Code)
	}
}

func TestCreateWalletInvalidJSONWritesSingleError(t *testing.T) {
	server := &HTTPServer{
		auth:    staticAuthenticator{principal: Principal{Roles: map[string]bool{"wallet-admin": true}}},
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		metrics: NewMetrics(),
	}
	request := httptest.NewRequest("POST", "/wallets", strings.NewReader("{"))
	response := httptest.NewRecorder()
	server.createWallet(response, request)
	if response.Code != 400 {
		t.Fatalf("status=%d, want 400", response.Code)
	}
	decoder := json.NewDecoder(response.Body)
	var body map[string]string
	if err := decoder.Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["code"] != "INVALID_JSON" {
		t.Fatalf("error code=%q, want INVALID_JSON", body["code"])
	}
	if err := decoder.Decode(&map[string]string{}); err != io.EOF {
		t.Fatalf("response contains more than one JSON object: %v", err)
	}
}

func TestParsePageLimitRejectsMalformedValues(t *testing.T) {
	for _, value := range []string{"", "0", "201", "20garbage", "-5"} {
		if got := parsePageLimit(value); got != 50 {
			t.Errorf("parsePageLimit(%q)=%d, want default 50", value, got)
		}
	}
	if got := parsePageLimit("25"); got != 25 {
		t.Fatalf("parsePageLimit(25)=%d, want 25", got)
	}
}

func TestReadyReportsNotReadyWhenDependenciesAreMissing(t *testing.T) {
	server := &HTTPServer{
		cfg:     Config{HTTPAddr: "127.0.0.1:0"},
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		metrics: NewMetrics(),
	}
	request := httptest.NewRequest("GET", "/health/ready", nil)
	response := httptest.NewRecorder()
	server.ready(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want %d", response.Code, http.StatusServiceUnavailable)
	}
}

func TestHTTPServerLifecycleStartsAndStopsListener(t *testing.T) {
	server := &HTTPServer{
		cfg:     Config{HTTPAddr: "127.0.0.1:0"},
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		metrics: NewMetrics(),
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	address := server.listener.Addr().String()
	request, err := http.Get("http://" + address + "/health/live")
	if err != nil {
		t.Fatalf("health request failed after startup: %v", err)
	}
	if request.StatusCode != http.StatusOK {
		t.Fatalf("liveness status=%d, want 200", request.StatusCode)
	}
	_ = request.Body.Close()
	shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = server.Shutdown(shutdownContext); err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 200 * time.Millisecond}
	if response, requestErr := client.Get("http://" + address + "/health/live"); requestErr == nil {
		_ = response.Body.Close()
		t.Fatal("listener still accepted requests after shutdown")
	}
}
