package app

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestOIDCAuthenticatorValidatesSignatureExpiryAndAudience(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"issuer":                                "http://" + r.Host,
				"jwks_uri":                              "http://" + r.Host + "/keys",
				"id_token_signing_alg_values_supported": []string{"RS256"},
			})
		case "/keys":
			_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
				"kty": "RSA", "kid": "test-key", "use": "sig", "alg": "RS256",
				"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
				"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
			}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	auth, err := NewAuthenticator(Config{OIDCIssuerURL: server.URL, OIDCAudience: "wager-api"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	valid := signTestIDToken(t, key, server.URL, "wager-api", now.Add(-time.Minute), now.Add(5*time.Minute))
	principal, err := auth.Authenticate(oidcTestRequest(valid))
	if err != nil {
		t.Fatalf("valid token rejected: %v", err)
	}
	if principal.ProviderID != "provider-test" || !principal.Roles["wager-provider"] {
		t.Fatalf("unexpected principal: %+v", principal)
	}

	expired := signTestIDToken(t, key, server.URL, "wager-api", now.Add(-2*time.Hour), now.Add(-time.Minute))
	if _, err = auth.Authenticate(oidcTestRequest(expired)); err == nil {
		t.Fatal("expired signed token was accepted")
	}
	wrongAudience := signTestIDToken(t, key, server.URL, "another-api", now.Add(-time.Minute), now.Add(5*time.Minute))
	if _, err = auth.Authenticate(oidcTestRequest(wrongAudience)); err != ErrUnauthorized {
		t.Fatalf("wrong audience error=%v, want ErrUnauthorized", err)
	}
	tampered := strings.Split(valid, ".")
	tampered[1] = base64.RawURLEncoding.EncodeToString([]byte(`{"iss":"tampered"}`))
	if _, err = auth.Authenticate(oidcTestRequest(strings.Join(tampered, "."))); err == nil {
		t.Fatal("token with invalid signature was accepted")
	}
	if _, err = auth.Authenticate(httptest.NewRequest(http.MethodGet, "/", nil)); err != ErrUnauthorized {
		t.Fatalf("missing bearer error=%v, want ErrUnauthorized", err)
	}
}

func oidcTestRequest(token string) *http.Request {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	return request
}

func signTestIDToken(t *testing.T, key *rsa.PrivateKey, issuer, audience string, issuedAt, expiresAt time.Time) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "RS256", "kid": "test-key", "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	claims, err := json.Marshal(map[string]any{
		"iss": issuer, "sub": "service-account", "aud": audience,
		"iat": issuedAt.Unix(), "exp": expiresAt.Unix(),
		"provider_id": "provider-test", "realm_access": map[string]any{"roles": []string{"wager-provider"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	encodedHeader := base64.RawURLEncoding.EncodeToString(header)
	encodedClaims := base64.RawURLEncoding.EncodeToString(claims)
	unsigned := encodedHeader + "." + encodedClaims
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature)
}
