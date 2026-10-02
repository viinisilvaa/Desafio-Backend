package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

var ErrUnauthorized = errors.New("unauthorized")

type Principal struct {
	ProviderID string
	Roles      map[string]bool
}

type Authenticator struct {
	verifier *oidc.IDTokenVerifier
	audience string
}

func NewAuthenticator(cfg Config) (*Authenticator, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	provider, err := oidc.NewProvider(ctx, cfg.OIDCIssuerURL)
	if err != nil {
		return nil, fmt.Errorf("discover OIDC issuer: %w", err)
	}
	return &Authenticator{verifier: provider.Verifier(&oidc.Config{SkipClientIDCheck: true}), audience: cfg.OIDCAudience}, nil
}

func (a *Authenticator) Authenticate(request *http.Request) (Principal, error) {
	header := request.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		return Principal{}, ErrUnauthorized
	}
	token, err := a.verifier.Verify(request.Context(), strings.TrimPrefix(header, "Bearer "))
	if err != nil {
		return Principal{}, ErrUnauthorized
	}
	if !containsString(token.Audience, a.audience) {
		return Principal{}, ErrUnauthorized
	}
	var claims struct {
		ProviderID  string `json:"provider_id"`
		RealmAccess struct {
			Roles []string `json:"roles"`
		} `json:"realm_access"`
	}
	if err = token.Claims(&claims); err != nil {
		return Principal{}, ErrUnauthorized
	}
	roles := make(map[string]bool, len(claims.RealmAccess.Roles))
	for _, role := range claims.RealmAccess.Roles {
		roles[role] = true
	}
	return Principal{ProviderID: claims.ProviderID, Roles: roles}, nil
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
