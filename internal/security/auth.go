package security

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// Authenticator authenticates API requests and injects the tenant into context.
//
// In production it validates an HS256 bearer JWT and reads the "tenant_id"
// claim. For local development (Disabled), it trusts the X-Tenant-ID header so
// the engine can run without an identity provider.
type Authenticator struct {
	Disabled bool
	Secret   []byte
}

// Middleware authenticates the request, rejecting unauthenticated calls with
// 401, and stores the tenant in the request context.
func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenant, err := a.authenticate(r)
		if err != nil {
			writeError(w, http.StatusUnauthorized, err.Error())
			return
		}
		next.ServeHTTP(w, r.WithContext(WithTenant(r.Context(), tenant)))
	})
}

func (a *Authenticator) authenticate(r *http.Request) (string, error) {
	if a.Disabled {
		tenant := r.Header.Get("X-Tenant-ID")
		if tenant == "" {
			return "", errMissingTenant
		}
		return tenant, nil
	}

	authz := r.Header.Get("Authorization")
	raw, ok := strings.CutPrefix(authz, "Bearer ")
	if !ok || raw == "" {
		return "", errMissingBearer
	}
	token, err := jwt.Parse(raw, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errBadSigning
		}
		return a.Secret, nil
	})
	if err != nil || !token.Valid {
		return "", errInvalidToken
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return "", errInvalidToken
	}
	tenant, _ := claims["tenant_id"].(string)
	if tenant == "" {
		return "", errMissingTenantClaim
	}
	return tenant, nil
}

// authErr is a small sentinel error type with a stable message.
type authErr string

func (e authErr) Error() string { return string(e) }

const (
	errMissingTenant      authErr = "missing X-Tenant-ID header"
	errMissingBearer      authErr = "missing bearer token"
	errBadSigning         authErr = "unexpected token signing method"
	errInvalidToken       authErr = "invalid token"
	errMissingTenantClaim authErr = "token missing tenant_id claim"
)

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
