// Package security provides API authentication (JWT/OIDC bearer) and tenant
// scoping for the workflow engine control plane (spec FR14, security NFR).
package security

import "context"

type ctxKey int

const tenantKey ctxKey = iota

// WithTenant returns a context carrying the authenticated tenant ID.
func WithTenant(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, tenantKey, tenantID)
}

// TenantFrom extracts the authenticated tenant ID from the context. The bool is
// false if no tenant is present (request was not authenticated).
func TenantFrom(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(tenantKey).(string)
	return v, ok && v != ""
}
