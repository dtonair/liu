package store

import (
	"context"
	"os"
	"testing"
)

// pgTestURL returns the test database URL or skips if none is configured, so
// `go test ./...` passes without a database while CI (which sets the env) runs
// the full Postgres contract.
func pgTestURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("LIU_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("LIU_TEST_DATABASE_URL not set; skipping Postgres-backed tests")
	}
	return url
}

func truncateAll(t *testing.T, s *PgStore) {
	t.Helper()
	_, err := s.pool.Exec(context.Background(),
		`TRUNCATE workflow_definitions, workflow_instances, workflow_history, tasks, timers, signals, outbox RESTART IDENTITY`)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

func TestPgStoreContract(t *testing.T) {
	url := pgTestURL(t)
	ctx := context.Background()

	// Migrate once up front.
	migrator, err := NewPgStore(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	migrator.Close()

	// Each subtest gets a freshly-truncated store with its own pool, so the
	// per-subtest `defer s.Close()` only closes that pool.
	RunStoreContract(t, func() Store {
		s, err := NewPgStore(ctx, url)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		truncateAll(t, s)
		return s
	})
}
