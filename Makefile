.PHONY: build test test-pg lint vet up down migrate run-engine run-worker chaos tidy

GO ?= go
PKG ?= ./...
DATABASE_URL ?= postgres://liu:liu@localhost:5432/liu?sslmode=disable

build:
	$(GO) build ./...

test:
	$(GO) test $(PKG)

# Runs the full suite including Postgres-backed tests (requires `make up`).
test-pg:
	LIU_TEST_DATABASE_URL="$(DATABASE_URL)" $(GO) test -tags=postgres $(PKG)

vet:
	$(GO) vet ./...

lint:
	golangci-lint run

tidy:
	$(GO) mod tidy

up:
	docker compose up -d postgres

down:
	docker compose down -v

migrate:
	LIU_DATABASE_URL="$(DATABASE_URL)" $(GO) run ./cmd/engine -migrate-only

run-engine:
	LIU_DATABASE_URL="$(DATABASE_URL)" LIU_AUTH_DISABLED=true LIU_MIGRATE_ON_BOOT=true $(GO) run ./cmd/engine

run-worker:
	LIU_ENGINE_URL=http://localhost:8080 LIU_TENANT_ID=demo \
	LIU_ACTIVITY_TYPES=reserve_inventory,capture_payment,release_inventory $(GO) run ./cmd/worker

chaos:
	$(GO) test -tags=postgres -run Chaos -v ./internal/engine
