// Command worker runs an out-of-engine task worker that polls the engine for
// the demo order_approval activities and executes them.
package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/dtonair/liu/telemetry"
	"github.com/dtonair/liu/worker"
)

var version = "dev"

func main() {
	log := telemetry.NewLogger(env("LIU_LOG_LEVEL", "info"))
	log.Info("starting liu worker", "version", version)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	client := worker.NewClient(env("LIU_ENGINE_URL", "http://localhost:6789"), hostname())
	client.TenantID = env("LIU_TENANT_ID", "demo")
	client.Token = os.Getenv("LIU_WORKER_TOKEN")

	runner := worker.NewRunner(client, worker.RunnerOptions{
		Concurrency:  envInt("LIU_WORKER_CONCURRENCY", 8),
		LeaseSeconds: envInt("LIU_LEASE_SECONDS", 30),
		Logger:       log,
	})

	// Activity types are configurable; default to the demo workflow's set.
	activities := env("LIU_ACTIVITY_TYPES", "reserve_inventory,capture_payment,release_inventory")
	registerActivities(runner, activities)

	if err := runner.Run(ctx); err != nil {
		log.Error("worker exited", "error", err)
		os.Exit(1)
	}
}

// registerActivities binds the demo handlers for the configured activity set.
func registerActivities(r *worker.Runner, csv string) {
	want := map[string]bool{}
	for _, a := range strings.Split(csv, ",") {
		want[strings.TrimSpace(a)] = true
	}
	for name, h := range worker.OrderApprovalHandlers() {
		if want[name] {
			r.Register(name, h)
		}
	}
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "worker"
	}
	return h
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n := 0
	for _, c := range v {
		if c < '0' || c > '9' {
			return def
		}
		n = n*10 + int(c-'0')
	}
	return n
}
