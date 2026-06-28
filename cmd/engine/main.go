// Command engine runs the workflow engine control plane: the HTTP API plus the
// background loops (scheduler, timer loop, lease sweeper, outbox publisher).
package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/dtonair/liu/internal/api"
	"github.com/dtonair/liu/internal/engine"
	"github.com/dtonair/liu/internal/security"
	"github.com/dtonair/liu/internal/store"
	"github.com/dtonair/liu/internal/telemetry"
)

var version = "dev"

func main() {
	migrateOnly := flag.Bool("migrate-only", false, "apply migrations and exit")
	flag.Parse()

	log := telemetry.NewLogger(env("LIU_LOG_LEVEL", "info"))
	log.Info("starting liu engine", "version", version)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	dbURL := env("LIU_DATABASE_URL", "postgres://liu:liu@localhost:5432/liu?sslmode=disable")
	pg, err := store.NewPgStore(ctx, dbURL)
	if err != nil {
		log.Error("connect database", "error", err)
		os.Exit(1)
	}
	defer func() { _ = pg.Close() }()

	if *migrateOnly || envBool("LIU_MIGRATE_ON_BOOT", false) {
		if err := pg.Migrate(ctx); err != nil {
			log.Error("migrate", "error", err)
			os.Exit(1)
		}
		log.Info("migrations applied")
		if *migrateOnly {
			return
		}
	}

	metrics := telemetry.NewMetrics()
	eng := engine.New(pg, engine.WithLogger(log), engine.WithMetrics(metrics))

	// Leader election: only the advisory-lock holder runs the mutating loops, so
	// timers never double-fire across replicas. The API layer runs everywhere.
	const leaderKey = 0x11A20001
	lead, isLeader, err := store.AcquireLeadership(ctx, pg.Pool(), leaderKey)
	if err != nil {
		log.Error("acquire leadership", "error", err)
		os.Exit(1)
	}
	if isLeader {
		defer lead.Release(context.Background())
		log.Info("this replica is the scheduler leader")
		go runLoop(ctx, log, "scheduler", engine.NewScheduler(eng, 100*time.Millisecond, 100).Run)
		go runLoop(ctx, log, "timers", engine.NewTimerLoop(eng, 250*time.Millisecond, 100).Run)
		go runLoop(ctx, log, "sweeper", engine.NewLeaseSweeper(eng, time.Second).Run)
		go runLoop(ctx, log, "outbox", engine.NewOutboxPublisher(eng, engine.LogSink{Log: log}, 500*time.Millisecond, 100).Run)
		go runLoop(ctx, log, "sampler", engine.NewMetricsSampler(eng, 5*time.Second).Run)
	} else {
		log.Info("another replica holds scheduler leadership; running API only")
	}

	auth := &security.Authenticator{
		Disabled: envBool("LIU_AUTH_DISABLED", false),
		Secret:   []byte(env("LIU_JWT_SECRET", "")),
	}
	srv := api.NewServer(eng, pg, api.Options{
		Auth: auth,
		ReadyFn: func(ctx context.Context) error {
			return pg.Pool().Ping(ctx)
		},
	})

	addr := env("LIU_HTTP_ADDR", ":8080")
	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           srv.Router(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutCtx, c := context.WithTimeout(context.Background(), 15*time.Second)
		defer c()
		_ = httpSrv.Shutdown(shutCtx)
	}()

	certFile, keyFile := os.Getenv("LIU_TLS_CERT"), os.Getenv("LIU_TLS_KEY")
	log.Info("http listening", "addr", addr, "tls", certFile != "")
	if certFile != "" && keyFile != "" {
		err = httpSrv.ListenAndServeTLS(certFile, keyFile)
	} else {
		err = httpSrv.ListenAndServe()
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("http server", "error", err)
		os.Exit(1)
	}
	log.Info("engine stopped")
}

// runLoop runs a background loop, logging unexpected exits (context
// cancellation is expected on shutdown).
func runLoop(ctx context.Context, log loggerLike, name string, run func(context.Context) error) {
	if err := run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("background loop exited", "loop", name, "error", err)
	}
}

type loggerLike interface {
	Error(msg string, args ...any)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}
