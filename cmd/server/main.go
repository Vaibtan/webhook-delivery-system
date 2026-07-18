// Command server runs the HTTP API, delivery workers, and migration tooling.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Vaibtan/webhook-delivery-system/internal/api"
	"github.com/Vaibtan/webhook-delivery-system/internal/config"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/breaker"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/metrics"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/queue"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/registry"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/safedial"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/scheduler"
	"github.com/Vaibtan/webhook-delivery-system/internal/lifecycle"
	"github.com/Vaibtan/webhook-delivery-system/internal/store"
	"github.com/Vaibtan/webhook-delivery-system/internal/subscription"
	"github.com/Vaibtan/webhook-delivery-system/internal/worker"
)

func main() {
	migrateMode := flag.String("migrate", "", "run migrations and exit: \"up\" applies all, \"down\" rolls back one step")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if err := run(*migrateMode); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(migrateMode string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	// Migration-only modes: run and exit (used by `make migrate-up/down` and as a
	// separable deploy step).
	switch migrateMode {
	case "up":
		slog.Info("running migrations: up")
		if err := store.MigrateUp(cfg.DatabaseURL); err != nil {
			return err
		}
		slog.Info("migrations up: complete")
		return nil
	case "down":
		slog.Info("running migrations: down (one step)")
		if err := store.MigrateDown(cfg.DatabaseURL); err != nil {
			return err
		}
		slog.Info("migrations down: complete")
		return nil
	case "":
		// normal boot
	default:
		return fmt.Errorf("invalid -migrate value %q (want \"up\" or \"down\")", migrateMode)
	}

	// Root context cancelled on SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Migrate-on-startup so a fresh deploy is self-provisioning.
	slog.Info("applying migrations on startup")
	if err := store.MigrateUp(cfg.DatabaseURL); err != nil {
		return err
	}

	dbPool, err := store.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer dbPool.Close()
	slog.Info("connected to postgres")

	rdb, err := store.NewRedis(ctx, cfg.RedisURL)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	slog.Info("connected to redis")

	subscriptionRepo := store.NewSubscriptionRepo(dbPool)
	deliveryLogRepo := store.NewDeliveryLogRepo(dbPool)
	taskQueue := queue.New(rdb)
	retrySched := scheduler.New(rdb)
	dlq := queue.NewDLQ(rdb)

	breakerReg := registry.New(func(string) *breaker.CircuitBreaker {
		return breaker.New(cfg.BreakerThreshold, cfg.BreakerResetTimeout)
	}, cfg.RegistryIdleTTL)

	subscriptionState := subscription.New(rdb, subscriptionRepo, deliveryLogRepo, subscription.Config{
		CacheTTL:         cfg.CacheTTL,
		RegistryIdleTTL:  cfg.RegistryIdleTTL,
		RateLimitPerSec:  cfg.RateLimitPerSec,
		RateLimitBurst:   cfg.RateLimitBurst,
		ConcurrencyLimit: cfg.PerSubConcurrency,
	})

	// Observability: delivery metrics collector (latency histogram + expvar
	// counters). Published to expvar once, here at the composition root.
	collector := metrics.NewCollector()
	metrics.PublishExpvar(collector)

	// Hardened, SSRF-safe HTTP client for all outbound deliveries.
	httpClient := safedial.NewHardenedClient(cfg.WebhookTimeout, cfg.AllowHTTPURLs)

	// The background scheduler/pool runs in both modes, so retry, breaker, and
	// concurrency reschedules are always durable. SYNC_DELIVERY controls only the
	// first attempt made by the ingest request.
	deliverer := worker.NewDeliverer(deliveryLogRepo, subscriptionState, retrySched, dlq, httpClient, worker.DelivererConfig{
		VisibilityTimeout:    cfg.VisibilityTimeout,
		MaxRetryAttempts:     cfg.MaxRetryAttempts,
		RetryBaseDelay:       cfg.RetryBaseDelay,
		RetryMaxDelay:        cfg.RetryMaxDelay,
		BreakerResetTimeout:  cfg.BreakerResetTimeout,
		AutoDisableThreshold: cfg.AutoDisableThreshold,
	}, worker.DeliveryControls{
		BreakerFor:   func(url string) worker.Breaker { return breakerReg.Get(url) },
		SemaphoreFor: func(subID string) worker.Sem { return subscriptionState.Semaphore(subID) },
		Metrics:      collector,
	})

	// The background pipeline always runs. In sync mode it handles successors
	// created by an in-request first attempt, as well as recovery and maintenance.
	pool := worker.NewPool(taskQueue, deliverer, cfg.WorkerConcurrency, cfg.DrainTimeout)
	retryPoller := worker.NewRetrySchedulerWorker(retrySched, time.Second)
	recoveryScan := worker.NewRecoveryWorker(
		deliveryLogRepo, taskQueue, cfg.RecoveryScanInterval, 100, cfg.OrphanThreshold,
	)
	cleanupWorker := worker.NewCleanupWorker(deliveryLogRepo, cfg.LogRetention, cfg.IdempotencyTTL, time.Hour)
	dlqReconciler := worker.NewDLQReconciler(deliveryLogRepo, dlq, cfg.DLQMaxAge, cfg.RecoveryScanInterval)

	if cfg.AdminAPIKey == "" {
		slog.Warn("ADMIN_API_KEY is unset; management endpoints will fail closed (503)")
	}

	srv := api.NewServer(api.Options{
		PingDB:      dbPool.Ping,
		PingRedis:   func(ctx context.Context) error { return rdb.Ping(ctx).Err() },
		WorkerReady: pool.Running,

		Subscriptions:  subscriptionState,
		DeliveryIngest: deliveryLogRepo,
		DeliveryStatus: deliveryLogRepo,
		DeliveryDLQ:    deliveryLogRepo,

		SyncDelivery: cfg.SyncDelivery,
		Deliverer:    deliverer,
		TaskQueue:    taskQueue,
		DLQ:          dlq,

		RateLimitAllow: subscriptionState.Allow,

		Metrics: collector,
		BreakerStates: func() map[string]string {
			states := make(map[string]string)
			for url, cb := range breakerReg.Snapshot() {
				states[url] = cb.State()
			}
			return states
		},
		EnablePprof: cfg.EnablePprof,

		AdminAPIKey:          cfg.AdminAPIKey,
		AllowHTTPURLs:        cfg.AllowHTTPURLs,
		CORSAllowedOrigins:   cfg.CORSAllowedOrigins,
		SignatureDriftWindow: cfg.SignatureDriftWindow,
		SecretGraceWindow:    cfg.SecretGraceWindow,
	})

	httpServer := newHTTPServer(cfg.Addr(), srv.Handler())
	supervisor := lifecycle.New(
		httpServer,
		cfg.DrainTimeout,
		lifecycle.Process{Name: "worker pool", Run: pool.Start},
		lifecycle.Process{Name: "retry scheduler", Run: retryPoller.Run},
		lifecycle.Process{Name: "recovery scanner", Run: recoveryScan.Run},
		lifecycle.Process{Name: "cleanup worker", Run: cleanupWorker.Run},
		lifecycle.Process{Name: "dlq reconciler", Run: dlqReconciler.Run},
		lifecycle.Process{
			Name: "registry sweeper",
			Run: func(ctx context.Context) error {
				return runSweep(ctx, sweepInterval(cfg.RegistryIdleTTL), subscriptionState, breakerReg)
			},
		},
	)

	slog.Info("http server listening", "addr", cfg.Addr())
	if err := supervisor.Run(ctx); err != nil {
		return err
	}
	slog.Info("shutdown complete")
	return nil
}

// newHTTPServer centralizes inbound resource limits. MaxBytesReader bounds body
// size in the handler; these deadlines additionally bound how long a client may
// occupy a connection while slowly sending headers/body or receiving a response.
func newHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
}

// sweeper is the subset of registry.Registry the sweep loop needs (the concrete
// generic types all satisfy it regardless of their element type).
type sweeper interface {
	Sweep() int
}

func sweepInterval(idleTTL time.Duration) time.Duration {
	d := idleTTL / 2
	if d < time.Minute {
		return time.Minute
	}
	return d
}

func runSweep(ctx context.Context, interval time.Duration, sweepers ...sweeper) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			for _, s := range sweepers {
				if n := s.Sweep(); n > 0 {
					slog.Debug("registry sweep evicted idle entries", "count", n)
				}
			}
		}
	}
}
