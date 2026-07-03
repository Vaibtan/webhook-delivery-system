// Command server is the single binary for the webhook delivery system. It runs
// the HTTP API, the background workers (added in later slices), and — gated by
// the -migrate flag — the database migration tooling. Bundling migrate into the
// same binary keeps the distroless deploy image dependency-free.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Vaibtan/webhook-delivery-system/internal/api"
	"github.com/Vaibtan/webhook-delivery-system/internal/config"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/breaker"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/cache"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/metrics"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/queue"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/ratelimit"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/registry"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/safedial"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/scheduler"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/semaphore"
	"github.com/Vaibtan/webhook-delivery-system/internal/store"
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

	// Bounded in-memory registries (idle-TTL swept + evicted on subscription
	// delete): per-sub rate buckets, per-sub concurrency semaphores, per-URL
	// circuit breakers (plan Adv §3).
	bucketReg := registry.New(func(string) *ratelimit.TokenBucket {
		return ratelimit.NewTokenBucket(cfg.RateLimitPerSec, cfg.RateLimitBurst)
	}, cfg.RegistryIdleTTL)
	semReg := registry.New(func(string) *semaphore.Semaphore {
		return semaphore.New(cfg.PerSubConcurrency)
	}, cfg.RegistryIdleTTL)
	breakerReg := registry.New(func(string) *breaker.CircuitBreaker {
		return breaker.New(cfg.BreakerThreshold, cfg.BreakerResetTimeout)
	}, cfg.RegistryIdleTTL)

	// Read-through subscription cache; ingest + delivery read subs through it.
	subCache := cache.NewSubscriptionCache(rdb, subscriptionRepo, cfg.CacheTTL)

	// Unified eviction (delete-on-write): cache + per-sub registries. Called from
	// every subscription write path (update/delete/rotate, and auto-disable).
	evictSubscription := func(subID string) {
		ectx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := subCache.Evict(ectx, subID); err != nil {
			slog.Warn("evict subscription cache failed", "subscription_id", subID, "error", err)
		}
		bucketReg.Remove(subID)
		semReg.Remove(subID)
	}

	// Observability: delivery metrics collector (latency histogram + expvar
	// counters). Published to expvar once, here at the composition root.
	collector := metrics.NewCollector()
	metrics.PublishExpvar(collector)

	// Hardened, SSRF-safe HTTP client for all outbound deliveries.
	httpClient := safedial.NewHardenedClient(cfg.WebhookTimeout, cfg.AllowHTTPURLs)

	// The breaker + per-sub concurrency gates are wired only in async mode (their
	// requeues are processed by the pool/scheduler). Construction is one shot —
	// absent options default to no-ops inside the deliverer.
	delivererOpts := []worker.Option{
		worker.WithEvictHook(evictSubscription),
		worker.WithMetrics(collector),
	}
	if !cfg.SyncDelivery {
		delivererOpts = append(delivererOpts,
			worker.WithBreakerRegistry(func(url string) worker.Breaker { return breakerReg.Get(url) }),
			worker.WithConcurrencyRegistry(func(subID string) worker.Sem { return semReg.Get(subID) }),
		)
	}
	deliverer := worker.NewDeliverer(deliveryLogRepo, subCache, retrySched, dlq, httpClient, worker.DelivererConfig{
		VisibilityTimeout:    cfg.VisibilityTimeout,
		MaxRetryAttempts:     cfg.MaxRetryAttempts,
		RetryBaseDelay:       cfg.RetryBaseDelay,
		RetryMaxDelay:        cfg.RetryMaxDelay,
		BreakerResetTimeout:  cfg.BreakerResetTimeout,
		AutoDisableThreshold: cfg.AutoDisableThreshold,
	}, delivererOpts...)

	// In async mode (the default) the background pipeline runs: a bounded worker
	// pool drains the queue, the retry poller re-enqueues due retries, the
	// recovery scanner reclaims orphaned pending rows, the cleanup worker reaps
	// expired rows, and the DLQ reconciler heals the LIST↔flag index.
	var (
		pool          *worker.Pool
		retryPoller   *worker.RetrySchedulerWorker
		recoveryScan  *worker.RecoveryWorker
		cleanupWorker *worker.CleanupWorker
		dlqReconciler *worker.DLQReconciler
	)
	if !cfg.SyncDelivery {
		pool = worker.NewPool(taskQueue, deliverer, cfg.WorkerConcurrency, cfg.DrainTimeout)
		retryPoller = worker.NewRetrySchedulerWorker(retrySched, time.Second)
		recoveryScan = worker.NewRecoveryWorker(deliveryLogRepo, taskQueue, cfg.RecoveryScanInterval, 100)
		cleanupWorker = worker.NewCleanupWorker(deliveryLogRepo, cfg.LogRetention, cfg.IdempotencyTTL, time.Hour)
		dlqReconciler = worker.NewDLQReconciler(deliveryLogRepo, dlq, cfg.DLQMaxAge, cfg.RecoveryScanInterval)
	}

	if cfg.AdminAPIKey == "" {
		slog.Warn("ADMIN_API_KEY is unset; management endpoints will fail closed (503)")
	}

	srv := api.NewServer(api.Options{
		PingDB:      dbPool.Ping,
		PingRedis:   func(ctx context.Context) error { return rdb.Ping(ctx).Err() },
		WorkerReady: workerReady(pool),

		Subscriptions: subCache,
		DeliveryLogs:  deliveryLogRepo,

		SyncDelivery: cfg.SyncDelivery,
		Deliverer:    deliverer,
		TaskQueue:    taskQueue,
		DLQ:          dlq,

		RateLimitAllow:    func(subID string) bool { return bucketReg.Get(subID).Allow() },
		EvictSubscription: evictSubscription,

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

	httpServer := &http.Server{
		Addr:              cfg.Addr(),
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Supervisor: HTTP server + graceful shutdown + worker pool. gctx is cancelled
	// on SIGTERM (parent ctx) or the first fatal error.
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		slog.Info("http server listening", "addr", cfg.Addr())
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		slog.Info("shutdown signal received; draining")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.DrainTimeout)
		defer cancel()
		return httpServer.Shutdown(shutdownCtx)
	})
	if pool != nil {
		g.Go(func() error {
			slog.Info("worker pool starting", "concurrency", cfg.WorkerConcurrency)
			return pool.Start(gctx) // stops dequeuing on gctx cancel, then drains in-flight
		})
		g.Go(func() error {
			slog.Info("retry scheduler starting")
			return retryPoller.Run(gctx)
		})
		g.Go(func() error {
			slog.Info("recovery scanner starting", "interval", cfg.RecoveryScanInterval)
			return recoveryScan.Run(gctx)
		})
		g.Go(func() error {
			slog.Info("cleanup worker starting", "retention", cfg.LogRetention)
			return cleanupWorker.Run(gctx)
		})
		g.Go(func() error {
			slog.Info("dlq reconciler starting")
			return dlqReconciler.Run(gctx)
		})
	}

	// Idle-eviction sweep for the in-memory registries (runs in both modes).
	g.Go(func() error {
		return runSweep(gctx, sweepInterval(cfg.RegistryIdleTTL), bucketReg, semReg, breakerReg)
	})

	if err := g.Wait(); err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}
	slog.Info("shutdown complete")
	return nil
}

// sweeper is the subset of registry.Registry the sweep loop needs (the concrete
// generic types all satisfy it regardless of their element type).
type sweeper interface {
	Sweep() int
	Len() int
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

// workerReady backs the worker-aware /ready probe. In sync mode (no pool) it
// returns nil so readiness falls back to dependency reachability.
func workerReady(pool *worker.Pool) func() bool {
	if pool == nil {
		return nil
	}
	return pool.Running
}
