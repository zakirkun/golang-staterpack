// Command api is the HTTP entrypoint for the service.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"

	"github.com/zakirkun/golang-staterpack/internal/broker"
	"github.com/zakirkun/golang-staterpack/internal/config"
	"github.com/zakirkun/golang-staterpack/internal/llm"
	"github.com/zakirkun/golang-staterpack/internal/logger"
	"github.com/zakirkun/golang-staterpack/internal/reconciler"
	"github.com/zakirkun/golang-staterpack/internal/repository"
	"github.com/zakirkun/golang-staterpack/internal/service"
	"github.com/zakirkun/golang-staterpack/internal/storage"
	transport "github.com/zakirkun/golang-staterpack/internal/transport/http"
	"github.com/zakirkun/golang-staterpack/internal/worker"
)

const (
	rateLimitRequests = 100
	rateLimitWindow   = 60 * time.Second
)

func main() {
	envFile := flag.String("env-file", ".env", "path to a .env file (optional)")
	migrateOnly := flag.Bool("migrate-only", false, "apply schema migrations and exit")
	flag.Parse()

	if err := run(*envFile, *migrateOnly); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(envFile string, migrateOnly bool) error {
	cfg, err := config.Load(envFile)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	log := logger.New(cfg.App.LogLevel, cfg.App.Env)
	slog.SetDefault(log.Logger)
	log.Info("starting", "app", cfg.App.Name, "env", cfg.App.Env)

	// Root context cancelled on SIGINT/SIGTERM; every long-lived component
	// derives from it so shutdown is a single cancellation.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- Postgres ---------------------------------------------------------
	db, err := storage.NewPostgres(ctx, cfg.DB, cfg.App.IsProduction())
	if err != nil {
		return err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("unwrap sql.DB: %w", err)
	}
	defer sqlDB.Close()
	log.Info("connected to postgres", "host", cfg.DB.Host, "db", cfg.DB.Name)

	if migrateOnly {
		if err := repository.AutoMigrate(db); err != nil {
			return err
		}
		log.Info("migrations applied, exiting")
		return nil
	}

	if cfg.DB.AutoMigrate {
		if err := repository.AutoMigrate(db); err != nil {
			return err
		}
		log.Info("auto-migration applied")
	}

	// --- Redis ------------------------------------------------------------
	rdb, err := storage.NewRedis(ctx, cfg.Redis)
	if err != nil {
		return err
	}
	defer rdb.Close()
	log.Info("connected to redis", "addr", cfg.Redis.Addr)

	// --- RabbitMQ ---------------------------------------------------------
	brk, err := broker.New(ctx, broker.Options{
		URL:      cfg.Broker.URL,
		Exchange: cfg.Broker.Exchange,
		Prefetch: cfg.Broker.Prefetch,
	})
	if err != nil {
		return err
	}
	defer brk.Close()
	log.Info("connected to rabbitmq", "exchange", cfg.Broker.Exchange)

	// --- LLM --------------------------------------------------------------
	// A missing key must not stop CRUD from serving; only async summarisation
	// degrades, so fall back to a no-op client instead of failing boot.
	llmClient, err := llm.New(cfg.LLM)
	if err != nil {
		log.Warn("LLM unavailable, summaries disabled", "err", err)
		llmClient = llm.Disabled(cfg.LLM.Model)
	} else {
		log.Info("LLM client ready", "provider", cfg.LLM.Provider, "model", cfg.LLM.Model)
	}

	// --- Wiring -----------------------------------------------------------
	taskRepo := repository.NewTaskRepository(db)
	taskSvc := service.NewTaskService(taskRepo, brk, rdb)

	dedupe := broker.NewDedupe(rdb, cfg.Reconciler.DedupeTTL)

	summarizer := worker.NewTaskSummarizer(taskSvc, llmClient, dedupe)
	consumer, err := summarizer.Register(brk)
	if err != nil {
		return fmt.Errorf("register summarizer worker: %w", err)
	}
	log.Info("summarizer worker registered", "queue", broker.QueueTaskEvents)

	// --- Reconciler -------------------------------------------------------
	// Rescues tasks whose publish was lost or whose worker died mid-flight.
	recon := reconciler.New(taskSvc, reconciler.Options{
		Interval:          cfg.Reconciler.Interval,
		PendingGrace:      cfg.Reconciler.PendingGrace,
		ProcessingTimeout: cfg.Reconciler.ProcessingTimeout,
		BatchSize:         cfg.Reconciler.BatchSize,
	})
	if cfg.Reconciler.Enabled {
		go recon.Run(ctx)
	} else {
		log.Warn("reconciler disabled; stranded tasks will not be rescued")
	}

	// --- HTTP -------------------------------------------------------------
	app := newFiberApp(cfg)
	app = transport.NewRouter(app, transport.RouterDeps{
		TaskService: taskSvc,
		Health:      transport.NewHealthHandler(db, rdb),
		RateLimit:   transport.RateLimiter(redisRateIncrementer(rdb), rateLimitRequests, rateLimitWindow),
	})

	serverErr := make(chan error, 1)
	go func() {
		addr := fmt.Sprintf(":%d", cfg.HTTP.Port)
		log.Info("http server listening", "addr", addr)
		if err := app.Listen(addr); err != nil && !errors.Is(err, fiber.ErrServiceUnavailable) {
			serverErr <- fmt.Errorf("http listen: %w", err)
		}
	}()

	// --- Wait for shutdown ------------------------------------------------
	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case err := <-serverErr:
		log.Error("http server failed", "err", err)
		return err
	case amqpErr := <-brk.Done():
		log.Error("broker connection lost", "err", amqpErr)
		return errors.New("broker connection lost")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.App.ShutdownTimeout)
	defer cancel()

	if err := app.ShutdownWithContext(shutdownCtx); err != nil {
		log.Error("http shutdown error", "err", err)
	}
	consumer.Stop()
	log.Info("shutdown complete")
	return nil
}

func newFiberApp(cfg *config.Config) *fiber.App {
	return fiber.New(fiber.Config{
		AppName:               cfg.App.Name,
		ReadTimeout:           cfg.HTTP.ReadTimeout,
		WriteTimeout:          cfg.HTTP.WriteTimeout,
		IdleTimeout:           cfg.HTTP.IdleTimeout,
		DisableStartupMessage: cfg.App.IsProduction(),
		ErrorHandler:          transport.ErrorHandler(!cfg.App.IsProduction()),
	})
}

// redisRateIncrementer returns a fixed-window counter for the rate limiter.
func redisRateIncrementer(rdb *redis.Client) func(key string, window time.Duration) (int64, error) {
	return func(key string, window time.Duration) (int64, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		pipe := rdb.TxPipeline()
		incr := pipe.Incr(ctx, key)
		pipe.Expire(ctx, key, window)
		if _, err := pipe.Exec(ctx); err != nil {
			return 0, err
		}
		return incr.Val(), nil
	}
}
