// Package http contains the Fiber transport: router, middleware and handlers.
package http

import (
	"context"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/zakirkun/golang-staterpack/internal/service"
)

// contextWithTimeout derives a context from the request's own context. Using
// c.UserContext() (rather than c.Context()) means cancellation propagates when
// the client disconnects, while still bounded by timeout.
func contextWithTimeout(c *fiber.Ctx, timeout time.Duration) (context.Context, context.CancelFunc) {
	base := c.UserContext()
	if base == nil {
		base = context.Background()
	}
	return context.WithTimeout(base, timeout)
}

// RouterDeps collects everything the router needs.
type RouterDeps struct {
	TaskService *service.TaskService
	Health      *HealthHandler
	RateLimit   fiber.Handler
}

// NewRouter registers all routes and returns the configured Fiber app.
func NewRouter(app *fiber.App, deps RouterDeps) *fiber.App {
	app.Use(RequestID())
	app.Use(Recoverer())

	taskHandler := NewTaskHandler(deps.TaskService)

	// Health endpoints sit outside the rate limiter so probes are never 429'd.
	app.Get("/health/live", deps.Health.Live)
	app.Get("/health/ready", deps.Health.Ready)

	api := app.Group("/api/v1")
	if deps.RateLimit != nil {
		api.Use(deps.RateLimit)
	}

	tasks := api.Group("/tasks")
	tasks.Post("/", taskHandler.Create)
	tasks.Get("/", taskHandler.List)
	tasks.Get("/:id", taskHandler.Get)
	tasks.Delete("/:id", taskHandler.Delete)

	app.Use(NotFound)
	return app
}
