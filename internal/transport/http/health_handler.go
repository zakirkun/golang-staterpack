package http

import (
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// HealthHandler reports liveness and readiness separately: liveness must not
// depend on backends (or a slow DB would get healthy pods killed), readiness
// must.
type HealthHandler struct {
	db  *gorm.DB
	rdb *redis.Client
}

// NewHealthHandler wires the dependencies used for readiness checks.
func NewHealthHandler(db *gorm.DB, rdb *redis.Client) *HealthHandler {
	return &HealthHandler{db: db, rdb: rdb}
}

// Live handles GET /health/live - process is up.
func (h *HealthHandler) Live(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{"status": "ok"})
}

// Ready handles GET /health/ready - dependencies are reachable.
func (h *HealthHandler) Ready(c *fiber.Ctx) error {
	ctx, cancel := contextWithTimeout(c, 2*time.Second)
	defer cancel()

	checks := fiber.Map{}
	healthy := true

	if sqlDB, err := h.db.DB(); err != nil {
		checks["postgres"], healthy = "unavailable", false
	} else if err := sqlDB.PingContext(ctx); err != nil {
		checks["postgres"], healthy = "unavailable", false
	} else {
		checks["postgres"] = "ok"
	}

	if err := h.rdb.Ping(ctx).Err(); err != nil {
		checks["redis"], healthy = "unavailable", false
	} else {
		checks["redis"] = "ok"
	}

	status := fiber.StatusOK
	overall := "ok"
	if !healthy {
		status, overall = fiber.StatusServiceUnavailable, "degraded"
	}

	return c.Status(status).JSON(fiber.Map{"status": overall, "checks": checks})
}
