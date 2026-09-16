package http

import (
	"errors"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	"github.com/example/golang-staterpack/internal/repository"
	"github.com/example/golang-staterpack/internal/service"
)

// RequestIDHeader is echoed on every response.
const RequestIDHeader = "X-Request-ID"

// RequestID assigns (or honours an inbound) correlation ID.
func RequestID() fiber.Handler {
	return func(c *fiber.Ctx) error {
		id := c.Get(RequestIDHeader)
		if id == "" {
			id = uuid.NewString()
		}
		c.Locals("request_id", id)
		c.Set(RequestIDHeader, id)
		return c.Next()
	}
}

// ErrorBody is the uniform error envelope.
type ErrorBody struct {
	Error   string `json:"error"`
	Code    string `json:"code"`
	Details string `json:"details,omitempty"`
	TraceID string `json:"trace_id,omitempty"`
}

// ErrorHandler converts returned errors into a consistent JSON shape so no
// handler has to hand-roll status selection. includeDetail should be true only
// outside production, where leaking internal error strings is acceptable.
func ErrorHandler(includeDetail bool) fiber.ErrorHandler {
	return func(c *fiber.Ctx, err error) error {
		code := fiber.StatusInternalServerError
		body := ErrorBody{
			Error:   "internal server error",
			Code:    "internal_error",
			TraceID: traceID(c),
		}

		var fe *fiber.Error
		switch {
		case errors.As(err, &fe):
			code = fe.Code
			body.Error = fe.Message
		case errors.Is(err, repository.ErrNotFound):
			code = fiber.StatusNotFound
			body.Error, body.Code = "resource not found", "not_found"
		case errors.Is(err, service.ErrInvalidInput):
			code = fiber.StatusBadRequest
			body.Error, body.Code = err.Error(), "invalid_input"
		default:
			if includeDetail {
				body.Details = err.Error()
			}
		}

		c.Set("Content-Type", "application/json")
		return c.Status(code).JSON(body)
	}
}

// NotFound is the catch-all for unmatched routes.
func NotFound(c *fiber.Ctx) error {
	return c.Status(fiber.StatusNotFound).JSON(ErrorBody{
		Error:   "route not found",
		Code:    "route_not_found",
		TraceID: traceID(c),
	})
}

// RateLimiter is a fixed-window limiter backed by Redis. It fails open: if
// Redis is unreachable the request is allowed rather than the whole API
// going down with a cache.
func RateLimiter(increment func(key string, window time.Duration) (int64, error), limit int, window time.Duration) fiber.Handler {
	return func(c *fiber.Ctx) error {
		key := "ratelimit:" + c.IP()

		count, err := increment(key, window)
		if err != nil {
			return c.Next() // fail open
		}

		remaining := limit - int(count)
		if remaining < 0 {
			remaining = 0
		}
		c.Set("X-RateLimit-Limit", itoa(limit))
		c.Set("X-RateLimit-Remaining", itoa(remaining))

		if int(count) > limit {
			c.Set("Retry-After", itoa(int(window.Seconds())))
			return c.Status(fiber.StatusTooManyRequests).JSON(ErrorBody{
				Error:   "rate limit exceeded",
				Code:    "rate_limited",
				TraceID: traceID(c),
			})
		}
		return c.Next()
	}
}

// Recoverer replaces Fiber's default panic handler with a JSON one.
func Recoverer() fiber.Handler {
	return func(c *fiber.Ctx) error {
		defer func() {
			if r := recover(); r != nil {
				c.Set("Content-Type", "application/json")
				_ = c.Status(fiber.StatusInternalServerError).JSON(ErrorBody{
					Error:   "internal server error",
					Code:    "panic",
					TraceID: traceID(c),
				})
			}
		}()
		return c.Next()
	}
}

func traceID(c *fiber.Ctx) string {
	if id, ok := c.Locals("request_id").(string); ok {
		return id
	}
	return ""
}

func itoa(n int) string { return strconv.Itoa(n) }
