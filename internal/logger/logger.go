// Package logger provides a thin structured-logging facade over the standard
// library slog, so call sites depend on one small surface rather than slog
// directly and the sink can be swapped in tests.
package logger

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

// Logger wraps *slog.Logger with a couple of convenience constructors.
type Logger struct {
	*slog.Logger
}

// New returns a logger writing JSON in production and human-readable text
// otherwise.
func New(level, env string) *Logger {
	var lv slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lv = slog.LevelDebug
	case "warn", "warning":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: lv}

	var h slog.Handler
	if env == "production" {
		h = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		h = slog.NewTextHandler(os.Stdout, opts)
	}

	return &Logger{Logger: slog.New(h)}
}

// With returns a logger with the given key/value pairs attached.
func (l *Logger) With(args ...any) *Logger {
	return &Logger{Logger: l.Logger.With(args...)}
}

// Fatal logs at error level and exits the process.
func (l *Logger) Fatal(msg string, args ...any) {
	l.Error(msg, args...)
	os.Exit(1)
}

// FromContext returns the logger stored in ctx, or a fallback if none is set.
func FromContext(ctx context.Context, fallback *Logger) *Logger {
	if ctx != nil {
		if l, ok := ctx.Value(loggerKey{}).(*Logger); ok && l != nil {
			return l
		}
	}
	return fallback
}

type loggerKey struct{}

// WithContext stores l in ctx.
func WithContext(ctx context.Context, l *Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, l)
}
