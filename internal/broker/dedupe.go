package broker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// DedupeKeyPrefix namespaces the processed-message markers in Redis.
const DedupeKeyPrefix = "processed:event:"

// Dedupe is an at-most-once guard for message handling, backed by Redis
// SETNX + TTL.
//
// TRADEOFF, stated plainly: this guard lives in Redis, so it is only as durable
// as the cache. A Redis flush, eviction, or failover silently restores the
// ability to double-process a message, and nothing will surface that as an
// error. If duplicate side effects are unacceptable (they cost money here --
// every duplicate is a second paid LLM call), replace this with a durable
// processed_events table in Postgres. The interface is deliberately narrow so
// that swap touches one call site.
type Dedupe struct {
	client *redis.Client
	ttl    time.Duration
}

// NewDedupe builds a guard. ttl bounds how long a marker is remembered and
// should comfortably exceed the longest possible retry window.
func NewDedupe(client *redis.Client, ttl time.Duration) *Dedupe {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return &Dedupe{client: client, ttl: ttl}
}

// Claim attempts to mark eventID as handled, returning true when this caller
// won the race and should proceed.
//
// It FAILS OPEN (returns true) when Redis is unreachable: a cache outage must
// not stop the queue draining, even though that re-exposes the double-process
// risk. The failure is logged so it is visible rather than silent.
func (d *Dedupe) Claim(ctx context.Context, eventID string) bool {
	if eventID == "" {
		// Nothing to key on; treat as unclaimable rather than permanently
		// suppressing every message with a missing ID.
		return true
	}

	key := DedupeKeyPrefix + eventID
	ok, err := d.client.SetNX(ctx, key, time.Now().UTC().Format(time.RFC3339Nano), d.ttl).Result()
	if err != nil {
		slog.Warn("dedupe claim failed, processing anyway (may duplicate)", "event_id", eventID, "err", err)
		return true
	}
	return ok
}

// Release removes a marker, used when a handler decides the message must be
// retried and should be allowed to run again.
func (d *Dedupe) Release(ctx context.Context, eventID string) {
	if eventID == "" {
		return
	}
	if err := d.client.Del(ctx, DedupeKeyPrefix+eventID).Err(); err != nil && !errors.Is(err, redis.Nil) {
		slog.Warn("dedupe release failed; message may be skipped after retry", "event_id", eventID, "err", err)
	}
}

// ErrAlreadyProcessed is returned by handlers that wish to signal a duplicate
// was dropped without treating it as a failure.
var ErrAlreadyProcessed = errors.New("broker: event already processed")
