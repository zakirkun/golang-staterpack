package broker

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// deadRedis points at a port nothing is listening on. Every call fails, which
// is the fail-open path we want to pin down.
func deadRedis() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: "127.0.0.1:0", MaxRetries: -1})
}

func TestClaimFailsOpenWhenRedisIsDown(t *testing.T) {
	d := NewDedupe(deadRedis(), time.Minute)

	// A cache outage must not stop the queue draining, even though it re-exposes
	// the duplicate-processing risk. If this ever returns false, a Redis blip
	// would silently drop every message.
	if !d.Claim(context.Background(), "evt-1") {
		t.Error("Claim() = false with a dead Redis, want true (fail open)")
	}
}

func TestClaimWithEmptyIDAlwaysProceeds(t *testing.T) {
	d := NewDedupe(deadRedis(), time.Minute)

	// An empty ID has nothing to key on. Returning false here would suppress
	// every such message permanently.
	if !d.Claim(context.Background(), "") {
		t.Error("Claim(\"\") = false, want true")
	}
}

func TestReleaseIsSafeWithDeadRedis(t *testing.T) {
	d := NewDedupe(deadRedis(), time.Minute)

	// Must not panic or block; a failed release only means a retry might be
	// suppressed.
	d.Release(context.Background(), "evt-1")
	d.Release(context.Background(), "")
}

func TestNewDedupeAppliesDefaultTTL(t *testing.T) {
	d := NewDedupe(deadRedis(), 0)
	if d.ttl <= 0 {
		t.Errorf("ttl = %v, want a positive default", d.ttl)
	}

	d = NewDedupe(deadRedis(), -time.Second)
	if d.ttl <= 0 {
		t.Errorf("ttl = %v with a negative input, want a positive default", d.ttl)
	}
}

func TestDedupeKeyPrefixIsNamespaced(t *testing.T) {
	if DedupeKeyPrefix == "" {
		t.Fatal("DedupeKeyPrefix must not be empty")
	}
}
