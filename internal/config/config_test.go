package config

import (
	"path/filepath"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	// An empty env file path means "do not read a .env file". We clear the
	// relevant vars so the test sees pure defaults.
	for _, k := range []string{"HTTP_PORT", "APP_ENV", "REDIS_ADDR", "LLM_MODEL"} {
		t.Setenv(k, "")
	}

	cfg, err := Load(filepath.Join(t.TempDir(), "does-not-exist"))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.HTTP.Port != 8080 {
		t.Errorf("HTTP.Port = %d, want 8080", cfg.HTTP.Port)
	}
	if cfg.App.Env != "development" {
		t.Errorf("App.Env = %q, want development", cfg.App.Env)
	}
	if cfg.DB.AutoMigrate != true {
		t.Errorf("DB.AutoMigrate = %v, want true", cfg.DB.AutoMigrate)
	}
	if cfg.HTTP.ReadTimeout != 15*time.Second {
		t.Errorf("HTTP.ReadTimeout = %v, want 15s", cfg.HTTP.ReadTimeout)
	}
}

func TestLoadOverridesFromEnv(t *testing.T) {
	t.Setenv("HTTP_PORT", "9999")
	t.Setenv("POSTGRES_DB", "custom_db")
	t.Setenv("LLM_TEMPERATURE", "0.75")
	t.Setenv("DB_AUTO_MIGRATE", "false")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.HTTP.Port != 9999 {
		t.Errorf("HTTP.Port = %d, want 9999", cfg.HTTP.Port)
	}
	if cfg.DB.Name != "custom_db" {
		t.Errorf("DB.Name = %q, want custom_db", cfg.DB.Name)
	}
	if cfg.LLM.Temperature != 0.75 {
		t.Errorf("LLM.Temperature = %v, want 0.75", cfg.LLM.Temperature)
	}
	if cfg.DB.AutoMigrate {
		t.Error("DB.AutoMigrate = true, want false")
	}
}

func TestValidateRejectsBadPort(t *testing.T) {
	t.Setenv("HTTP_PORT", "70000")
	if _, err := Load(""); err == nil {
		t.Fatal("Load() error = nil, want an out-of-range error")
	}
}

func TestValidateRequiresAPIKeyInProduction(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv("LLM_API_KEY", "")

	if _, err := Load(""); err == nil {
		t.Fatal("Load() error = nil, want a missing-API-key error")
	}
}

func TestReconcilerRejectsGraceShorterThanInterval(t *testing.T) {
	// The sweep would otherwise outrun normal queue latency and republish work
	// that is still in flight, causing duplicate LLM calls.
	t.Setenv("RECONCILER_ENABLED", "true")
	t.Setenv("RECONCILER_INTERVAL", "10m")
	t.Setenv("RECONCILER_PENDING_GRACE", "1m")

	if _, err := Load(""); err == nil {
		t.Fatal("Load() error = nil, want a pending-grace/interval validation error")
	}
}

func TestReconcilerAllowsGraceEqualToInterval(t *testing.T) {
	t.Setenv("RECONCILER_ENABLED", "true")
	t.Setenv("RECONCILER_INTERVAL", "5m")
	t.Setenv("RECONCILER_PENDING_GRACE", "5m")

	if _, err := Load(""); err != nil {
		t.Fatalf("Load() error = %v, want nil (equal values are allowed)", err)
	}
}

func TestReconcilerDefaultsAreValid(t *testing.T) {
	// The shipped defaults must themselves satisfy the invariant, or the app
	// would refuse to boot out of the box.
	for _, k := range []string{"RECONCILER_INTERVAL", "RECONCILER_PENDING_GRACE", "RECONCILER_ENABLED"} {
		t.Setenv(k, "")
	}

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load() error = %v, want nil with default reconciler settings", err)
	}
	if !cfg.Reconciler.Enabled {
		t.Error("Reconciler.Enabled = false, want true by default")
	}
	if cfg.Reconciler.PendingGrace < cfg.Reconciler.Interval {
		t.Errorf("default PendingGrace (%s) < Interval (%s)",
			cfg.Reconciler.PendingGrace, cfg.Reconciler.Interval)
	}
}

func TestDSN(t *testing.T) {
	d := DBConfig{Host: "h", Port: 5432, User: "u", Password: "p", Name: "n", SSLMode: "disable"}
	want := "host=h port=5432 user=u password=p dbname=n sslmode=disable"
	if got := d.DSN(); got != want {
		t.Errorf("DSN() = %q, want %q", got, want)
	}
}
