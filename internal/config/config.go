// Package config loads application configuration from environment variables
// (optionally seeded from a .env file) into a single validated struct.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// Config is the root configuration tree. Every field has a sane default so the
// application can boot in a local dev environment with nothing but a .env file.
type Config struct {
	App     AppConfig
	HTTP    HTTPConfig
	DB      DBConfig
	Redis   RedisConfig
	Broker  BrokerConfig
	LLM     LLMConfig
	Metrics MetricsConfig
}

type AppConfig struct {
	Env             string
	Name            string
	LogLevel        string
	ShutdownTimeout time.Duration
}

func (a AppConfig) IsProduction() bool { return a.Env == "production" }

type HTTPConfig struct {
	Port         int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

type DBConfig struct {
	Host            string
	Port            int
	User            string
	Password        string
	Name            string
	SSLMode         string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	AutoMigrate     bool
}

// DSN builds the Postgres connection string used by GORM.
func (d DBConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		d.Host, d.Port, d.User, d.Password, d.Name, d.SSLMode,
	)
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type BrokerConfig struct {
	URL      string
	Exchange string
	Prefetch int
}

type LLMConfig struct {
	Provider    string
	BaseURL     string
	APIKey      string
	Model       string
	Temperature float64
	MaxTokens   int
	Timeout     time.Duration
}

type MetricsConfig struct {
	Enabled bool
}

// Load reads configuration. If envFile is non-empty and exists it is loaded
// first, but real environment variables always win.
func Load(envFile string) (*Config, error) {
	if envFile != "" {
		if _, err := os.Stat(envFile); err == nil {
			if err := godotenv.Load(envFile); err != nil {
				return nil, fmt.Errorf("load %s: %w", envFile, err)
			}
		}
	}

	cfg := &Config{
		App: AppConfig{
			Env:             getEnv("APP_ENV", "development"),
			Name:            getEnv("APP_NAME", "golang-staterpack"),
			LogLevel:        getEnv("LOG_LEVEL", "debug"),
			ShutdownTimeout: getDuration("SHUTDOWN_TIMEOUT", 20*time.Second),
		},
		HTTP: HTTPConfig{
			Port:         getInt("HTTP_PORT", 8080),
			ReadTimeout:  getDuration("HTTP_READ_TIMEOUT", 15*time.Second),
			WriteTimeout: getDuration("HTTP_WRITE_TIMEOUT", 15*time.Second),
			IdleTimeout:  getDuration("HTTP_IDLE_TIMEOUT", 60*time.Second),
		},
		DB: DBConfig{
			Host:            getEnv("POSTGRES_HOST", "localhost"),
			Port:            getInt("POSTGRES_PORT", 5432),
			User:            getEnv("POSTGRES_USER", "app"),
			Password:        getEnv("POSTGRES_PASSWORD", "app"),
			Name:            getEnv("POSTGRES_DB", "app"),
			SSLMode:         getEnv("POSTGRES_SSLMODE", "disable"),
			MaxOpenConns:    getInt("POSTGRES_MAX_OPEN_CONNS", 25),
			MaxIdleConns:    getInt("POSTGRES_MAX_IDLE_CONNS", 5),
			ConnMaxLifetime: getDuration("POSTGRES_CONN_MAX_LIFETIME", 30*time.Minute),
			AutoMigrate:     getBool("DB_AUTO_MIGRATE", true),
		},
		Redis: RedisConfig{
			Addr:     getEnv("REDIS_ADDR", "localhost:6379"),
			Password: getEnv("REDIS_PASSWORD", ""),
			DB:       getInt("REDIS_DB", 0),
		},
		Broker: BrokerConfig{
			URL:      getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
			Exchange: getEnv("RABBITMQ_EXCHANGE", "app.events"),
			Prefetch: getInt("RABBITMQ_PREFETCH", 10),
		},
		LLM: LLMConfig{
			Provider:    getEnv("LLM_PROVIDER", "deepseek"),
			BaseURL:     getEnv("LLM_BASE_URL", "https://api.deepseek.com/v1"),
			APIKey:      getEnv("LLM_API_KEY", ""),
			Model:       getEnv("LLM_MODEL", "deepseek-v4.1-flash"),
			Temperature: getFloat("LLM_TEMPERATURE", 0.2),
			MaxTokens:   getInt("LLM_MAX_TOKENS", 2048),
			Timeout:     getDuration("LLM_TIMEOUT", 60*time.Second),
		},
		Metrics: MetricsConfig{
			Enabled: getBool("METRICS_ENABLED", true),
		},
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	if c.HTTP.Port <= 0 || c.HTTP.Port > 65535 {
		return fmt.Errorf("config: HTTP_PORT %d out of range", c.HTTP.Port)
	}
	if c.DB.Name == "" {
		return fmt.Errorf("config: POSTGRES_DB must not be empty")
	}
	if c.Broker.URL == "" {
		return fmt.Errorf("config: RABBITMQ_URL must not be empty")
	}
	if c.App.IsProduction() && c.LLM.APIKey == "" {
		return fmt.Errorf("config: LLM_API_KEY is required in production")
	}
	return nil
}

// --- primitive helpers -------------------------------------------------------

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func getInt(key string, fallback int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getFloat(key string, fallback float64) float64 {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

func getBool(key string, fallback bool) bool {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return fallback
}

func getDuration(key string, fallback time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
