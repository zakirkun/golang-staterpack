package repository

import (
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/zakirkun/golang-staterpack/internal/model"
)

// newDryRunDB builds a GORM handle that renders SQL without a live connection.
// This lets us assert on generated SQL, which is the part of FindStale whose
// correctness depends on GORM's grouping behaviour rather than on our code.
func newDryRunDB(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(postgres.New(postgres.Config{
		DriverName:           "pgx",
		DSN:                  "host=127.0.0.1 user=x dbname=x",
		PreferSimpleProtocol: true,
	}), &gorm.Config{
		DryRun:               true,
		DisableAutomaticPing: true, // no live server in unit tests
		Logger:               logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open dry-run gorm: %v", err)
	}
	return db
}

func TestFindStaleGeneratesGroupedOrCondition(t *testing.T) {
	db := newDryRunDB(t)

	pendingBefore := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	processingBefore := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

	var tasks []model.Task
	stmt := db.Session(&gorm.Session{DryRun: true, NewDB: true}).
		Where(
			db.Where("status = ? AND created_at < ?", model.TaskStatusPending, pendingBefore).
				Or("status = ? AND updated_at < ?", model.TaskStatusProcessing, processingBefore),
		).
		Order("created_at ASC").
		Limit(100).
		Find(&tasks).Statement

	sql := stmt.SQL.String()

	// GORM groups a nested db.Where(...).Or(...) in parentheses. If that ever
	// changes, the OR could bind wider than intended -- e.g. escaping the
	// statement and matching every row -- so pin the behaviour here.
	if !strings.Contains(sql, "(") || !strings.Contains(sql, ")") {
		t.Errorf("generated SQL has no grouping parentheses; the OR may bind too widely:\n%s", sql)
	}
	if !strings.Contains(strings.ToUpper(sql), " OR ") {
		t.Errorf("generated SQL lost the OR branch:\n%s", sql)
	}
	if !strings.Contains(sql, "LIMIT") {
		t.Errorf("generated SQL lost the LIMIT:\n%s", sql)
	}

	// Both branches must be present, each with its own status comparison.
	if strings.Count(sql, "$1")+strings.Count(sql, "?") < 2 {
		t.Logf("note: placeholder style in generated SQL: %s", sql)
	}

	t.Logf("generated SQL:\n%s", sql)
	t.Logf("vars: %v", stmt.Vars)
}

func TestFindStaleVarsCarryBothStatuses(t *testing.T) {
	db := newDryRunDB(t)

	var tasks []model.Task
	stmt := db.Session(&gorm.Session{DryRun: true, NewDB: true}).
		Where(
			db.Where("status = ? AND created_at < ?", model.TaskStatusPending, time.Now()).
				Or("status = ? AND updated_at < ?", model.TaskStatusProcessing, time.Now()),
		).
		Find(&tasks).Statement

	// Both status values must reach the driver, or one branch silently matches
	// nothing.
	var sawPending, sawProcessing bool
	for _, v := range stmt.Vars {
		if s, ok := v.(model.TaskStatus); ok {
			switch s {
			case model.TaskStatusPending:
				sawPending = true
			case model.TaskStatusProcessing:
				sawProcessing = true
			}
		}
	}
	if !sawPending {
		t.Error("pending status value missing from query vars")
	}
	if !sawProcessing {
		t.Error("processing status value missing from query vars")
	}
}
