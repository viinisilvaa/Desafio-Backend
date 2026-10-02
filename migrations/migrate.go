package migrations

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func Run(ctx context.Context, databaseURL, direction string) error {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err = pool.Ping(ctx); err != nil {
		return err
	}
	connection, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer connection.Release()
	if _, err = connection.Exec(ctx, `SELECT pg_advisory_lock(817263541)`); err != nil {
		return err
	}
	defer func() {
		unlockContext, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = connection.Exec(unlockContext, `SELECT pg_advisory_unlock(817263541)`)
	}()
	if _, err = pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations(version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	if direction == "down" {
		return rollbackOne(ctx, pool)
	}
	if direction != "up" {
		return fmt.Errorf("migration direction must be up or down")
	}
	entries, err := files.ReadDir(".")
	if err != nil {
		return err
	}
	versions := make([]string, 0)
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasSuffix(name, ".up.sql") {
			versions = append(versions, strings.TrimSuffix(name, ".up.sql"))
		}
	}
	sort.Strings(versions)
	for _, version := range versions {
		var exists bool
		if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, version).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		sql, readErr := files.ReadFile(version + ".up.sql")
		if readErr != nil {
			return readErr
		}
		tx, beginErr := pool.BeginTx(ctx, pgx.TxOptions{})
		if beginErr != nil {
			return beginErr
		}
		if _, err = tx.Exec(ctx, string(sql), pgx.QueryExecModeSimpleProtocol); err == nil {
			_, err = tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES ($1)`, version)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", version, err)
		}
		if err = tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

func rollbackOne(ctx context.Context, pool *pgxpool.Pool) error {
	var version string
	err := pool.QueryRow(ctx, `SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1`).Scan(&version)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil
		}
		return err
	}
	sql, err := files.ReadFile(version + ".down.sql")
	if err != nil {
		return err
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, string(sql), pgx.QueryExecModeSimpleProtocol); err == nil {
		_, err = tx.Exec(ctx, `DELETE FROM schema_migrations WHERE version=$1`, version)
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("rollback migration %s: %w", version, err)
	}
	return tx.Commit(ctx)
}
