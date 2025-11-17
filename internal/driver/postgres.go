package driver

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type pgDrv struct{ pool *pgxpool.Pool }
type pgTx struct{ tx pgx.Tx }

func NewPostgres(dsn string) (Driver, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &pgDrv{pool: pool}, nil
}

func (d *pgDrv) Close() { d.pool.Close() }

func (d *pgDrv) Begin(ctx context.Context, iso Isolation) (Tx, error) {
	opts := pgx.TxOptions{}
	switch iso {
	case IsoSerializable:
		opts.IsoLevel = pgx.Serializable
	case IsoRepeatable:
		opts.IsoLevel = pgx.RepeatableRead
	case IsoReadCommitted:
		opts.IsoLevel = pgx.ReadCommitted
	}
	tx, err := d.pool.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &pgTx{tx: tx}, nil
}

func (t *pgTx) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	ct, err := t.tx.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return ct.RowsAffected(), nil
}

type row struct{ r pgx.Row }

func (t *pgTx) QueryRow(ctx context.Context, sql string, args ...any) (ScanRow, error) {
	return row{r: t.tx.QueryRow(ctx, sql, args...)}, nil
}
func (r row) Scan(dest ...any) error { return r.r.Scan(dest...) }

func (t *pgTx) Commit(ctx context.Context) error   { return t.tx.Commit(ctx) }
func (t *pgTx) Rollback(ctx context.Context) error { return t.tx.Rollback(ctx) }

func (d *pgDrv) Clock(ctx context.Context) (string, error) {
	var ts string
	err := d.pool.QueryRow(ctx, "SELECT clock_timestamp()::text").Scan(&ts)
	return ts, err
}

func (d *pgDrv) InitSchema(ctx context.Context) error {
	ddl := `
CREATE TABLE IF NOT EXISTS kv (
  k INT PRIMARY KEY,
  v BIGINT NOT NULL DEFAULT 0
);
-- seed a small integer keyspace for quick runs
INSERT INTO kv(k, v)
SELECT i, 0
FROM generate_series(0, 9) AS s(i)
ON CONFLICT (k) DO NOTHING;`
	_, err := d.pool.Exec(ctx, ddl)
	return err
}
