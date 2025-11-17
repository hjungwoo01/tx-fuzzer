package driver

import "context"

// Isolation represents a transaction isolation level for the DB session.
// Implementations should interpret "" (IsoDefault) as "use the DB's default".
type Isolation string

const (
	IsoDefault       Isolation = "" // use database default
	IsoReadCommitted Isolation = "READ COMMITTED"
	IsoRepeatable    Isolation = "REPEATABLE READ"
	IsoSerializable  Isolation = "SERIALIZABLE"
)

// Tx is a single database transaction context.
// All Exec/QueryRow calls occur within this transaction until Commit/Rollback.
type Tx interface {
	// Exec executes a statement and returns number of affected rows, if available.
	// Implementations may return -1 when the backend does not report this.
	Exec(ctx context.Context, sql string, args ...any) (int64, error)

	// QueryRow executes a query expected to return a single row.
	// The returned ScanRow's Scan must be called to read the row.
	// If no row exists, Scan should return an appropriate "no rows" error.
	QueryRow(ctx context.Context, sql string, args ...any) (ScanRow, error)

	// Commit commits the transaction. It must return an error on failure,
	// e.g., serialization error, deadlock, timeout, etc.
	Commit(ctx context.Context) error

	// Rollback aborts the transaction. It should be idempotent.
	Rollback(ctx context.Context) error
}

// ScanRow is a minimal row scanner abstraction returned by QueryRow.
type ScanRow interface {
	Scan(dest ...any) error
}

// Driver abstracts a database backend used by the runner.
// Implementations should be safe for concurrent use by multiple goroutines.
type Driver interface {
	// Begin starts a new transaction at the requested isolation level.
	// IsoDefault means "use the database default isolation".
	Begin(ctx context.Context, iso Isolation) (Tx, error)

	// Clock may return a database/server time (as text) for optional logging.
	// Implementations can return ("", nil) if not supported.
	Clock(ctx context.Context) (string, error)

	// InitSchema creates tables/indexes used by the runner, if they don't exist.
	InitSchema(ctx context.Context) error

	// Close releases resources (connections, pools, etc.).
	Close()
}
