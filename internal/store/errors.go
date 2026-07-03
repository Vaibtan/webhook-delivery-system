package store

import (
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// PostgreSQL SQLSTATE codes we special-case. Hardcoded (rather than importing
// jackc/pgerrcode) to keep go.mod's direct requires at exactly the three runtime
// packages.
const (
	sqlStateInvalidTextRepresentation = "22P02" // e.g. a malformed UUID literal
	sqlStateForeignKeyViolation       = "23503"
)

func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// isInvalidUUID reports whether err is Postgres rejecting a malformed UUID
// literal. Callers treat this as "no such row" for id-addressed lookups.
func isInvalidUUID(err error) bool { return pgCode(err) == sqlStateInvalidTextRepresentation }

// isForeignKeyViolation reports a foreign-key conflict (23503), e.g. ingest
// against a subscription id that does not exist.
func isForeignKeyViolation(err error) bool { return pgCode(err) == sqlStateForeignKeyViolation }

// isAbsent reports whether err means "the requested row is not there" for an
// id-addressed QueryRow().Scan lookup: either no row was returned
// (pgx.ErrNoRows) or the id was a malformed UUID literal (Postgres 22P02).
// Callers map this to ErrNotFound / an empty result. NOTE: this is correct only
// for QueryRow().Scan sites — pool.Query/Exec never emit pgx.ErrNoRows, so those
// keep using isInvalidUUID alone.
func isAbsent(err error) bool {
	return errors.Is(err, pgx.ErrNoRows) || isInvalidUUID(err)
}
