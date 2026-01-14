package runner

import (
	"errors"
	"regexp"

	"github.com/jackc/pgx/v5/pgconn"
)

var sqlStateRe = regexp.MustCompile(`SQLSTATE ([0-9A-Z]{5})`)

func addErrorDetails(details map[string]any, err error) map[string]any {
	if err == nil {
		return details
	}
	if details == nil {
		details = map[string]any{}
	}
	if code, ok := sqlStateFromError(err); ok {
		details["sqlstate"] = code
	}
	if pgErr := pgError(err); pgErr != nil {
		if pgErr.Detail != "" {
			details["pg_detail"] = pgErr.Detail
		}
		if pgErr.Hint != "" {
			details["pg_hint"] = pgErr.Hint
		}
		if pgErr.Where != "" {
			details["pg_where"] = pgErr.Where
		}
	}
	return details
}

func pgError(err error) *pgconn.PgError {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr
	}
	return nil
}

func sqlStateFromError(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	if pgErr := pgError(err); pgErr != nil && pgErr.Code != "" {
		return pgErr.Code, true
	}
	if match := sqlStateRe.FindStringSubmatch(err.Error()); len(match) == 2 {
		return match[1], true
	}
	return "", false
}
