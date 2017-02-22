package loggerhead

import (
	"database/sql"
	_ "github.com/lib/pq"
)

const (
	driver = "postgres"

	// This works for a local connection for testing; do not use in production
	connectionString = "user=rbarnes dbname=rbarnes sslmode=disable"
)

func getDB() (*sql.DB, error) {
	return sql.Open(driver, connectionString)
}
