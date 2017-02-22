package loggerhead

import (
	"database/sql"
	_ "github.com/lib/pq"
	"testing"
)

const (
	driver           = "postgres"
	connectionString = "user=rbarnes dbname=rbarnes sslmode=disable"
	certDeleteQ      = "DELETE FROM certificates;"
)

func getDB() (*sql.DB, error) {
	return sql.Open(driver, connectionString)
}

func clearDB(db *sql.DB) error {
	_, err := db.Exec(frontierDeleteQ)
	if err != nil {
		return err
	}

	_, err = db.Exec(certDeleteQ)
	return err
}

func TestDB(t *testing.T) {
	db, err := getDB()
	if err != nil {
		t.Fatalf("Error opening DB: %v", err)
	}

	_, err = db.Exec("INSERT INTO frontier VALUES ($1, $2, $3);", 1, 1, []byte{0, 1, 2, 3})
	if err != nil {
		t.Fatalf("Error inserting into table: %v", err)
	}

	err = clearDB(db)
	if err != nil {
		t.Fatalf("Error clearing DB: %v", err)
	}
}
