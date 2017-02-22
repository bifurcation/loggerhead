package loggerhead

import (
	"testing"
)

func TestDB(t *testing.T) {
	db, err := getDB()
	if err != nil {
		t.Fatalf("Error opening DB: %v", err)
	}

	_, err = db.Exec("CREATE TABLE frontier (index BIGINT, subtree_size BIGINT, subhead BYTEA);")
	if err != nil {
		t.Fatalf("Error creating table: %v", err)
	}

	_, err = db.Exec("INSERT INTO frontier VALUES ($1, $2, $3);", 1, 1, []byte{0, 1, 2, 3})
	if err != nil {
		t.Fatalf("Error inserting into table: %v", err)
	}

	_, err = db.Exec("DROP TABLE frontier;")
	if err != nil {
		t.Fatalf("Error dropping table: %v", err)
	}
}
