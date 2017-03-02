package loggerhead

import (
	"crypto/rand"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"testing"
	"time"
)

const (
	actuallyRun      = false
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

func execOrFatal(t *testing.T, db *sql.DB, query string, args ...interface{}) {
	_, err := db.Exec(query, args...)
	if err != nil {
		t.Fatalf("Error executing [%s]: %v", query, err)
	}
}

func TestDBSpeed(t *testing.T) {
	if !actuallyRun {
		return
	}

	iterations := 1000

	delFrontierQ := "DELETE FROM frontier;"
	delCertificatesQ := "DELETE FROM certificates;"
	selFrontierQ := "SELECT * FROM frontier;"
	insFrontierQ := "INSERT INTO FRONTIER VALUES ($1, $2);"
	insCertificatesQ := "INSERT INTO CERTIFICATES VALUES ($1, $2, $3, $4)"
	//peekQ := "SELECT * FROM certificates ORDER BY tree_size DESC LIMIT 1;"
	//pushQ := "INSERT INTO certificates VALUES ($1, $2, $3, $4)"

	cert := make([]byte, 2048)
	node := make([]byte, 32)
	rand.Read(cert)
	rand.Read(node)

	db, err := getDB()
	if err != nil {
		t.Fatalf("Error opening DB: %v", err)
	}

	// Clear the DB
	execOrFatal(t, db, delFrontierQ)
	execOrFatal(t, db, delCertificatesQ)

	// Run a bunch of test transactions
	elapsed := float64(0.0)
	for i := 0; i < iterations; i += 1 {
		enter := time.Now()

		// Option 1: Two-table, replace one; append to the other
		/*
			execOrFatal(t, db, selFrontierQ)
			execOrFatal(t, db, delFrontierQ)

			m := i
			s := 1
			for m > 0 {
				if m&1 == 1 {
					execOrFatal(t, db, insFrontierQ, s, node)
				}
				m >>= 1
				s <<= 1
			}

			execOrFatal(t, db, insCertificatesQ, i+1, 1, node, cert)
		*/

		/*
			// Option 1b: ... with frontier pre-serialized
			execOrFatal(t, db, selFrontierQ)
			execOrFatal(t, db, delFrontierQ)
			execOrFatal(t, db, insFrontierQ, i, cert)
			execOrFatal(t, db, insCertificatesQ, i+1, 1, node, cert)
		*/

		// Option 1c: ... with transactions
		tx, _ := db.Begin()
		tx.Exec(selFrontierQ)
		tx.Exec(insFrontierQ, i, cert)
		tx.Exec(insCertificatesQ, i+1, 1, node, cert)
		tx.Commit()

		/*
			// Option 2: One table, pure append
			execOrFatal(t, db, peekQ)
			execOrFatal(t, db, pushQ, i+1, 1, cert, cert)
		*/

		/*
			// Option 2b: ... with transactions
			//execOrFatal(t, db, peekQ)
			//execOrFatal(t, db, pushQ, i+1, 1, cert, cert)
			tx, _ := db.Begin()
			tx.Exec(peekQ)
			tx.Exec(pushQ, i+1, 1, cert, cert)
			tx.Commit()
		*/

		exit := time.Now()
		elapsed += float64(exit.Sub(enter)) / float64(time.Second)
	}
	fmt.Printf("total time: %f\n", elapsed)
	fmt.Printf("queries/s : %f\n", float64(iterations)/elapsed)
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
