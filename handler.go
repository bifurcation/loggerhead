package loggerhead

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	frontierSelectQ = "SELECT * FROM frontier ORDER BY index;"
	frontierDeleteQ = "DELETE FROM frontier;"
	frontierInsertQ = "INSERT INTO frontier VALUES ($1, $2, $3);"

	certInsertQ = "INSERT INTO certificates VALUES ($1, $2, $3, $4);"
)

type LogHandler struct {
	db *sql.DB
}

type addChainRequest struct {
	Chain []string `json:"chain"`
}

func readfrontier(tx *sql.Tx) (*frontier, error) {
	f := frontier{}

	rows, err := tx.Query(frontierSelectQ)
	if err != nil {
		return nil, err
	}

	next := uint64(0)
	var index uint64
	var subtreeSize uint64
	var value []byte
	for rows.Next() {
		err = rows.Scan(&index, &subtreeSize, &value)
		if err != nil {
			return nil, err
		}

		if index != next {
			return nil, fmt.Errorf("Row returned out of order [%d] != [%d]", index, next)
		}

		next += 1
		f = append(f, frontierEntry{subtreeSize, value})
	}

	return &f, nil
}

func logCertificate(tx *sql.Tx, timestamp, treeSize uint64, treeHead, cert []byte) error {
	_, err := tx.Exec(certInsertQ, timestamp, treeSize, treeHead, cert)
	return err
}

func writefrontier(tx *sql.Tx, f *frontier) error {
	_, err := tx.Exec(frontierDeleteQ)
	if err != nil {
		return err
	}

	for i, entry := range *f {
		_, err = tx.Exec(frontierInsertQ, i, entry.SubtreeSize, entry.Value)
		if err != nil {
			return err
		}
	}

	return nil
}

func (lh *LogHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	// Extract certificate from request
	// XXX: No verification of input certificate
	//  - No check that it parses as valid X.509
	//  - No verification of the certificate chain
	//  - No deduplication
	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		response.WriteHeader(http.StatusBadRequest)
		response.Write([]byte(fmt.Sprintf("Failed to read body: %v", err)))
		return
	}

	ctRequest := addChainRequest{}
	err = json.Unmarshal(body, &ctRequest)
	if err != nil {
		response.WriteHeader(http.StatusBadRequest)
		response.Write([]byte(fmt.Sprintf("Failed to parse body: %v", err)))
		return
	}

	if len(ctRequest.Chain) == 0 {
		response.WriteHeader(http.StatusBadRequest)
		response.Write([]byte("No certificates provided in body"))
		return
	}

	cert, err := base64.StdEncoding.DecodeString(ctRequest.Chain[0])
	if err != nil {
		response.WriteHeader(http.StatusBadRequest)
		response.Write([]byte(fmt.Sprintf("Base64 decoding failed: %v", err)))
		return
	}

	// Update the DB
	tx, err := lh.db.Begin()
	if err != nil {
		response.WriteHeader(http.StatusServiceUnavailable)
		response.Write([]byte(fmt.Sprintf("Could not get DB transaction: %v", err)))
		return
	}

	// Get the frontier from the DB
	f, err := readfrontier(tx)
	if err != nil {
		tx.Rollback()
		response.WriteHeader(http.StatusInternalServerError)
		response.Write([]byte(fmt.Sprintf("Failed to fetch frontier: %v", err)))
		return
	}

	// Update the frontier with this certificate
	f.Add(cert)
	treeSize := f.Size()
	treeHead := f.Head()

	// Log the certificate
	timestamp := uint64(time.Now().Unix())
	err = logCertificate(tx, timestamp, treeSize, treeHead, cert)
	if err != nil {
		tx.Rollback()
		response.WriteHeader(http.StatusInternalServerError)
		response.Write([]byte(fmt.Sprintf("Failed to log certificate: %v", err)))
		return
	}

	// Update the frontier
	err = writefrontier(tx, f)
	if err != nil {
		tx.Rollback()
		response.WriteHeader(http.StatusInternalServerError)
		response.Write([]byte(fmt.Sprintf("Failed to log certificate: %v", err)))
		return
	}

	// Commit the changes
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		response.WriteHeader(http.StatusInternalServerError)
		response.Write([]byte(fmt.Sprintf("Failed to commit changes: %v", err)))
		return
	}

	// XXX: Should sign and return SCT
	response.WriteHeader(http.StatusOK)
}
