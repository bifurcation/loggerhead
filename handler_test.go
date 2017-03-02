package loggerhead

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func fatalIfNotNil(t *testing.T, err error, msg string) {
	if err != nil {
		t.Fatalf("%s: %v", msg, err)
	}
}

func TestHandler(t *testing.T) {
	method := "POST"
	path := "/ct/v1/add-chain"
	certs := []string{
		"8TpFNrV+YbVkOX6VRjDoxKGb32DNgBo0nPNgOvivsnA=",
		"HNlVdY13CCavI+R8L4SQbwfbZmgMR1INW70mejX3LQU=",
		"cs0n2iQFlKV3AQ6eW9UuWwhmGb1n/D/BEH1D8S02D40=",
	}
	certSelectQ := "SELECT * FROM certificates WHERE tree_size = $1;"

	db, err := getDB()
	fatalIfNotNil(t, err, "getDB")

	handler := LogHandler{DB: db}
	d := [][]byte{}
	for i, cert := range certs {
		certDER, _ := base64.StdEncoding.DecodeString(cert)
		d = append(d, leafHash(certDER))

		body := fmt.Sprintf(`{"chain":["%s"]}`, cert)
		req := httptest.NewRequest(method, path, bytes.NewReader([]byte(body)))
		resp := httptest.NewRecorder()

		handler.ServeHTTP(resp, req)

		// Check that the request succeeded
		if resp.Code != http.StatusOK {
			t.Fatalf("Request failed: [%d] [%d] [%s]", i, resp.Code, string(resp.Body.Bytes()))
		}

		// Check that the certificates table looks correct
		tx, err := db.Begin()
		fatalIfNotNil(t, err, "db.Begin")

		now := uint64(time.Now().Unix())
		var timestamp, treeSize uint64
		var frontierBuf, certData []byte
		err = tx.QueryRow(certSelectQ, i+1).Scan(&timestamp, &treeSize, &frontierBuf, &certData)
		fatalIfNotNil(t, err, "Error finding certificate")

		if now-timestamp > 1 /* seconds */ {
			t.Fatalf("Incorrect timestamp [%d] != [%d]", timestamp, now)
		}

		if !bytes.Equal(certDER, certData) {
			t.Fatalf("Incorrect cert data [%x] != [%x]", certDER, certData)
		}

		f := frontier{}
		err = f.Unmarshal(frontierBuf)
		fatalIfNotNil(t, err, "Error unmarshaling frontier")
		if f.Size() != treeSize {
			t.Fatalf("Inconsistent tree size: [%d] != [%d]", f.Size(), treeSize)
		}

		tx.Rollback()
	}

	err = clearDB(db)
	if err != nil {
		t.Fatal("clearDB:", err)
	}
}
