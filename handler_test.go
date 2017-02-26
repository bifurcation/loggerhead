package loggerhead

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
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

	db, err := getDB()
	fatalIfNotNil(t, err, "getDB")

	handler := LogHandler{db}
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

		// Check that the frontier looks correct
		var f *frontier
		_, err := db.ReadWriteTransaction(ctx(), func(txn *spanner.ReadWriteTransaction) error {
			f, err = readFrontier(txn)
			return err
		})
		fatalIfNotNil(t, err, "Reading frontier")

		treeSize := f.Size()
		if treeSize != uint64(i+1) {
			t.Fatalf("Incorrect size [%d] != [%d]", treeSize, i+1)
		}

		mh := merkleTreeHead(d)
		fh := f.Head()
		if !bytes.Equal(mh, fh) {
			t.Fatalf("Incorrect frontier tree head [%x] != [%x]", mh, fh)
		}

		// Check that the certificates table looks correct
		now := time.Now().Unix()
		var timestamp, treeSize2 int64
		var treeHead, certData []byte

		cols := []string{"timestamp", "tree_size", "tree_head", "cert"}
		row, err := db.Single().ReadRow(ctx(), "certificates", spanner.Key{int64(treeSize)}, cols)
		fatalIfNotNil(t, err, "Error finding certificate")

		err = row.Columns(&timestamp, &treeSize2, &treeHead, &certData)
		fatalIfNotNil(t, err, "Error reading certificate data")

		if now-timestamp > 1 /* seconds */ {
			t.Fatalf("Incorrect timestamp [%d] != [%d]", timestamp, now)
		}

		if !bytes.Equal(treeHead, mh) {
			t.Fatalf("Incorrect cert tree head [%x] != [%x]", mh, treeHead)
		}

		if !bytes.Equal(certDER, certData) {
			t.Fatalf("Incorrect cert data [%x] != [%x]", certDER, certData)
		}

	}

	/*
		err = clearDB(db)
		if err != nil {
			t.Fatal("clearDB:", err)
		}
	*/
}
