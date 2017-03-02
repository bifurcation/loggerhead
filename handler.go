package loggerhead

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	frontierSelectQ = "SELECT frontier FROM certificates ORDER BY tree_size DESC LIMIT 1;"
	certInsertQ     = "INSERT INTO certificates VALUES ($1, $2, $3, $4);"
)

// Prometheus metrics
var (
	// Outcomes of logging requests
	requestResult = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "add_chain_outcome_total",
			Help: "Number of requests with each outcome.",
		},
		[]string{"outcome"},
	)

	// Overall handler execution time
	handlerTime = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "log_handler_time_seconds",
		Help:    "The overall time for the log HTTP handler to return.",
		Buckets: prometheus.LinearBuckets(0, 0.05, 100),
	})

	// DB interaction time
	transactionTime = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "transaction_time_seconds",
		Help:    "The time the DB transaction was active.",
		Buckets: prometheus.LinearBuckets(0, 0.05, 100),
	})

	// Update time (exclusive of DB interaction)
	updateTime = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "update_time_seconds",
		Help:    "The time this node spent processing DB results before returning.",
		Buckets: prometheus.LinearBuckets(0, 0.005, 100),
	})
)

func init() {
	prometheus.MustRegister(requestResult)
	prometheus.MustRegister(handlerTime)
	prometheus.MustRegister(transactionTime)
	prometheus.MustRegister(updateTime)
}

type LogHandler struct {
	DB     *sql.DB
	dbLock sync.Mutex
}

type addChainRequest struct {
	Chain []string `json:"chain"`
}

func readfrontier(tx *sql.Tx) (*frontier, error) {
	f := frontier{}

	var buf []byte
	err := tx.QueryRow(frontierSelectQ).Scan(&buf)
	switch err {
	case sql.ErrNoRows:
		return &f, nil
	case nil:
		err = f.Unmarshal(buf)
		return &f, err
	default:
		return nil, err
	}
}

func logCertificate(tx *sql.Tx, timestamp, treeSize int64, frontier, cert []byte) error {
	_, err := tx.Exec(certInsertQ, timestamp, treeSize, frontier, cert)
	return err
}

var (
	outcomeBodyReadErr      = "body-read-err"
	outcomeJSONParseErr     = "json-parse-err"
	outcomeEmptyChainErr    = "empty-chain"
	outcomeBase64DecodeErr  = "base64-decode-err"
	outcomeDBLockTimeout    = "db-lock-timeout"
	outcomeTxBeginErr       = "tx-begin-err"
	outcomeReadFrontierErr  = "read-frontier-err"
	outcomeLogCertErr       = "log-cert-err"
	outcomeWriteFrontierErr = "write-frontier-err"
	outcomeTxCommitErr      = "tx-commit-err"
	outcomeSuccess          = "success"

	responseValues = map[string]struct {
		Status  int
		Message string
	}{
		outcomeBodyReadErr:      {http.StatusBadRequest, "Failed to read body: %v"},
		outcomeJSONParseErr:     {http.StatusBadRequest, "Failed to parse body: %v"},
		outcomeEmptyChainErr:    {http.StatusBadRequest, "No certificates provided in body: %v"},
		outcomeBase64DecodeErr:  {http.StatusBadRequest, "Base64 decoding failed: %v"},
		outcomeDBLockTimeout:    {http.StatusInternalServerError, "Timed out waiting for DB: %v"},
		outcomeTxBeginErr:       {http.StatusInternalServerError, "Could not get DB transaction: %v"},
		outcomeReadFrontierErr:  {http.StatusInternalServerError, "Failed to fetch frontier: %v"},
		outcomeLogCertErr:       {http.StatusInternalServerError, "Failed to log certificate: %v"},
		outcomeWriteFrontierErr: {http.StatusInternalServerError, "Failed to write frontier: %v"},
		outcomeTxCommitErr:      {http.StatusInternalServerError, "Failed to commit changes: %v"},
		outcomeSuccess:          {http.StatusOK, "success: %v"},
	}
)

func (lh *LogHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	outcome := outcomeSuccess
	err := error(nil)
	treeSize := int64(0)
	enterHandler := float64(time.Now().UnixNano()) / 1000000000.0
	defer func() {
		exitHandler := float64(time.Now().UnixNano()) / 1000000000.0

		elapsed := exitHandler - enterHandler
		status := responseValues[outcome].Status
		message := fmt.Sprintf(responseValues[outcome].Message, err)

		handlerTime.Observe(elapsed)
		requestResult.With(prometheus.Labels{"outcome": outcome}).Inc()

		response.WriteHeader(status)
		response.Write([]byte(message + "\n"))
		log.Printf("[%03d] [%d] [%8.6f] %s", status, treeSize, elapsed, message)
	}()

	// Extract certificate from request
	// XXX: No verification of input certificate
	//  - No check that it parses as valid X.509
	//  - No verification of the certificate chain
	//  - No deduplication
	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		outcome = outcomeBodyReadErr
		return
	}

	ctRequest := addChainRequest{}
	err = json.Unmarshal(body, &ctRequest)
	if err != nil {
		outcome = outcomeJSONParseErr
		return
	}

	if len(ctRequest.Chain) == 0 {
		outcome = outcomeEmptyChainErr
		return
	}

	cert, err := base64.StdEncoding.DecodeString(ctRequest.Chain[0])
	if err != nil {
		outcome = outcomeBase64DecodeErr
		return
	}

	// Await access to the DB
	gotLock := make(chan bool, 1)
	cancelled := make(chan bool, 1)
	go func() {
		lh.dbLock.Lock()
		gotLock <- true
		if <-cancelled {
			lh.dbLock.Unlock()
		}
	}()

	select {
	case <-gotLock:
	case <-time.After(500 * time.Millisecond):
		cancelled <- true
		outcome = outcomeDBLockTimeout
		return
	}
	defer lh.dbLock.Unlock()

	tx, err := lh.DB.Begin()
	if err != nil {
		outcome = outcomeTxBeginErr
		return
	}

	enterTx := float64(time.Now().UnixNano()) / 1000000000.0
	defer func() {
		exitTx := float64(time.Now().UnixNano()) / 1000000000.0
		transactionTime.Observe(exitTx - enterTx)
	}()

	// Get the frontier from the DB
	f, err := readfrontier(tx)
	if err != nil {
		tx.Rollback()
		outcome = outcomeReadFrontierErr
		return
	}

	// Update the frontier with this certificate
	enterUpdate := float64(time.Now().UnixNano()) / 1000000000.0
	f.Add(cert)
	treeSize = int64(f.Size())
	exitUpdate := float64(time.Now().UnixNano()) / 1000000000.0
	updateTime.Observe(exitUpdate - enterUpdate)

	// Log the certificate
	timestamp := time.Now().Unix()
	err = logCertificate(tx, timestamp, treeSize, f.Marshal(), cert)
	if err != nil {
		tx.Rollback()
		outcome = outcomeLogCertErr
		return
	}

	// Commit the changes
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		outcome = outcomeTxCommitErr
		return
	}

	outcome = outcomeSuccess
}
