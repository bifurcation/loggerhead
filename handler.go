package loggerhead

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	frontierSelectQ = "SELECT * FROM frontier ORDER BY index;"
	frontierDeleteQ = "DELETE FROM frontier;"
	frontierInsertQ = "INSERT INTO frontier VALUES ($1, $2, $3);"

	certInsertQ = "INSERT INTO certificates VALUES ($1, $2, $3, $4);"
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
	DB *sql.DB
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
	enterHandler := float64(time.Now().UnixNano()) / 1000000000.0
	defer func() {
		exitHandler := float64(time.Now().UnixNano()) / 1000000000.0
		handlerTime.Observe(exitHandler - enterHandler)
	}()

	// Extract certificate from request
	// XXX: No verification of input certificate
	//  - No check that it parses as valid X.509
	//  - No verification of the certificate chain
	//  - No deduplication
	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		requestResult.With(prometheus.Labels{"outcome": "body-read-err"}).Inc()
		response.WriteHeader(http.StatusBadRequest)
		response.Write([]byte(fmt.Sprintf("Failed to read body: %v", err)))
		return
	}

	ctRequest := addChainRequest{}
	err = json.Unmarshal(body, &ctRequest)
	if err != nil {
		requestResult.With(prometheus.Labels{"outcome": "body-parse-err"}).Inc()
		response.WriteHeader(http.StatusBadRequest)
		response.Write([]byte(fmt.Sprintf("Failed to parse body: %v", err)))
		return
	}

	if len(ctRequest.Chain) == 0 {
		requestResult.With(prometheus.Labels{"outcome": "empty-chain"}).Inc()
		response.WriteHeader(http.StatusBadRequest)
		response.Write([]byte("No certificates provided in body"))
		return
	}

	cert, err := base64.StdEncoding.DecodeString(ctRequest.Chain[0])
	if err != nil {
		requestResult.With(prometheus.Labels{"outcome": "base64-decode-err"}).Inc()
		response.WriteHeader(http.StatusBadRequest)
		response.Write([]byte(fmt.Sprintf("Base64 decoding failed: %v", err)))
		return
	}

	// Update the DB
	tx, err := lh.DB.Begin()
	if err != nil {
		requestResult.With(prometheus.Labels{"outcome": "tx-begin-err"}).Inc()
		response.WriteHeader(http.StatusServiceUnavailable)
		response.Write([]byte(fmt.Sprintf("Could not get DB transaction: %v", err)))
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
		requestResult.With(prometheus.Labels{"outcome": "read-frontier-err"}).Inc()
		response.WriteHeader(http.StatusInternalServerError)
		response.Write([]byte(fmt.Sprintf("Failed to fetch frontier: %v", err)))
		return
	}

	// Update the frontier with this certificate
	enterUpdate := float64(time.Now().UnixNano()) / 1000000000.0
	f.Add(cert)
	treeSize := f.Size()
	treeHead := f.Head()
	exitUpdate := float64(time.Now().UnixNano()) / 1000000000.0
	updateTime.Observe(exitUpdate - enterUpdate)

	// Log the certificate
	timestamp := uint64(time.Now().Unix())
	err = logCertificate(tx, timestamp, treeSize, treeHead, cert)
	if err != nil {
		tx.Rollback()
		requestResult.With(prometheus.Labels{"outcome": "log-cert-err"}).Inc()
		response.WriteHeader(http.StatusInternalServerError)
		response.Write([]byte(fmt.Sprintf("Failed to log certificate: %v", err)))
		return
	}

	// Update the frontier
	err = writefrontier(tx, f)
	if err != nil {
		tx.Rollback()
		requestResult.With(prometheus.Labels{"outcome": "log-cert-err"}).Inc()
		response.WriteHeader(http.StatusInternalServerError)
		response.Write([]byte(fmt.Sprintf("Failed to log certificate: %v", err)))
		return
	}

	// Commit the changes
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		requestResult.With(prometheus.Labels{"outcome": "tx-commit-err"}).Inc()
		response.WriteHeader(http.StatusInternalServerError)
		response.Write([]byte(fmt.Sprintf("Failed to commit changes: %v", err)))
		return
	}

	// XXX: Should sign and return SCT
	requestResult.With(prometheus.Labels{"outcome": "success"}).Inc()
	response.WriteHeader(http.StatusOK)
}
