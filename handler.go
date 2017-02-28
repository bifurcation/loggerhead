package loggerhead

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"
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
	Client *spanner.Client
}

type addChainRequest struct {
	Chain []string `json:"chain"`
}

var (
	outcomeDefault           = "default"
	outcomeBodyReadErr       = "body-read-err"
	outcomeJSONParseErr      = "json-parse-err"
	outcomeEmptyChainErr     = "empty-chain"
	outcomeBase64DecodeErr   = "base64-decode-err"
	outcomeTxBeginErr        = "tx-begin-err"
	outcomeReadFrontierErr   = "read-frontier-err"
	outcomeLogCertErr        = "log-cert-err"
	outcomeDeleteFrontierErr = "delete-frontier-err"
	outcomeWriteFrontierErr  = "write-frontier-err"
	outcomeTxCommitErr       = "tx-commit-err"
	outcomeSuccess           = "success"

	responseValues = map[string]struct {
		Status  int
		Message string
	}{
		outcomeDefault:           {http.StatusInternalServerError, ""},
		outcomeBodyReadErr:       {http.StatusBadRequest, "Failed to read body: %v"},
		outcomeJSONParseErr:      {http.StatusBadRequest, "Failed to parse body: %v"},
		outcomeEmptyChainErr:     {http.StatusBadRequest, "No certificates provided in body: %v"},
		outcomeBase64DecodeErr:   {http.StatusBadRequest, "Base64 decoding failed: %v"},
		outcomeReadFrontierErr:   {http.StatusInternalServerError, "Failed to fetch frontier: %v"},
		outcomeLogCertErr:        {http.StatusInternalServerError, "Failed to log certificate: %v"},
		outcomeDeleteFrontierErr: {http.StatusInternalServerError, "Failed to delete frontier: %v"},
		outcomeWriteFrontierErr:  {http.StatusInternalServerError, "Failed to write frontier: %v"},
		outcomeTxCommitErr:       {http.StatusInternalServerError, "Failed to commit changes: %v"},
		outcomeSuccess:           {http.StatusOK, "success: %v"},
	}
)

func readFrontier(txn *spanner.ReadWriteTransaction) (*frontier, spanner.KeyRange, error) {

	f := frontier{}

	minIndex := int64(-1)
	maxIndex := int64(-1)

	keySet := spanner.KeySet{All: true}
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	iter := txn.Read(ctx, "frontier", keySet, []string{"index", "subtree_size", "subhead"})
	err := iter.Do(func(row *spanner.Row) error {
		var index int64
		var subtreeSize int64
		var subhead []byte
		err := row.Columns(&index, &subtreeSize, &subhead)
		if err != nil {
			return err
		}

		if minIndex == -1 || (index < minIndex) {
			minIndex = index
		}
		if maxIndex == -1 || (index > maxIndex) {
			maxIndex = index
		}

		f.entries = append(f.entries, frontierEntry{uint64(subtreeSize), subhead})
		return nil
	})

	kr := spanner.KeyRange{
		Start: spanner.Key{minIndex},
		End:   spanner.Key{maxIndex},
		Kind:  spanner.ClosedClosed,
	}

	if err != nil {
		return nil, kr, err
	}

	f.Sort()
	return &f, kr, nil
}

func (lh *LogHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	outcome := outcomeDefault
	err := error(nil)
	enterHandler := float64(time.Now().UnixNano()) / 1000000000.0
	defer func() {
		exitHandler := float64(time.Now().UnixNano()) / 1000000000.0

		elapsed := exitHandler - enterHandler
		status := responseValues[outcome].Status
		message := fmt.Sprintf(responseValues[outcome].Message, err)

		handlerTime.Observe(elapsed)
		requestResult.With(prometheus.Labels{"outcome": outcome}).Inc()

		response.WriteHeader(status)
		response.Write([]byte(message))
		log.Printf("[%03d] [%8.6f] %s", status, elapsed, message)
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

	// Update the DB
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	_, err = lh.Client.ReadWriteTransaction(ctx, func(txn *spanner.ReadWriteTransaction) error {
		enterTx := float64(time.Now().UnixNano()) / 1000000000.0
		defer func() {
			exitTx := float64(time.Now().UnixNano()) / 1000000000.0
			transactionTime.Observe(exitTx - enterTx)
		}()

		// Read the frontier
		f, keyRange, err := readFrontier(txn)
		if err != nil {
			outcome = outcomeReadFrontierErr
			return err
		}

		// Add the certificate to the frontier
		f.Add(cert)

		// Write the certificate
		timestamp := time.Now().Unix()
		certCols := []string{"timestamp", "tree_size", "tree_head", "cert"}
		certVals := []interface{}{timestamp, int64(f.Size()), f.Head(), cert}
		mutations := []*spanner.Mutation{spanner.Insert("certificates", certCols, certVals)}
		err = txn.BufferWrite(mutations)
		if err != nil {
			outcome = outcomeLogCertErr
			return err
		}

		// Delete the old frontier
		mutations = []*spanner.Mutation{spanner.DeleteKeyRange("frontier", keyRange)}
		err = txn.BufferWrite(mutations)
		if err != nil {
			outcome = outcomeDeleteFrontierErr
			return err
		}

		// Write the new frontier
		frontierCols := []string{"index", "subtree_size", "subhead"}
		mutations = make([]*spanner.Mutation, f.Len())
		for i, entry := range f.entries {
			vals := []interface{}{i, int64(entry.SubtreeSize), entry.Value}
			mutations[i] = spanner.InsertOrUpdate("frontier", frontierCols, vals)
		}
		err = txn.BufferWrite(mutations)
		if err != nil {
			outcome = outcomeWriteFrontierErr
			return err
		}

		return nil
	})

	if err != nil && outcome == outcomeDefault {
		outcome = outcomeTxCommitErr
		return
	}

	outcome = outcomeSuccess
}
