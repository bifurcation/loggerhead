package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/bifurcation/loggerhead"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/net/context"
)

/*

Test invocation:

> export GOOGLE_APPLICATION_CREDENTIALS=<path-to-credentials>
> go run main.go -conn "projects/loggerhead-159916/instances/loggerhead/databases/loggerhead"

Test query:

-----
POST /ct/v1/add-chain HTTP/1.1
Host: localhost:8080
Content-Type: text/json
Content-Length: 58

{"chain":["8TpFNrV+YbVkOX6VRjDoxKGb32DNgBo0nPNgOvivsnA="]}
-----

*/

func main() {
	var dbName, port string
	flag.StringVar(&dbName, "db", "", "Spanner DB name")
	flag.StringVar(&port, "port", "8080", "Port")
	flag.Parse()

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	db, err := spanner.NewClient(ctx, dbName)
	if err != nil {
		log.Fatalf("Error opening DB connection: %v", err)
	}

	handler := &loggerhead.LogHandler{db}

	http.Handle("/metrics", promhttp.Handler())
	http.Handle("/ct/v1/add-chain", handler)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
