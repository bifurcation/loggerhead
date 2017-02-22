package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/bifurcation/loggerhead"
	_ "github.com/lib/pq"
)

/*

Test invocation:

> go run main.go -conn "user=rbarnes dbname=rbarnes sslmode=disable"

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
	var conn, port string
	flag.StringVar(&conn, "conn", "", "Connection string")
	flag.StringVar(&port, "port", "8080", "Port")
	flag.Parse()

	db, err := sql.Open("postgres", conn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening DB: %v", err)
	}

	handler := &loggerhead.LogHandler{db}
	http.Handle("/ct/v1/add-chain", handler)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
