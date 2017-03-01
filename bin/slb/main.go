package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"strings"
)

func main() {
	var host string
	var portList string
	flag.StringVar(&host, "host", "localhost", "Host to connect to")
	flag.StringVar(&portList, "ports", "8000", "Comma-separated list of ports")
	flag.Parse()

	ports := strings.Split(portList, ",")
	hosts := make([]string, len(ports))
	for i, port := range ports {
		hosts[i] = fmt.Sprintf("%s:%s", host, port)
	}

	curr := 0
	director := func(req *http.Request) {
		log.Printf("routing request to %s", hosts[curr])

		req.URL.Scheme = "http"
		req.URL.Host = hosts[curr]
		curr = (curr + 1) % len(hosts)
	}

	proxy := &httputil.ReverseProxy{Director: director}

	log.Print("Listening...")
	http.Handle("/", proxy)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
