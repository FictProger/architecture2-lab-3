package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/FictProger/architecture2-lab-2/httptools"
	"github.com/FictProger/architecture2-lab-2/signal"
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var (
	timeout     = time.Duration(*timeoutSec) * time.Second
	serversPool = []string{
		"localhost:8080",
		"localhost:8081",
		"localhost:8082",
	}
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func forward(reqCnt int, dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	fwdRequest.Header.Set("lb-author", "a")
	fwdRequest.Header.Set("lb-req-cnt", strconv.Itoa(reqCnt))

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func existAlive(poolAlive *[]bool) (int, error) {
	for i, v := range *poolAlive {
		if v == true {
			return i, nil
		}
	}
	return 0, fmt.Errorf("not exist alive server")
}

func chooseServer(poolTraffic []int, poolAlive *[]bool) (int, error) {
	index, err := existAlive(poolAlive)
	if err != nil {
		return index, err
	}

	value := poolTraffic[index]
	for i := index + 1; i < len(poolTraffic); i += 1 {
		if poolTraffic[i] < value && (*poolAlive)[i] {
			index = i
			value = poolTraffic[i]
		}
	}
	return index, nil
}

func main() {
	flag.Parse()

	serversPoolAlive := make([]bool, len(serversPool))
	for i, server := range serversPool {
		server := server
		serverIndex := i

		var mu sync.Mutex
		go func() {
			for range time.Tick(10 * time.Second) {
				log.Println(server, health(server)) //
				mu.Lock()                           //
				serversPoolAlive[serverIndex] = health(server)
				mu.Unlock() //
			}
		}()
	}

	reqCnt := 0
	serversPoolTraffic := make([]int, len(serversPool))
	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		serverIndex, err := chooseServer(serversPoolTraffic, &serversPoolAlive)
		if err != nil {
			log.Printf("Error: %v", err)
			return
		}

		forward(reqCnt, serversPool[serverIndex], rw, r)

		contentLength, _ := strconv.Atoi(rw.Header().Get("Content-Length"))
		serversPoolTraffic[serverIndex] += contentLength

		reqCnt += 1
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
