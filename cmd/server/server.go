package main

import (
	// "encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	// "strconv"
	"time"
	"io"

	"github.com/FictProger/architecture2-lab-2/httptools"
	"github.com/FictProger/architecture2-lab-2/signal"
)

var port = flag.Int("port", 8080, "server port")

const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"
const dbAddr = "http://db:8070"

func main() {
	defClient := http.DefaultClient

	putJsonStr := `{"value";"`+time.Now().Format("2021-04-25")+`"}`
	r, err := http.NewRequest(
		"POST",
		"http://db:8091/db/?key=FictProger",
		bytes.NewBuffer([]byte(putJsonStr)),
	)
	if err != nil || r.StatusCode != http.StatusCreated {
		log.Fatal(err)
	}

	h := new(http.ServeMux)

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		key := r.FormValue("key")
			if key == "" {
				rw.WriteHeader(http.StatusNotFound)
				return
			}

			resp, err := http.Get(fmt.Sprintf("%s/db/%s", dbAddr, key))
			if err != nil {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}

			for k, values := range resp.Header {
				for _, value := range values {
					rw.Header().Add(k, value)
				}
			}

			rw.WriteHeader(resp.StatusCode)

			if _, err = io.Copy(rw, resp.Body); err != nil {
				return
			}
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
