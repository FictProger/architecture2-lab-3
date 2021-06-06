package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"io/ioutil"

	"github.com/FictProger/architecture2-lab-3/datastore"
	"github.com/FictProger/architecture2-lab-3/httptools"
	"github.com/FictProger/architecture2-lab-3/signal"
)

type dbRow struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

var (
	port = flag.Int("port", 8090, "server port")
	dir  = flag.String("dir", ".", "database store directory")
)

const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"
const dbAddr = "http://db:8070"

func main() {
	flag.Parse()

	db, err := datastore.NewDb(*dir, "current-data", false)
	if err != nil {
		fmt.Printf("db run error: %v\n", err)
		return
	}

	h := new(http.ServeMux)

	h.HandleFunc("/db/", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")

		if r.Method == "GET" {
			key := r.URL.Query().Get("key")
			if len(key) == 0 {
				rw.WriteHeader(http.StatusNotFound)
				return
			}

			value, err := db.Get(key)
			if errors.Is(err, datastore.ErrNotFound) || len(value) == 0 {
				rw.WriteHeader(http.StatusNotFound)
				return
			} else if err != nil {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}

			valueJson, err := json.Marshal(dbRow{key, value})
			if err != nil {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}

			if _, err = rw.Write(valueJson); err != nil {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusOK)
		} else if r.Method == "POST" {
			key := r.URL.Query().Get("key")
			if len(key) == 0 {
				rw.WriteHeader(http.StatusNotFound)
				return
			}

			body, err := ioutil.ReadAll(r.Body)
			defer r.Body.Close()
			if err != nil {
				rw.WriteHeader(http.StatusBadRequest)
				return
			}

			var row dbRow
			if err = json.Unmarshal(body, &row); err != nil {
				rw.WriteHeader(http.StatusBadRequest)
				return
			}

			if err = db.Put(key, row.Value); err != nil {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}

			rw.WriteHeader(http.StatusCreated)
		}
	})

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
