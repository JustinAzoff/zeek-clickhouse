package main

import (
	"bytes"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	zeekclickhouse "github.com/JustinAzoff/zeek-clickhouse"
)

type HECClickhouse struct {
	ch   *zeekclickhouse.Inserter
	lock sync.Mutex
}

func NewHECClickhouse(URI string) (*HECClickhouse, error) {
	inserter, err := zeekclickhouse.NewInserter(URI)
	if err != nil {
		return nil, err
	}
	return &HECClickhouse{
		ch: inserter,
	}, nil
}

func (h *HECClickhouse) Ingest(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Printf("Error reading body: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"text":"Failure","code":503}` + "\n"))
		return
	}
	separated := bytes.Replace(body, []byte("}{"), []byte("}\n{"), -1)
	splitIntoLines := bytes.Split(separated, []byte("\n"))
	count := len(splitIntoLines)
	log.Printf("Got %d lines", count)
	records := make([]zeekclickhouse.DBRecord, 0, count)
	for _, line := range splitIntoLines {
		rec, err := zeekclickhouse.ZeekToDBRecord(line)
		if err != nil {
			log.Printf("Error converting record: %v", err)
		} else {
			records = append(records, rec)
		}
	}
	h.lock.Lock()
	defer h.lock.Unlock()

	err = h.ch.Begin()
	if err != nil {
		log.Printf("Error starting tx: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"text":"Failure","code":503}` + "\n"))
		return
	}

	for _, rec := range records {
		err = h.ch.Insert(rec)
		if err != nil {
			log.Printf("Error inserting: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"text":"Failure","code":503}` + "\n"))
			return
		}
	}
	err = h.ch.Commit()
	if err != nil {
		log.Printf("Error committing tx: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"text":"Failure","code":503}` + "\n"))
		return
	}
	h.lock.Unlock()

	w.Write([]byte(`{"text":"Success","code":0}` + "\n"))
}

func main() {
	var uri string
	var bindAddr string
	flag.StringVar(&uri, "uri", "tcp://192.168.2.68:9000?debug=false", "server uri")
	flag.StringVar(&bindAddr, "bind", ":8090", "bind addr")
	flag.Parse()

	myHandler := http.NewServeMux()
	srv := &http.Server{
		Addr:           bindAddr,
		Handler:        myHandler,
		ReadTimeout:    60 * time.Second,
		WriteTimeout:   120 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	hec, err := NewHECClickhouse(uri)
	if err != nil {
		log.Fatal(err)
	}
	myHandler.HandleFunc("/ingest", hec.Ingest)
	log.Printf("Listening on %s\n", bindAddr)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		// Error starting or closing listener:
		log.Fatalf("HTTP server ListenAndServe: %v", err)
	}

}
