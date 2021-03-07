package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	zeekclickhouse "github.com/JustinAzoff/zeek-clickhouse"
)

var beginning = []byte(`"event":`)

func extractEvent(message []byte) ([]byte, error) {
	//Should probably just use jsonparser here, but I know the structure
	start := bytes.Index(message, []byte(beginning))
	if start == -1 {
		return message, errors.New("Can't find start of event")
	}
	return message[start+len(beginning) : len(message)-1], nil
}

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
func (h *HECClickhouse) doInsert(records []zeekclickhouse.DBRecord) error {
	var err error
	h.lock.Lock()
	defer h.lock.Unlock()

	err = h.ch.Begin()
	if err != nil {
		return fmt.Errorf("Error starting tx: %w", err)
	}

	for _, rec := range records {
		err = h.ch.Insert(rec)
		if err != nil {
			return fmt.Errorf("Error inserting : %w", err)
		}
	}
	err = h.ch.Commit()
	if err != nil {
		return fmt.Errorf("Error commiting : %w", err)
	}
	return nil
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
		zeek, err := extractEvent(line)
		if err != nil {
			log.Printf("Error extracting event: %v", err)
			continue
		}
		rec, err := zeekclickhouse.ZeekToDBRecord(zeek)
		if err != nil {
			log.Printf("Error converting record: %v", err)
			continue
		}
		records = append(records, rec)
	}
	err = h.doInsert(records)

	if err != nil {
		log.Printf("Error: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"text":"Failure","code":503}` + "\n"))
		return
	}
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
	err = inserter.LoadSchema()
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
