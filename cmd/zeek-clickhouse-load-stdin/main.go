package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go"
	zeekclickhouse "github.com/JustinAzoff/zeek-clickhouse"
)

func main() {
	var uri string
	var batchSize int
	var format string
	flag.StringVar(&format, "format", "json", "json or csv")
	flag.StringVar(&uri, "uri", "tcp://192.168.2.68:9000?debug=false", "server uri")
	flag.IntVar(&batchSize, "batchsize", 100_000, "commit batch size")
	flag.Parse()

	inserter, err := zeekclickhouse.NewInserter(uri)
	if err != nil {
		log.Fatal(err)
	}
	if err := inserter.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return
	}
	err = inserter.LoadSchema()
	if err != nil {
		log.Fatal(err)
	}
	err = inserter.Begin()
	if err != nil {
		log.Fatal(err)
	}
	var z zeekclickhouse.DBConverter
	switch format {
	case "tsv":
		z = zeekclickhouse.NewZeekTSVReader(os.Stdin)
	case "json":
		z = zeekclickhouse.NewZeekJSONReader(os.Stdin)
	default:
		log.Fatalf("Invalid format: %v. Not tsv or json", format)
	}
	n := 0
	totalRecords := 0
	startTime := time.Now()
	for {
		rec, err := z.Next()
		if err != nil {
			log.Printf("Next: %v", err)
			break
		}
		err = inserter.Insert(rec)
		if err != nil {
			log.Printf("Error inserting: %v: %v", rec, err)
			continue
		}
		n++
		totalRecords++
		if n%batchSize == 0 {
			log.Printf("Committing %d records", n)
			n = 0
			if err := inserter.Commit(); err != nil {
				log.Fatal(err)
			}
			err = inserter.Begin()
			if err != nil {
				log.Fatal(err)
			}
		}

	}

	if n > 0 {
		log.Printf("Committing %d records", n)
		if err := inserter.Commit(); err != nil {
			log.Fatal(err)
		}
	}

	if totalRecords > 0 {
		duration := time.Since(startTime)
		log.Printf("Inserted %d records in %.1f seconds, %d records/sec", totalRecords, duration.Seconds(), float64(totalRecords)/duration.Seconds())
	}
}
