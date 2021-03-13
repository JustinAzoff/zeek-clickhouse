package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/ClickHouse/clickhouse-go"
	zeekclickhouse "github.com/JustinAzoff/zeek-clickhouse"
)

func main() {
	var uri string
	var batchSize int
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
	z := NewZeekTSVReader(os.Stdin)
	n := 0
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
}
