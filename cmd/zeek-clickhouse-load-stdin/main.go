package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/ClickHouse/clickhouse-go"
	zeekclickhouse "github.com/JustinAzoff/zeek-clickhouse"
)

func main() {
	var uri string
	flag.StringVar(&uri, "uri", "tcp://192.168.2.68:9000?debug=false", "server uri")
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
	br := bufio.NewReaderSize(os.Stdin, 16*1024*1024)
	for {
		line, err := br.ReadSlice('\n')
		if err != nil {
			log.Printf("ReadSlice: %v", err)
			break
		}
		rec, err := zeekclickhouse.ZeekToDBRecord(line)
		if err != nil {
			log.Printf("Error Converting: %s: %v", line, err)
			continue
		}
		//log.Printf("Strings: %v %v", string_names, string_values)
		//log.Printf("numbers: %v %v", number_names, number_values)
		//log.Printf("bools: %v %v", bool_names, bool_values)
		//log.Printf("arrays: %v %v", array_names, array_values)
		err = inserter.Insert(rec)
		if err != nil {
			log.Printf("Error inserting: %v: %v", rec, err)
			continue
		}
	}

	if err := inserter.Commit(); err != nil {
		log.Fatal(err)
	}
}
