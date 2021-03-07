package main

import (
	"bufio"
	"database/sql"
	_ "embed"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/ClickHouse/clickhouse-go"
)

//go:embed schema.sql
var schema string

func main() {
	var uri string
	flag.StringVar(&uri, "uri", "tcp://192.168.2.68:9000?debug=false", "server uri")
	flag.Parse()

	connect, err := sql.Open("clickhouse", uri)
	if err != nil {
		log.Fatal(err)
	}
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return
	}

	_, err = connect.Exec(schema)
	if err != nil {
		log.Fatal(err)
	}
	tx, err := connect.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := tx.Prepare(`
			INSERT INTO logs (
				_timestamp, _path, _hostname, _source,
				"string.names", "string.values",
				"number.names", "number.values",
				"bool.names", "bool.values",
				"array.names", "array.values"
			) VALUES (
				?, ?, ?, ?,
				?, ?,
				?, ?,
				?, ?,
				?, ?
			)`)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	br := bufio.NewReaderSize(os.Stdin, 16*1024*1024)
	for {
		line, err := br.ReadSlice('\n')
		if err != nil {
			log.Printf("ReadSlice: %v", err)
			break
		}
		rec, err := ZeekToDBRecord(line)
		if err != nil {
			log.Printf("Error Converting: %s: %v", line, err)
			continue
		}
		//log.Printf("Strings: %v %v", string_names, string_values)
		//log.Printf("numbers: %v %v", number_names, number_values)
		//log.Printf("bools: %v %v", bool_names, bool_values)
		//log.Printf("arrays: %v %v", array_names, array_values)
		if _, err := stmt.Exec(
			rec.Timestamp,
			rec.Path,
			rec.Hostname,
			line,
			clickhouse.Array(rec.string_names),
			clickhouse.Array(rec.string_values),
			clickhouse.Array(rec.number_names),
			clickhouse.Array(rec.number_values),
			clickhouse.Array(rec.bool_names),
			clickhouse.Array(rec.bool_values),
			clickhouse.Array(rec.array_names),
			clickhouse.Array(rec.array_values),
		); err != nil {
			log.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
}
