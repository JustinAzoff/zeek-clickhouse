package main

import (
	"database/sql"
	_ "embed"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go"
)

//go:embed schema.sql
var schema string

func main() {
	connect, err := sql.Open("clickhouse", "tcp://192.168.2.68:9000?debug=true")
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
	var (
		tx, _   = connect.Begin()
		stmt, _ = tx.Prepare(`
			INSERT INTO logs (
				_timestamp, _path, _hostname, _source,
				"string.names", "string.values",
				"number.names", "number.values",
				"bool.names", "bool.values"
			) VALUES (
				?, ?, ?, ?,
				?, ?,
				?, ?,
				?, ?
			)`)
	)
	defer stmt.Close()

	for i := 0; i < 100; i++ {
		if _, err := stmt.Exec(
			"RU",
			10+i,
			100+i,
			clickhouse.Array([]int16{1, 2, 3}),
			time.Now(),
			time.Now(),
		); err != nil {
			log.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
}
