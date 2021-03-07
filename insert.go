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

type Inserter struct {
	URI  string
	conn *sql.DB
	tx   *sql.Tx
	stmt *sql.Stmt
}

func NewInserter(URI string) (*Inserter, error) {
	connect, err := sql.Open("clickhouse", URI)
	if err != nil {
		log.Fatal(err)
	}
	return &Inserter{
		URI:  URI,
		conn: connect,
	}, nil
}
func (i *Inserter) Ping() error {
	return i.conn.Ping()
}
func (i *Inserter) LoadSchema() error {
	_, err := i.conn.Exec(schema)
	return err
}
func (i *Inserter) Begin() error {
	tx, err := i.conn.Begin()
	if err != nil {
		return err
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
		return err
	}
	i.tx = tx
	i.stmt = stmt
	return nil
}
func (i *Inserter) Commit() error {
	if err := i.tx.Commit(); err != nil {
		return err
	}
	i.stmt.Close()
	return nil
}

func (i *Inserter) Insert(rec DBRecord) error {
	_, err := i.stmt.Exec(
		rec.Timestamp, rec.Path, rec.Hostname, rec.Source,
		clickhouse.Array(rec.string_names), clickhouse.Array(rec.string_values),
		clickhouse.Array(rec.number_names), clickhouse.Array(rec.number_values),
		clickhouse.Array(rec.bool_names), clickhouse.Array(rec.bool_values),
		clickhouse.Array(rec.array_names), clickhouse.Array(rec.array_values),
	)
	return err
}

func main() {
	var uri string
	flag.StringVar(&uri, "uri", "tcp://192.168.2.68:9000?debug=false", "server uri")
	flag.Parse()

	inserter, err := NewInserter(uri)
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
		rec, err := ZeekToDBRecord(line)
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
