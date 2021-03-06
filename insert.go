package main

import (
	"bufio"
	"database/sql"
	_ "embed"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/buger/jsonparser"
)

//go:embed schema.sql
var schema string

func main() {
	connect, err := sql.Open("clickhouse", "tcp://192.168.2.68:9000?debug=false")
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

	var string_names []string
	var number_names []string
	var bool_names []string

	var string_values []string
	var number_values []float64
	var bool_values []bool

	br := bufio.NewReaderSize(os.Stdin, 16*1024*1024)
	for {
		string_names = string_names[:0]
		number_names = number_names[:0]
		bool_names = bool_names[:0]

		string_values = string_values[:0]
		number_values = number_values[:0]
		bool_values = bool_values[:0]

		line, err := br.ReadSlice('\n')
		if err != nil {
			log.Printf("ReadSlice: %v", err)
			break
		}
		path, err := jsonparser.GetString(line, "_path")
		if err != nil {
			log.Fatal(err) //FIXME
		}
		hostname, err := jsonparser.GetString(line, "_system_name")
		if err != nil {
			log.Fatal(err) //FIXME
		}
		ts, err := jsonparser.GetString(line, "ts")
		if err != nil {
			log.Fatal(err) //FIXME
		}
		t, err := time.Parse("2006-01-02T15:04:05.000000Z", ts)
		if err != nil {
			log.Fatal(err) //FIXME
		}
		ts_millis := t.UnixNano() / 1e6

		jsonparser.ObjectEach(line, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
			switch dataType {
			case jsonparser.String:
				string_names = append(string_names, string(key))
				string_values = append(string_values, string(value))
			case jsonparser.Number:
				number_names = append(number_names, string(key))
				val, err := strconv.ParseFloat(string(value), 64)
				if err != nil {
					log.Fatal(err) //FIXME
				}
				number_values = append(number_values, val)
			case jsonparser.Boolean:
				bool_names = append(bool_names, string(key))
				val, err := strconv.ParseBool(string(value))
				if err != nil {
					log.Fatal(err) //FIXME
				}
				bool_values = append(bool_values, val)
			default:
				log.Printf("Don't handle: Key: '%s' Value: '%s' Type: %s", string(key), string(value), dataType)
			}
			return nil
		})
		//log.Printf("Strings: %v %v", string_names, string_values)
		//log.Printf("numbers: %v %v", number_names, number_values)
		//log.Printf("bools: %v %v", bool_names, bool_values)
		if _, err := stmt.Exec(
			ts_millis,
			path,
			hostname,
			line,
			clickhouse.Array(string_names),
			clickhouse.Array(string_values),
			clickhouse.Array(number_names),
			clickhouse.Array(number_values),
			clickhouse.Array(bool_names),
			clickhouse.Array(bool_values),
		); err != nil {
			log.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
}
