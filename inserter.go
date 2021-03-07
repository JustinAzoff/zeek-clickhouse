package zeekclickhouse

import (
	"database/sql"
	_ "embed"
	"log"

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
