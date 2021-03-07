package zeekclickhouse

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/buger/jsonparser"
)

func ZeekToDBRecord(line []byte) (DBRecord, error) {
	var rec DBRecord
	path, err := jsonparser.GetString(line, "_path")
	if err != nil {
		return rec, fmt.Errorf("Invalid record %q, missing _path", line)
	}
	rec.Source = string(line)
	rec.Path = path

	hostname, err := jsonparser.GetString(line, "_system_name")
	if err == nil {
		rec.Hostname = hostname
	}
	ts, err := jsonparser.GetString(line, "ts")
	if err != nil {
		return rec, fmt.Errorf("Invalid record %q, missing ts", line)
	}
	t, err := time.Parse("2006-01-02T15:04:05.000000Z", ts)
	if err != nil {
		log.Fatal(err) //FIXME
	}
	rec.Timestamp = t.UnixNano() / 1e6
	err = jsonparser.ObjectEach(line, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		skey := string(key)
		//Already pulled out
		if skey == "_path" || skey == "_system_name" {
			return nil
		}
		switch dataType {
		case jsonparser.String:
			rec.string_names = append(rec.string_names, skey)
			rec.string_values = append(rec.string_values, string(value))
		case jsonparser.Number:
			rec.number_names = append(rec.number_names, skey)
			val, err := strconv.ParseFloat(string(value), 64)
			if err != nil {
				return err
			}
			rec.number_values = append(rec.number_values, val)
		case jsonparser.Boolean:
			rec.bool_names = append(rec.bool_names, skey)
			val, err := strconv.ParseBool(string(value))
			if err != nil {
				return err
			}
			rec.bool_values = append(rec.bool_values, val)
		case jsonparser.Array:
			rec.array_names = append(rec.array_names, skey)
			var items []string
			jsonparser.ArrayEach(value, func(nestedvalue []byte, dataType jsonparser.ValueType, offset int, err error) {
				items = append(items, string(nestedvalue))
			})
			rec.array_values = append(rec.array_values, items)
		default:
			log.Printf("Don't handle: Key: '%s' Value: '%s' Type: %s", skey, string(value), dataType)
		}
		return nil
	})
	return rec, err
}
