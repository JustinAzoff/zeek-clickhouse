package zeekclickhouse

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
)

func decodeHexEscaped(escaped string) ([]byte, error) {
	cleaned := strings.ReplaceAll(escaped, "\\x", "")
	decoded, err := hex.DecodeString(cleaned)
	return decoded, err
}

type ZeekTSVReader struct {
	r            *bufio.Reader
	path         string
	fields       []string
	types        []string
	separator    string
	setSeparator string
	emptyField   string
	unsetField   string
}

func (z *ZeekTSVReader) Next() (DBRecord, error) {
	var rec DBRecord
	line, err := z.r.ReadSlice('\n')
	if err != nil {
		return rec, err
	}
	line = line[0 : len(line)-1]
	sline := string(line)
	if bytes.HasPrefix(line, []byte{'#'}) {
		//Handle #separator separately
		if bytes.HasPrefix(line, []byte("#separator")) {
			sep := string(line[len("#separator "):])
			decoded_sep, err := decodeHexEscaped(sep)
			if err != nil {
				return rec, fmt.Errorf("Invalid separator: %v: %w", sep, err)
			}
			z.separator = string(decoded_sep)
			return z.Next()
		}
		parts := strings.Split(sline, string(z.separator))
		key := parts[0]
		switch key {
		case "#path":
			z.path = parts[1]
		case "#set_separator":
			z.setSeparator = parts[1]
		case "#empty_field":
			z.emptyField = parts[1]
		case "#unset_field":
			z.unsetField = parts[1]
		case "#fields":
			z.fields = parts[1:]
		case "#types":
			z.types = parts[1:]
		case "#open":
		//do nothing
		case "#close":
		//do nothing
		default:
			log.Printf("unhandled header line: %s", line)
		}
		return z.Next()
	}
	parts := strings.Split(sline, z.separator)
	var fname string
	var ftype string
	for i, val := range parts {
		if val == z.emptyField || val == z.unsetField {
			continue
		}
		fname = z.fields[i]
		ftype = z.types[i]
		if strings.HasPrefix(ftype, "vector") || strings.HasPrefix(ftype, "set") {
			vval := strings.Split(val, z.setSeparator)
			rec.array_names = append(rec.array_names, fname)
			rec.array_values = append(rec.array_values, vval)
			continue
		}
		if fname == "ts" {
			nval, err := strconv.ParseFloat(string(val), 64)
			if err != nil {
				return rec, err
			}
			rec.Timestamp = int64(nval * 1000)
		}
		switch ftype {
		case "uid", "string", "addr", "enum":
			rec.string_names = append(rec.string_names, fname)
			rec.string_values = append(rec.string_values, val)
		case "time", "port", "count", "interval", "double", "int":
			nval, err := strconv.ParseFloat(string(val), 64)
			if err != nil {
				return rec, err
			}
			rec.number_names = append(rec.number_names, fname)
			rec.number_values = append(rec.number_values, nval)
		case "bool":
			bval, err := strconv.ParseBool(string(val))
			if err != nil {
				return rec, err
			}
			rec.bool_names = append(rec.bool_names, fname)
			rec.bool_values = append(rec.bool_values, bval)
		default:
			log.Printf("Unhandled %v %v", ftype, fname)
		}
	}
	rec.Path = z.path
	return rec, nil
}

func NewZeekTSVReader(r io.Reader) *ZeekTSVReader {
	br := bufio.NewReaderSize(r, 16*1024*1024)
	return &ZeekTSVReader{
		r:            br,
		separator:    "\t",
		setSeparator: ",",
	}
}
