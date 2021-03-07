package main

type DBRecord struct {
	Timestamp int64
	Path      string
	Hostname  string
	Source    string

	string_names  []string
	string_values []string

	number_names  []string
	number_values []float64

	bool_names  []string
	bool_values []bool

	array_names  []string
	array_values [][]string
}
