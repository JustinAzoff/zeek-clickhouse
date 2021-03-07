package main

import "testing"

var rec = []byte(`{"time":1615153266.918483,"sourcetype":"corelight_weird","event":{"_path":"weird","_system_name":"ja-ap200.home","_write_ts":"2021-03-07T21:41:06.918483Z","ts":"2021-03-07T21:41:06.918483Z","name":"non_ip_packet_in_ethernet","notice":false}}`)

func TestExtractEvent(t *testing.T) {
	out, err := extractEvent(rec)
	if err != nil {
		t.Errorf("Error extracting event: %v", err)
	}
	t.Logf("Got %s", out)
}
