package zeekclickhouse

import (
	"testing"
)

var LogExample = `{"_path":"dns","_system_name":"ja-ap200.home","_write_ts":"2021-03-05T14:04:11.711171Z","ts":"2021-03-05T14:04:11.710918Z","uid":"CFl2ra4eJhk9DpOFYk","id.orig_h":"192.168.2.11","id.orig_p":57844,"id.resp_h":"192.168.2.1","id.resp_p":53,"proto":"udp","trans_id":30780,"rtt":0.0002529621124267578,"query":"unifi.home","qclass":1,"qclass_name":"C_INTERNET","qtype":1,"qtype_name":"A","rcode":0,"rcode_name":"NOERROR","AA":true,"TC":false,"RD":true,"RA":true,"Z":0,"answers":["192.168.2.25"],"TTLs":[86400.0],"rejected":false}`

func TestConvert(t *testing.T) {
	rec, err := ZeekToDBRecord([]byte(LogExample))
	if err != nil {
		t.Fatalf("ZeekToDBRecord failed: %v", err)
	}
	t.Logf("Got: %+v", rec)

	if rec.Path != "dns" {
		t.Errorf("Expected rec.path=dns, got %v", rec.Path)
	}
	if rec.Hostname != "ja-ap200.home" {
		t.Errorf("Expected rec.Hostname=ja-ap200.home, got %v", rec.Hostname)
	}
	//FIXME: is this even what i want? it throws away the .000918
	if rec.Timestamp != 1614953051710 {
		t.Errorf("Expected rec.Hostname=1614953051710, got %v", rec.Timestamp)
	}
	//TODO: test the kv fields
}

func BenchmarkConvert(b *testing.B) {
	// run the Fib function b.N times
	var r string
	brec := []byte(LogExample)
	for n := 0; n < b.N; n++ {
		rec, err := ZeekToDBRecord(brec)
		r = rec.Path
		if err != nil {
			b.Fatalf("ZeekToDBRecord failed: %v", err)
		}
	}
	if r != "dns" {
		b.Errorf("Expected rec.path=dns, got %v", r)
	}
}
