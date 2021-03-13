package zeekclickhouse

import (
	"os"
	"testing"
)

func TestZeekTSVReader(t *testing.T) {
	r, err := os.Open("test_data/http.log")
	if err != nil {
		t.Fatalf("failed to open test data: %v", err)
	}

	z := NewZeekTSVReader(r)

	rec, err := z.Next()
	if err != nil {
		t.Fatalf("Next() failed: %v", err)
	}
	t.Logf("Got: %+v", rec)

	if rec.Path != "http" {
		t.Errorf("Expected rec.path=http, got %v", rec.Path)
	}
	//if rec.Hostname != "ja-ap200.home" {
	//	t.Errorf("Expected rec.Hostname=ja-ap200.home, got %v", rec.Hostname)
	//}
	//FIXME: is this even what i want? it throws away the .000932
	if rec.Timestamp != 1389719056899 {
		t.Errorf("Expected rec.Timestamp=1389719056899, got %v", rec.Timestamp)
	}
	//TODO: test the kv fields
}
