package peer

import (
	"dht/util"
	"testing"
)

func TestPeerStorage(t *testing.T) {
	ih, err := util.DecodeInfoHash("d1c5676ae7ac98e8b19f63565905105e3c4c37a2")
	if err != nil {
		t.Fatalf("DecodeInfoHash: %v", err)
	}
	// Allow 1 IH and 2 peers.
	p := NewPeerStore(1, 2)

	if ok := p.AddContact(ih, "abcedf"); !ok {
		t.Fatalf("AddContact(1/2) expected true, got false")
	}
	if p.Count(ih) != 1 {
		t.Fatalf("Added 1st contact, got Count %v, wanted 1", p.Count(ih))
	}
	p.AddContact(ih, "ABCDEF")
	if p.Count(ih) != 2 {
		t.Fatalf("Added 2nd contact, got Count %v, wanted 2", p.Count(ih))
	}
	p.AddContact(ih, "ABCDEF")
	if p.Count(ih) != 2 {
		t.Fatalf("Repeated 2nd contact, got Count %v, wanted 2", p.Count(ih))
	}
	p.AddContact(ih, "XXXXXX")
	if p.Count(ih) != 2 {
		t.Fatalf("Added 3rd contact, got Count %v, wanted 2", p.Count(ih))
	}

	ih2, err := util.DecodeInfoHash("deca7a89a1dbdc4b213de1c0d5351e92582f31fb")
	if err != nil {
		t.Fatalf("DecodeInfoHash: %v", err)
	}
	if p.Count(ih2) != 0 {
		t.Fatalf("ih2 got Count %d, wanted 0", p.Count(ih2))
	}
	p.AddContact(ih2, "ABCDEF")
	if p.Count(ih) != 0 {
		t.Fatalf("ih got Count %d, wanted 0", p.Count(ih))
	}
	if p.Count(ih2) != 1 {
		t.Fatalf("ih2 got Count %d, wanted 1", p.Count(ih))
	}
}
