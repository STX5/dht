package routingTable

import (
	"crypto/rand"
	"dht/remoteNode"
	"dht/util"
	"fmt"
	"sort"
	"strings"
	"testing"
)

// 16 bytes.
const ffff = "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"

func BenchmarkInsertRecursive(b *testing.B) {
	b.StopTimer()

	// Add 1k nodes to the tree.
	const count = 1000
	nodes := make([]*remoteNode.RemoteNode, 0, count)

	for i := 0; i < count; i++ {
		rId := make([]byte, 4)
		if _, err := rand.Read(rId); err != nil {
			b.Fatal("Couldnt produce random numbers for FindClosest:", err)
		}
		id := string(rId) + ffff
		if len(id) != 20 {
			b.Fatalf("Random InfoHash construction error, wrong len: want %d, got %d",
				20, len(id))
		}
		nodes = append(nodes, &remoteNode.RemoteNode{ID: id})
	}
	b.StartTimer()
	// Each op is adding 1000 nodes to the tree.
	for i := 0; i < b.N; i++ {
		tree := &nTree{}
		for _, r := range nodes {
			tree.Insert(r)
		}
	}
}

type testData struct {
	query string
	want  int // just the size.
}

var nodes = []*remoteNode.RemoteNode{
	{ID: "\x00"},
	{ID: "\x01"},
	{ID: "\x02"},
	{ID: "\x03"},
	{ID: "\x04"},
	{ID: "\x05"},
	{ID: "\x06"},
	{ID: "\x07"},
	{ID: "\x08"},
	{ID: "\x09"},
	{ID: "\x10"},
}

func TestNodeDelete(t *testing.T) {
	tree := &nTree{}

	for _, r := range nodes[:4] {
		tree.Insert(r)
	}
	for i, r := range []string{"\x00", "\x01"} {
		id := util.InfoHash(r)
		t.Logf("Removing node: %x", r)
		tree.Cut(id, 0)
		neighbors := tree.Lookup(id)
		if len(neighbors) == 0 {
			t.Errorf("Deleted too many nodes.")
		}
		if len(neighbors) != 3-i {
			t.Errorf("Too many nodes left in the tree: got %d, wanted %d", len(neighbors), 3-i)
		}
		if r == neighbors[0].ID {
			t.Errorf("Node didnt get deleted as expected: %x", r)
		}
	}

}

func TestNodeDistance(t *testing.T) {
	tree := &nTree{}
	for _, r := range nodes {
		r.Reachable = true
		tree.Insert(r)
	}
	tests := []testData{
		{"\x04", 8},
		{"\x07", 8},
	}
	for _, r := range tests {
		q := util.InfoHash(r.query)
		distances := make([]string, 0, len(tests))
		neighbors := tree.Lookup(q)
		if len(neighbors) != r.want {
			t.Errorf("id: %x, wanted len=%d, got len=%d", q, r.want, len(neighbors))
			t.Errorf("Details: %#v", neighbors)
		}
		for _, x := range neighbors {
			d := util.HashDistance(q, util.InfoHash(x.ID))
			var b []string
			for _, c := range d {
				if c != 0 {
					b = append(b, fmt.Sprintf("%08b", c))
				} else {
					b = append(b, "00000000")
				}
			}
			d = strings.Join(b, ".")
			distances = append(distances, d)
		}
		if !sort.StringsAreSorted(distances) {
			t.Errorf("Resulting distances for %x are not sorted", r.query)
			for i, d := range distances {
				t.Errorf("id: %x, d: %v", neighbors[i].ID, d)
			}
		}
	}

}

// ===================== lookup benchmark =================================

// $ go test -v -bench='BenchmarkFindClosest' -run=NONE
//
// #1 In hindsight, this was a very embarrasing first attempt. I kept a list of
// my nodes, and every time I had to do a lookup, I re-sorted the whole list of
// nodes in the routing table using the XOR distance to the target remoteNode.InfoHash.
// Honestly I had no idea how bad this was when I was wrote it. :-)
// BenchmarkFindClosest	       1	7020661000 ns/op
//
// #2 not-checked in attempt to use a trie. Not even correct.
// BenchmarkFindClosest	       1	1072682000 ns/op
//
// #3 only compare bytes that we need.
// BenchmarkFindClosest	       1	1116333000 ns/op
//
// #4 moved to buckets, but using only one.
// BenchmarkFindClosest	       1	1170809000 ns/op
//
// #5 using my new nTree (not yet correct)
// BenchmarkFindClosest	  100000	     27194 ns/op
//
// #6 recursive nTree (correct)
// BenchmarkFindClosest	  200000	     10585 ns/op
//
// #7 removed an unnecessary wrapper function.
// BenchmarkFindClosest	  200000	      9691 ns/op
//
// #8 Random remoteNode.InfoHashes now have identical suffix instead of prefix. In the
// wild, most of the calculations are done in the most significant bits so this
// is closer to reality.
// BenchmarkFindClosest	   50000	     35165 ns/op
//
// #9 Suffix compression. Magic? :-)
// BenchmarkFindClosest	 1000000	      2795 ns/op

// ===================== insertion benchmark =================================
// $ go test -v -bench='BenchmarkInsert.*' -run=none
//
// #1 initial version of the test.
// BenchmarkInsertRecursive	     500	   4701600 ns/op
// BenchmarkInsert	     500	   3595448 ns/op
//
// #2 Random remoteNode.InfoHashes have identical suffix instead of prefix.
// BenchmarkInsertRecursive	     100	  22598150 ns/op
// BenchmarkInsert	     100	  19239120 ns/op
//
// #3 Suffix compression. Much less work (iterative version removed).
// BenchmarkInsertRecursive	    5000	    448471 ns/op
