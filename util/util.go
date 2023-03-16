package util

import (
	"encoding/hex"
	"fmt"
)

const (
	// Each query returns up to this number of nodes.
	KNodes = 8
	// ConsIDer a node stale if it has more than this number of oustanding
	// queries from us.
	MaxNodePendingQueries = 5
)

type InfoHash string

func (i InfoHash) String() string {
	return fmt.Sprintf("%x", string(i))
}

// DecodeInfoHash transforms a hex-encoded 20-characters string to a binary
// infohash.
func DecodeInfoHash(x string) (b InfoHash, err error) {
	var h []byte
	h, err = hex.DecodeString(x)
	if len(h) != 20 {
		return "", fmt.Errorf("DecodeInfoHash: expected InfoHash len=20, got %d", len(h))
	}
	return InfoHash(h), err
}

// DecodePeerAddress transforms the binary-encoded host:port address into a
// human-readable format. So, "abcdef" becomes 97.98.99.100:25958.
func DecodePeerAddress(x string) string {
	return BinaryToDottedPort(x)
}

// Calculates the distance between two hashes. In DHT/Kademlia, "distance" is
// the XOR of the torrent util.InfoHash and the peer node ID.  This is slower than
// necessary. Should only be used for displaying friendly messages.
func HashDistance(ID1 InfoHash, ID2 InfoHash) (distance string) {
	d := make([]byte, len(ID1))
	if len(ID1) != len(ID2) {
		return ""
	} else {
		for i := 0; i < len(ID1); i++ {
			d[i] = ID1[i] ^ ID2[i]
		}
		return string(d)
	}
}
