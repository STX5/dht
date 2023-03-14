package remoteNode

import (
	"crypto/rand"
	"dht/logger"
	"dht/nettools"
	"dht/util"
	"io"
	"net"
	"strconv"
	"time"
)

// Owned by the DHT engine.
type RemoteNode struct {
	Address net.UDPAddr
	// addressDotFormatted contains a binary representation of the node's host:port address.
	AddressBinaryFormat string
	ID                  string
	// lastQueryID should be incremented after consumed. Based on the
	// protocol, it would be two letters, but I'm using 0-255, although
	// treated as string.
	LastQueryID int
	// TODO: key by util.InfoHash instead?
	PendingQueries   map[string]*QueryType // key: transaction ID
	PastQueries      map[string]*QueryType // key: transaction ID
	Reachable        bool
	LastResponseTime time.Time
	LastSearchTime   time.Time
	ActiveDownloads  []string // List of util.InfoHashes we know this peer is downloading.
	Log              *logger.DebugLogger
}

func NewRemoteNode(addr net.UDPAddr, id string, log *logger.DebugLogger) *RemoteNode {
	return &RemoteNode{
		Address:             addr,
		AddressBinaryFormat: nettools.DottedPortToBinary(addr.String()),
		LastQueryID:         NewTransactionId(),
		ID:                  id,
		Reachable:           false,
		PendingQueries:      map[string]*QueryType{},
		PastQueries:         map[string]*QueryType{},
		Log:                 log,
	}
}

// newQuery creates a new transaction id and adds an entry to r.PendingQueries.
// It does not set any extra information to the transaction information, so the
// caller must take care of that.
func (r *RemoteNode) NewQuery(transType string) (transId string) {
	(*r.Log).Debugf("newQuery for %x, lastID %v", r.ID, r.LastQueryID)
	r.LastQueryID = (r.LastQueryID + 1) % 256
	transId = strconv.Itoa(r.LastQueryID)
	(*r.Log).Debugf("... new id %v", r.LastQueryID)
	r.PendingQueries[transId] = &QueryType{Type: transType}
	return
}

// wasContactedRecently returns true if a node was contacted recently _and_
// one of the recent queries (not necessarily the last) was about the ih. If
// the ih is different at each time, it will keep returning false.
func (r *RemoteNode) WasContactedRecently(ih util.InfoHash) bool {
	if len(r.PendingQueries) == 0 && len(r.PastQueries) == 0 {
		return false
	}
	if !r.LastResponseTime.IsZero() && time.Since(r.LastResponseTime) > SearchRetryPeriod {
		return false
	}
	for _, q := range r.PendingQueries {
		if q.IH == ih {
			return true
		}
	}
	if !r.LastSearchTime.IsZero() && time.Since(r.LastSearchTime) > SearchRetryPeriod {
		return false
	}
	for _, q := range r.PastQueries {
		if q.IH == ih {
			return true
		}
	}
	return false
}

func RandNodeId() ([]byte, error) {
	b := make([]byte, 20)
	_, err := io.ReadFull(rand.Reader, b)
	return b, err
}
