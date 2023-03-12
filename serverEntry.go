package dht

import "net"

// DHT node registration
type Registration struct {
	// infohash the dht node has
	InfoHash []string
	// DHT Node UPDAddr: IP and Port
	NodeAddr net.UDPAddr
	// DHT Node ID
	Nodeid string
	// DHT node runs a http server on this URL
	// if method POST: accept peer info update
	// if method GET: respond to peer info query
	PeerUpdateURL string
}

type patchEntry struct {
	reg Registration
}

type patch struct {
	Added   []patchEntry
	Removed []patchEntry
}
