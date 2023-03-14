package routingTable

import (
	"expvar"
	"fmt"
	"net"
	"time"

	"dht/logger"
	"dht/nettools"
	"dht/peer"
	"dht/remoteNode"
	"dht/util"
)

func NewRoutingTable(Log *logger.DebugLogger) *RoutingTable {
	return &RoutingTable{
		nTree:     &nTree{},
		Addresses: make(map[string]*remoteNode.RemoteNode),
		Log:       Log,
	}
}

type RoutingTable struct {
	*nTree
	// Addresses is a map of UDP Addresses in host:port format and
	// remoteNodes. A string is used because it's not possible to create
	// a map using net.UDPAddr
	// as a key.
	Addresses map[string]*remoteNode.RemoteNode

	// Neighborhood.
	NodeID       string // This shouldn't be here. Move neighborhood upkeep one level up?
	BoundaryNode *remoteNode.RemoteNode
	// How many prefix bits are shared between boundaryNode and nodeID.
	Proximity int

	Log *logger.DebugLogger
}

// hostPortToNode finds a node based on the specified hostPort specification,
// which should be a UDP Address in the form "host:port".
func (r *RoutingTable) HostPortToNode(hostPort string, port string) (
	node *remoteNode.RemoteNode, addr string, existed bool, err error) {
	if hostPort == "" {
		panic("programming error: hostPortToNode received a nil hostPort")
	}
	Address, err := net.ResolveUDPAddr(port, hostPort)
	if err != nil {
		return nil, "", false, err
	}
	if Address.String() == "" {
		return nil, "", false, fmt.Errorf("programming error: Address resolution for hostPortToNode returned an empty string")
	}
	n, existed := r.Addresses[Address.String()]
	if existed && n == nil {
		return nil, "", false, fmt.Errorf("programming error: hostPortToNode found nil node in Address table")
	}
	return n, Address.String(), existed, nil
}

func (r *RoutingTable) Length() int {
	return len(r.Addresses)
}

func (r *RoutingTable) ReachableNodes() (tbl map[string][]byte) {
	tbl = make(map[string][]byte)
	for addr, r := range r.Addresses {
		if addr == "" {
			(*r.Log).Debugf("ReachableNodes: found empty Address for node %x.", r.ID)
			continue
		}
		if r.Reachable && len(r.ID) == 20 {
			tbl[addr] = []byte(r.ID)
		}
	}

	hexID := fmt.Sprintf("%x", r.NodeID)
	// This creates a new expvar everytime, but the alternative is too
	// bothersome (get the current value, type cast it, ensure it
	// exists..). Also I'm not using NewInt because I don't want to publish
	// the value.
	v := new(expvar.Int)
	v.Set(int64(len(tbl)))
	ReachableNodes.Set(hexID, v)
	return

}

func (r *RoutingTable) NumNodes() int {
	return len(r.Addresses)
}

func IsValIDAddr(addr string) bool {
	if addr == "" {
		return false
	}
	if h, p, err := net.SplitHostPort(addr); h == "" || p == "" || err != nil {
		return false
	}
	return true
}

// update the existing routingTable entry for this node by setting its correct
// infohash ID. Gives an error if the node was not found.
func (r *RoutingTable) Update(node *remoteNode.RemoteNode, proto string) error {
	_, addr, existed, err := r.HostPortToNode(node.Address.String(), proto)
	if err != nil {
		return err
	}
	if !IsValIDAddr(addr) {
		return fmt.Errorf("routingTable.update received an invalID Address %v", addr)
	}
	if !existed {
		return fmt.Errorf("node missing from the routing table: %v", node.Address.String())
	}
	if node.ID != "" {
		r.nTree.Insert(node)
		totalNodes.Add(1)
		r.Addresses[addr].ID = node.ID
	}
	return nil
}

// insert the provIDed node into the routing table. Gives an error if another
// node already existed with that Address.
func (r *RoutingTable) Insert(node *remoteNode.RemoteNode, proto string) error {
	if node.Address.Port == 0 {
		return fmt.Errorf("routingTable.insert() got a node with Port=0")
	}
	if node.Address.IP.IsUnspecified() {
		return fmt.Errorf("routingTable.insert() got a node with a non-specified IP Address")
	}
	_, addr, existed, err := r.HostPortToNode(node.Address.String(), proto)
	if err != nil {
		return err
	}
	if !IsValIDAddr(addr) {
		return fmt.Errorf("routingTable.insert received an invalID Address %v", addr)

	}
	if existed {
		return nil // fmt.Errorf("node already existed in routing table: %v", node.Address.String())
	}
	r.Addresses[addr] = node
	// We don't know the ID of all nodes.
	if !remoteNode.BogusId(node.ID) {
		// recursive version of node insertion.
		r.nTree.Insert(node)
		totalNodes.Add(1)
	}
	return nil
}

// getOrCreateNode returns a node for hostPort, which can be an IP:port or
// Host:port, which will be resolved if possible.  Preferably return an entry
// that is already in the routing table, but create a new one otherwise, thus
// being IDempotent.
func (r *RoutingTable) GetOrCreateNode(ID string, hostPort string, proto string) (node *remoteNode.RemoteNode, err error) {
	node, addr, existed, err := r.HostPortToNode(hostPort, proto)
	if err != nil {
		return nil, err
	}
	if existed {
		return node, nil
	}
	udpAddr, err := net.ResolveUDPAddr(proto, addr)
	if err != nil {
		return nil, err
	}
	node = remoteNode.NewRemoteNode(*udpAddr, ID, r.Log)
	return node, r.Insert(node, proto)
}

func (r *RoutingTable) Kill(n *remoteNode.RemoteNode, p *peer.PeerStore) {
	delete(r.Addresses, n.Address.String())
	r.nTree.Cut(util.InfoHash(n.ID), 0)
	totalKilledNodes.Add(1)

	if r.BoundaryNode != nil && n.ID == r.BoundaryNode.ID {
		r.ResetNeighborhoodBoundary()
	}
	p.KillContact(nettools.BinaryToDottedPort(n.AddressBinaryFormat))
}

func (r *RoutingTable) ResetNeighborhoodBoundary() {
	r.Proximity = 0
	// Try to find a distant one within the neighborhood and promote it as
	// the most distant node in the neighborhood.
	neighbors := r.Lookup(util.InfoHash(r.NodeID))
	if len(neighbors) > 0 {
		r.BoundaryNode = neighbors[len(neighbors)-1]
		r.Proximity = CommonBits(r.NodeID, r.BoundaryNode.ID)
	}

}

func (r *RoutingTable) Cleanup(cleanupPeriod time.Duration, p *peer.PeerStore) (needPing []*remoteNode.RemoteNode) {
	needPing = make([]*remoteNode.RemoteNode, 0, 10)
	t0 := time.Now()
	// Needs some serious optimization.
	for addr, n := range r.Addresses {
		if addr != n.Address.String() {
			(*r.Log).Debugf("cleanup: node Address mismatches: %v != %v. Deleting node", addr, n.Address.String())
			r.Kill(n, p)
			continue
		}
		if addr == "" {
			(*r.Log).Debugf("cleanup: found empty Address for node %x. Deleting node", n.ID)
			r.Kill(n, p)
			continue
		}
		if n.Reachable {
			if len(n.PendingQueries) == 0 {
				goto PING
			}
			// Tolerate 2 cleanup cycles.
			if time.Since(n.LastResponseTime) > cleanupPeriod*2+(cleanupPeriod/15) {
				(*r.Log).Debugf("DHT: Old node seen %v ago. Deleting", time.Since(n.LastResponseTime))
				r.Kill(n, p)
				continue
			}
			if time.Since(n.LastResponseTime).Nanoseconds() < cleanupPeriod.Nanoseconds()/2 {
				// Seen recently. Don't need to ping.
				continue
			}

		} else {
			// Not Reachable.
			if len(n.PendingQueries) > util.MaxNodePendingQueries {
				// DIDn't reply to 2 consecutive queries.
				(*r.Log).Debugf("DHT: Node never replied to ping. Deleting. %v", n.Address)
				r.Kill(n, p)
				continue
			}
		}
	PING:
		needPing = append(needPing, n)
	}
	duration := time.Since(t0)
	// If this pauses the server for too long I may have to segment the cleanup.
	// 2000 nodes: it takes ~12ms
	// 4000 nodes: ~24ms.
	(*r.Log).Debugf("DHT: Routing table cleanup took %v\n", duration)
	return needPing
}

// neighborhoodUpkeep will update the routingtable if the node n is closer than
// the 8 nodes in our neighborhood, by replacing the least close one
// (boundary). n.ID is assumed to have length 20.
func (r *RoutingTable) NeighborhoodUpkeep(n *remoteNode.RemoteNode, proto string, p *peer.PeerStore) {
	if r.BoundaryNode == nil {
		r.AddNewNeighbor(n, false, proto, p)
		return
	}
	if r.Length() < util.KNodes {
		r.AddNewNeighbor(n, false, proto, p)
		return
	}
	cmp := CommonBits(r.NodeID, n.ID)
	if cmp == 0 {
		// Not significantly better.
		return
	}
	if cmp > r.Proximity {
		r.AddNewNeighbor(n, true, proto, p)
		return
	}
}

func (r *RoutingTable) AddNewNeighbor(n *remoteNode.RemoteNode, displaceBoundary bool, proto string, p *peer.PeerStore) {
	if err := r.Insert(n, proto); err != nil {
		(*r.Log).Debugf("addNewNeighbor error: %v", err)
		return
	}
	if displaceBoundary && r.BoundaryNode != nil {
		// This will also take care of setting a new boundary.
		r.Kill(r.BoundaryNode, p)
	} else {
		r.ResetNeighborhoodBoundary()
	}
	(*r.Log).Debugf("New neighbor added %s with proximity %d", nettools.BinaryToDottedPort(n.AddressBinaryFormat), r.Proximity)
}

// pingSlowly pings the remote nodes in needPing, distributing the pings
// throughout an interval of cleanupPeriod, to avoID network traffic bursts. It
// doesn't really send the pings, but signals to the main goroutine that it
// should ping the nodes, using the pingRequest channel.
func PingSlowly(pingRequest chan *remoteNode.RemoteNode, needPing []*remoteNode.RemoteNode, cleanupPeriod time.Duration, stop chan bool) {
	if len(needPing) == 0 {
		return
	}
	duration := cleanupPeriod - (1 * time.Minute)
	perPingWait := duration / time.Duration(len(needPing))
	for _, r := range needPing {
		pingRequest <- r
		select {
		case <-time.After(perPingWait):
		case <-stop:
			return
		}
	}
}

var (
	// totalKilledNodes is a monotonically increasing counter of times nodes were killed from
	// the routing table. If a node is later added to the routing table and killed again, it is
	// counted twice.
	totalKilledNodes = expvar.NewInt("totalKilledNodes")
	// totalNodes is a monotonically increasing counter of times nodes were added to the routing
	// table. If a node is removed then later added again, it is counted twice.
	totalNodes = expvar.NewInt("totalNodes")
	// ReachableNodes is the count of all Reachable nodes from a particular DHT node. The map
	// key is the local node's infohash. The value is a gauge with the count of Reachable nodes
	// at the latest time the routing table was persisted on disk.
	ReachableNodes = expvar.NewMap("ReachableNodes")
)
