// DHT node for Taipei Torrent, for tracker-less peer information exchange.
// Status: Supports all DHT operations from the specification.

package dht

// Summary from the bittorrent DHT protocol specification:
//
// Message types:
//  - query
//  - response
//  - error
//
// RPCs:
//      ping:
//         see if node is reachable and save it on routing table.
//      find_node:
//	       run when DHT node count drops, or every X minutes. Just to ensure
//	       our DHT routing table is still useful.
//      get_peers:
//	       the real deal. Iteratively queries DHT nodes and find new sources
//	       for a particular infohash.
//	announce_peer:
//         announce that the peer associated with this node is downloading a
//         torrent.
//
// Reference:
//     http://www.bittorrent.org/beps/bep_0005.html
//

import (
	"crypto/rand"
	"crypto/sha1"
	"expvar"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"dht/logger"
	"dht/peer"
	"dht/remoteNode"
	"dht/routingTable"
	"dht/util"
	"dht/util/arena"
)

// Config for the DHT Node. Use NewConfig to create a configuration with default values.
type Config struct {
	// IP Address to listen on.  If left blank, one is chosen automatically.
	Address string
	// UDP port the DHT node should listen on. If zero, it picks a random port.
	Port int
	// Number of peers that DHT will try to find for each infohash being searched. This might
	// later be moved to a per-infohash option. Default value: 5.
	NumTargetPeers int
	// Comma separated list of DHT routers used for bootstrapping the network.
	DHTRouters string
	// Maximum number of nodes to store in the routing table. Default value: 100.
	MaxNodes int
	// How often to ping nodes in the network to see if they are reachable. Default value: 15 min.
	CleanupPeriod time.Duration
	//  If true, the node will read the routing table from disk on startup and save routing
	//  table snapshots on disk every few minutes. Default value: true.
	SaveRoutingTable bool
	// How often to save the routing table to disk. Default value: 5 minutes.
	SavePeriod time.Duration
	// Maximum packets per second to be processed. Disabled if negative. Default value: 100.
	RateLimit int64
	// MaxInfoHashes is the limit of number of infohashes for which we should keep a peer list.
	// If this and MaxInfoHashPeers are unchanged, it should consume around 25 MB of RAM. Larger
	// values help keeping the DHT network healthy. Default value: 2048.
	MaxInfoHashes int
	// MaxInfoHashPeers is the limit of number of peers to be tracked for each infohash. A
	// single peer contact typically consumes 6 bytes. Default value: 256.
	MaxInfoHashPeers int
	// ClientPerMinuteLimit protects against spammy clients. Ignore their requests if exceeded
	// this number of packets per minute. Default value: 50.
	ClientPerMinuteLimit int
	// ThrottlerTrackedClients is the number of hosts the client throttler remembers. An LRU is used to
	// track the most interesting ones. Default value: 1000.
	ThrottlerTrackedClients int64
	// Protocol for UDP connections, udp4= IPv4, udp6 = IPv6
	UDPProto string
	//
	StartHTTPServer bool
	//
}

// Creates a *Config populated with default values.
func NewConfig() *Config {
	return &Config{
		Address:                 "",
		Port:                    0, // Picks a random port.
		NumTargetPeers:          5,
		DHTRouters:              "router.magnets.im:6881,router.bittorrent.com:6881,dht.transmissionbt.com:6881",
		MaxNodes:                500,
		CleanupPeriod:           15 * time.Minute,
		SaveRoutingTable:        true,
		SavePeriod:              5 * time.Minute,
		RateLimit:               100,
		MaxInfoHashes:           2048,
		MaxInfoHashPeers:        256,
		ClientPerMinuteLimit:    50,
		ThrottlerTrackedClients: 1000,
		UDPProto:                "udp4",
		StartHTTPServer:         true,
	}
}

var DefaultConfig = NewConfig()

// Registers Config fields as command line flags.  If c is nil, DefaultConfig
// is used.
func RegisterFlags(c *Config) {
	if c == nil {
		c = DefaultConfig
	}
	flag.StringVar(&c.DHTRouters, "routers", c.DHTRouters,
		"Comma separated addresses of DHT routers used to bootstrap the DHT network.")
	flag.IntVar(&c.MaxNodes, "maxNodes", c.MaxNodes,
		"Maximum number of nodes to store in the routing table, in memory. This is the primary configuration for how noisy or aggressive this node should be. When the node starts, it will try to reach d.config.MaxNodes/2 as quick as possible, to form a healthy routing table.")
	flag.DurationVar(&c.CleanupPeriod, "cleanupPeriod", c.CleanupPeriod,
		"How often to ping nodes in the network to see if they are reachable.")
	flag.DurationVar(&c.SavePeriod, "savePeriod", c.SavePeriod,
		"How often to save the routing table to disk.")
	flag.Int64Var(&c.RateLimit, "rateLimit", c.RateLimit,
		"Maximum packets per second to be processed. Beyond this limit they are silently dropped. Set to -1 to disable rate limiting.")
}

const (
	// Try to ensure that at least these many nodes are in the routing table.
	minNodes           = 16
	secretRotatePeriod = 5 * time.Minute
)

// DHT should be created by New(). It provides DHT features to a torrent
// client, such as finding new peers for torrent downloads without requiring a
// tracker.
type DHT struct {
	// PeersRequestResults receives results after user calls PeersRequest method.
	// Map key contains the 20 bytes infohash string, value contains the list of peer addresses.
	// Peer addresses are in binary format. You can use DecodePeerAddress function to decode peer addresses.
	PeersRequestResults chan map[util.InfoHash][]string
	// Logger contains hooks for a client to attach for certain RPCs.
	// Hooks is a better name for the job but we don't want to change it and break existing users.
	Logger Logger
	// DebugLogger is called with log messages.
	// By default, nothing is printed to the output from the library.
	// If you want to see log messages, you have to provide a DebugLogger implementation.
	DebugLogger logger.DebugLogger

	nodeId                 string
	config                 Config
	routingTable           *routingTable.RoutingTable
	peerStore              *peer.PeerStore
	conn                   *net.UDPConn
	exploredNeighborhood   bool
	RemoteNodeAcquaintance chan string
	peersRequest           chan ihReq
	nodesRequest           chan ihReq
	pingRequest            chan *remoteNode.RemoteNode
	portRequest            chan int
	removeInfoHash         chan util.InfoHash
	stop                   chan bool
	wg                     sync.WaitGroup
	clientThrottle         *util.ClientThrottle
	store                  *dhtStore
	tokenSecrets           []string
}

// New creates a DHT node. If config is nil, DefaultConfig will be used.
// Changing the config after calling this function has no effect.
//
// This method replaces NewDHTNode.
func New(config *Config) (node *DHT, err error) {
	if config == nil {
		config = DefaultConfig
	}
	// Copy to avoid changes.
	cfg := *config
	node = &DHT{
		config:               cfg,
		peerStore:            peer.NewPeerStore(cfg.MaxInfoHashes, cfg.MaxInfoHashPeers),
		PeersRequestResults:  make(chan map[util.InfoHash][]string, 1),
		stop:                 make(chan bool),
		DebugLogger:          &logger.NullLogger{},
		exploredNeighborhood: false,
		// Buffer to avoid blocking on sends.
		RemoteNodeAcquaintance: make(chan string, 100),
		// Buffer to avoid deadlocks and blocking on sends.
		peersRequest:   make(chan ihReq, 100),
		nodesRequest:   make(chan ihReq, 100),
		pingRequest:    make(chan *remoteNode.RemoteNode),
		portRequest:    make(chan int),
		removeInfoHash: make(chan util.InfoHash),
		clientThrottle: util.NewThrottler(cfg.ClientPerMinuteLimit, cfg.ThrottlerTrackedClients),
	}
	routingTable := routingTable.NewRoutingTable(&node.DebugLogger)
	node.routingTable = routingTable
	node.tokenSecrets = []string{node.newTokenSecret(), node.newTokenSecret()}
	c := openStore(cfg.Port, cfg.SaveRoutingTable)
	node.store = c
	if len(c.Id) != 20 {
		var err error
		c.Id, err = remoteNode.RandNodeId()
		if err != nil {
			return nil, err
		}
		node.DebugLogger.Debugf("Using a new random node ID: %x %d", c.Id, len(c.Id))
		saveStore(*c)
	}
	// The types don't match because JSON marshalling needs []byte.
	node.nodeId = string(c.Id)

	// XXX refactor.
	node.routingTable.NodeID = node.nodeId

	// This is called before the engine is up and ready to read from the
	// underlying channel.
	node.wg.Add(1)
	go func() {
		defer node.wg.Done()
		for addr := range c.Remotes {
			node.AddNode(addr)
		}
	}()
	return
}

func (d *DHT) newTokenSecret() string {
	b := make([]byte, 5)
	if _, err := rand.Read(b); err != nil {
		// This would return a string with up to 5 null chars.
		d.DebugLogger.Errorf("DHT: failed to generate random newTokenSecret: %v", err)
	}
	return string(b)
}

// Logger allows the DHT client to attach hooks for certain RPCs so it can log
// interesting events any way it wants.
type Logger interface {
	GetPeers(addr net.UDPAddr, queryID string, infoHash util.InfoHash)
}

type ihReq struct {
	ih      util.InfoHash
	options announceOptions
}

type announceOptions struct {
	announce bool
	port     int
}

// PeersRequest asks the DHT to search for more peers for the infoHash
// provided. announce should be true if the connected peer is actively
// downloading this infohash, which is normally the case - unless this DHT node
// is just a router that doesn't downloads torrents.
// The infoHash added to the store can be deleted with RemoveInfoHash method.
func (d *DHT) PeersRequest(ih string, announce bool) {
	d.PeersRequestPort(ih, announce, d.config.Port)
}

// PeersRequestPort is same as PeersRequest but it takes additional port argument to use in "announce_peer" request.
func (d *DHT) PeersRequestPort(ih string, announce bool, port int) {
	d.peersRequest <- ihReq{util.InfoHash(ih), announceOptions{announce, port}}
	d.DebugLogger.Infof("DHT: torrent client asking more peers for %x.", ih)
}

// RemoveInfoHash removes infoHash from local store.
// This method should be called when the peer is no longer downloading this infoHash.
func (d *DHT) RemoveInfoHash(ih string) {
	d.removeInfoHash <- util.InfoHash(ih)
	d.DebugLogger.Infof("DHT: torrent client removes info hash %x.", ih)
}

// Stop the DHT node.
func (d *DHT) Stop() {
	close(d.stop)
	d.wg.Wait()
}

// Port returns the port number assigned to the DHT. This is useful when
// when initialising the DHT with port 0, i.e. automatic port assignment,
// in order to retrieve the actual port number used.
func (d *DHT) Port() int {
	return <-d.portRequest
}

// AddNode informs the DHT of a new node it should add to its routing table.
// addr is a string containing the target node's "host:port" UDP address.
func (d *DHT) AddNode(addr string) {
	d.RemoteNodeAcquaintance <- addr
}

// Asks for more peers for a torrent.
func (d *DHT) getPeers(infoHash util.InfoHash) {
	closest := d.routingTable.LookupFiltered(infoHash)
	if len(closest) == 0 {
		for _, s := range strings.Split(d.config.DHTRouters, ",") {
			if s != "" {
				r, e := d.routingTable.GetOrCreateNode("", s, d.config.UDPProto)
				if e == nil {
					d.getPeersFrom(r, infoHash)
				}
			}
		}
	}
	for _, r := range closest {
		d.getPeersFrom(r, infoHash)
	}
}

// Find a DHT node.
func (d *DHT) findNode(id string) {
	ih := util.InfoHash(id)
	closest := d.routingTable.LookupFiltered(ih)
	if len(closest) == 0 {
		for _, s := range strings.Split(d.config.DHTRouters, ",") {
			if s != "" {
				r, e := d.routingTable.GetOrCreateNode("", s, d.config.UDPProto)
				if e == nil {
					d.findNodeFrom(r, id)
				}
			}
		}
	}
	for _, r := range closest {
		d.findNodeFrom(r, id)
	}
}

// Start launches the dht node. It starts a listener
// on the desired address, then runs the main loop in a
// separate go routine - Start replaces Run and will
// always return, with nil if the dht successfully
// started or with an error either. d.Stop() is expected
// by the caller to stop the dht
func (d *DHT) Start() (err error) {
	if err = d.initSocket(); err == nil {
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			d.loop()
		}()
	}
	return err
}

// Run launches the dht node. It starts a listener
// on the desired address, then runs the main loop in the
// same go routine.
// If initSocket fails, Run returns with the error.
// If initSocket succeeds, Run blocks until d.Stop() is called.
// DEPRECATED - Start should be used instead of Run
func (d *DHT) Run() error {
	d.DebugLogger.Infof("dht.Run() is deprecated, use dht.Start() instead")
	if err := d.initSocket(); err != nil {
		return err
	}
	d.loop()
	return nil
}

// initSocket initializes the udp socket
// listening to incoming dht requests
func (d *DHT) initSocket() (err error) {
	d.conn, err = remoteNode.Listen(d.config.Address, d.config.Port, d.config.UDPProto, d.DebugLogger)
	if err != nil {
		return err
	}

	// Update the stored port number in case it was set 0, meaning it was
	// set automatically by the system
	d.config.Port = d.conn.LocalAddr().(*net.UDPAddr).Port
	return nil
}

func (d *DHT) bootstrap() {
	// Bootstrap the network (only if there are configured dht routers).
	for _, s := range strings.Split(d.config.DHTRouters, ",") {
		if s != "" {
			d.ping(s)
			r, e := d.routingTable.GetOrCreateNode("", s, d.config.UDPProto)
			if e == nil {
				d.findNodeFrom(r, d.nodeId)
			}
		}
	}
	d.findNode(d.nodeId)
	d.getMorePeers(nil)
}

// loop is the main working section of dht.
// It bootstraps a routing table, if necessary,
// and listens for incoming DHT requests until d.Stop()
// is called from another go routine.
func (d *DHT) loop() {
	// Close socket
	defer d.conn.Close()

	// There is goroutine pushing and one popping items out of the arena.
	// One passes work to the other. So there is little contention in the
	// arena, so it doesn't need many items (it used to have 500!). If
	// readFromSocket or the packet processing ever need to be
	// parallelized, this would have to be bumped.
	bytesArena := arena.NewArena(remoteNode.MaxUDPPacketSize, 3)
	socketChan := make(chan remoteNode.PacketType)
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		remoteNode.ReadFromSocket(d.conn, socketChan, bytesArena, d.stop, d.DebugLogger)
	}()

	d.bootstrap()

	cleanupTicker := time.NewTicker(d.config.CleanupPeriod).C
	secretRotateTicker := time.NewTicker(secretRotatePeriod).C

	saveTicker := make(<-chan time.Time)
	if d.store != nil {
		saveTicker = time.NewTicker(d.config.SavePeriod).C
	}

	var fillTokenBucket <-chan time.Time
	tokenBucket := d.config.RateLimit

	if d.config.RateLimit < 0 {
		d.DebugLogger.Infof("rate limiting disabled")
	} else {
		// Token bucket for limiting the number of packets per second.
		fillTokenBucket = time.NewTicker(time.Second / 10).C
		if d.config.RateLimit > 0 && d.config.RateLimit < 10 {
			// Less than 10 leads to rounding problems.
			d.config.RateLimit = 10
		}
	}
	d.DebugLogger.Infof("DHT: Starting DHT node %x on port %d.", d.nodeId, d.config.Port)

	// if d.config.StartHTTPServer {
	// 	d.DebugLogger.Infof("HTTP server started on localhost:6666")
	// 	go d.StartHTTPServer("localhost", "6666")
	// }

	for {
		select {
		case <-d.stop:
			d.DebugLogger.Infof("DHT exiting.")
			d.clientThrottle.Stop()
			return
		case addr := <-d.RemoteNodeAcquaintance:
			d.helloFromPeer(addr)
		case req := <-d.peersRequest:
			// torrent server is asking for more peers for infoHash.  Ask the closest
			// nodes for directions. The goroutine will write into the
			// PeersNeededResults channel.

			// Drain all requests sitting in the channel and de-dupe them.
			m := map[util.InfoHash]announceOptions{req.ih: req.options}
		P:
			for {
				select {
				case req = <-d.peersRequest:
					m[req.ih] = req.options
				default:
					// Channel drained.
					break P
				}
			}
			// Process each unique infohash for which there were requests.
			for ih, options := range m {
				if options.announce {
					d.peerStore.AddLocalDownload(ih, options.port)
				}

				d.getPeers(ih) // I might have enough peers in the peerstore, but no seeds
			}

		case ih := <-d.removeInfoHash:
			d.peerStore.RemoveLocalDownload(ih)
		case req := <-d.nodesRequest:
			m := map[util.InfoHash]bool{req.ih: true}
		L:
			for {
				select {
				case req = <-d.nodesRequest:
					m[req.ih] = true
				default:
					// Channel drained.
					break L
				}
			}
			for ih := range m {
				d.findNode(string(ih))
			}

		case p := <-socketChan:
			totalRecv.Add(1)
			if d.config.RateLimit > 0 {
				if tokenBucket > 0 {
					d.processPacket(p)
					tokenBucket -= 1
				} else {
					// TODO In the future it might be better to avoid dropping things like ping replies.
					totalDroppedPackets.Add(1)
				}
			} else {
				d.processPacket(p)
			}
			bytesArena.Push(p.B)

		case <-fillTokenBucket:
			if tokenBucket < d.config.RateLimit {
				tokenBucket += d.config.RateLimit / 10
			}
		case <-cleanupTicker:
			needPing := d.routingTable.Cleanup(d.config.CleanupPeriod, d.peerStore)
			d.wg.Add(1)
			go func() {
				defer d.wg.Done()
				routingTable.PingSlowly(d.pingRequest, needPing, d.config.CleanupPeriod, d.stop)
			}()
			if d.needMoreNodes() {
				d.bootstrap()
			}
		case node := <-d.pingRequest:
			d.pingNode(node)
		case <-secretRotateTicker:
			d.tokenSecrets = []string{d.newTokenSecret(), d.tokenSecrets[0]}
		case d.portRequest <- d.config.Port:
			continue
		case <-saveTicker:
			tbl := d.routingTable.ReachableNodes()
			if len(tbl) > 5 {
				d.store.Remotes = tbl
				saveStore(*d.store)
			}
		}
	}
}

func (d *DHT) needMoreNodes() bool {
	n := d.routingTable.NumNodes()
	return n < minNodes || n*2 < d.config.MaxNodes
}

func (d *DHT) needMorePeers(ih util.InfoHash) bool {
	return d.peerStore.Alive(ih) < d.config.NumTargetPeers
}

func (d *DHT) getMorePeers(r *remoteNode.RemoteNode) {
	for ih := range d.peerStore.LocalActiveDownloads {
		if d.needMorePeers(ih) {
			if r == nil {
				d.getPeers(ih)
			} else {
				d.getPeersFrom(r, ih)
			}
		}
	}
}

func (d *DHT) helloFromPeer(addr string) {
	// We've got a new node id. We need to:
	// - see if we know it already, skip accordingly.
	// - ping it and see if it's reachable.
	// - if it responds, save it in the routing table.
	_, addrResolved, existed, err := d.routingTable.HostPortToNode(addr, d.config.UDPProto)
	if err != nil {
		d.DebugLogger.Debugf("helloFromPeer error: %v", err)
		return
	}
	if existed {
		// Node host+port already known.
		return
	}
	if d.routingTable.Length() < d.config.MaxNodes {
		d.ping(addrResolved)
		return
	}
}

func (d *DHT) ADDHonestPeer(id, addr string) error {
	// We've got a new node id. We need to:
	// - see if we know it already, skip accordingly.
	// - ping it and see if it's reachable.
	// - if it responds, save it in the routing table.
	_, _, existed, err := d.routingTable.HostPortToNode(addr, d.config.UDPProto)
	if existed {
		return nil
	}
	if err != nil {
		d.DebugLogger.Debugf("AddHonestNode error: %v", err)
		return err
	}
	if d.routingTable.Length()+1 < d.config.MaxNodes {
		r, err := d.routingTable.GetOrCreateNode(id, addr, d.config.UDPProto)
		if err != nil {
			d.DebugLogger.Debugf("AddHonestNode error: %v", err)
		}
		log.Printf("node %v added", &r.Address)
		d.pingNode(r)
		return nil
	}
	return nil
}

func (d *DHT) processPacket(p remoteNode.PacketType) {
	d.DebugLogger.Debugf("DHT processing packet from %v", p.Raddr.String())
	if !d.clientThrottle.CheckBlock(p.Raddr.IP.String()) {
		totalPacketsFromBlockedHosts.Add(1)
		d.DebugLogger.Debugf("Node exceeded rate limiter. Dropping packet.")
		return
	}
	if p.B[0] != 'd' {
		// Malformed DHT packet. There are protocol extensions out
		// there that we don't support or understand.
		d.DebugLogger.Debugf("Malformed DHT packet.")
		return
	}
	r, err := remoteNode.ReadResponse(p, d.DebugLogger)
	if err != nil {
		d.DebugLogger.Debugf("DHT: readResponse Error: %v, %q", err, string(p.B))
		return
	}
	switch {
	// Response.
	case r.Y == "r":
		d.DebugLogger.Debugf("DHT processing response from %x", r.R.Id)
		if remoteNode.BogusId(r.R.Id) {
			d.DebugLogger.Debugf("DHT received packet with bogus node id %x", r.R.Id)
			return
		}
		if r.R.Id == d.nodeId {
			d.DebugLogger.Debugf("DHT received reply from self, id %x", r.A.Id)
			return
		}
		node, addr, existed, err := d.routingTable.HostPortToNode(p.Raddr.String(), d.config.UDPProto)
		if err != nil {
			d.DebugLogger.Debugf("DHT readResponse error processing response: %v", err)
			return
		}
		if !existed {
			d.DebugLogger.Debugf("DHT: Received reply from a host we don't know: %v", p.Raddr)
			if d.routingTable.Length() < d.config.MaxNodes {
				d.ping(addr)
			}
			return
		}
		// Fix the node ID.
		if node.ID == "" {
			node.ID = r.R.Id
			d.routingTable.Update(node, d.config.UDPProto)
		}
		if node.ID != r.R.Id {
			d.DebugLogger.Debugf("DHT: Node changed IDs %x => %x", node.ID, r.R.Id)
		}
		if query, ok := node.PendingQueries[r.T]; ok {
			d.DebugLogger.Debugf("DHT: Received reply to %v", query.Type)
			if !node.Reachable {
				node.Reachable = true
				totalNodesReached.Add(1)
			}
			node.LastResponseTime = time.Now()
			node.PastQueries[r.T] = query
			d.routingTable.NeighborhoodUpkeep(node, d.config.UDPProto, d.peerStore)

			// If this is the first host added to the routing table, attempt a
			// recursive Lookup of our own address, to build our neighborhood ASAP.
			if d.needMoreNodes() {
				d.DebugLogger.Debugf("DHT: need more nodes")
				d.findNode(d.nodeId)
			}
			d.exploredNeighborhood = true

			switch query.Type {
			case "ping":
				// Served its purpose, nothing else to be done.
				totalRecvPingReply.Add(1)
			case "get_peers":
				d.DebugLogger.Debugf("DHT: got get_peers response")
				d.processGetPeerResults(node, r)
			case "find_node":
				d.DebugLogger.Debugf("DHT: got find_node response")
				d.processFindNodeResults(node, r)
			case "announce_peer":
				// Nothing to do. In the future, update counters.
			default:
				d.DebugLogger.Debugf("DHT: Unknown query type: %v from %v", query.Type, addr)
			}
			delete(node.PendingQueries, r.T)
		} else {
			d.DebugLogger.Debugf("DHT: Unknown query id: %v", r.T)
		}
	case r.Y == "q":
		if r.A.Id == d.nodeId {
			d.DebugLogger.Debugf("DHT received packet from self, id %x", r.A.Id)
			return
		}
		node, addr, existed, err := d.routingTable.HostPortToNode(p.Raddr.String(), d.config.UDPProto)
		if err != nil {
			d.DebugLogger.Debugf("Error readResponse error processing query: %v", err)
			return
		}
		if !existed {
			// Another candidate for the routing table. See if it's reachable.
			if d.routingTable.Length() < d.config.MaxNodes {
				d.ping(addr)
			}
		}
		d.DebugLogger.Debugf("DHT processing %v request", r.Q)
		switch r.Q {
		case "ping":
			d.replyPing(p.Raddr, r)
		case "get_peers":
			d.replyGetPeers(p.Raddr, r)
		case "find_node":
			d.replyFindNode(p.Raddr, r)
		case "announce_peer":
			d.replyAnnouncePeer(p.Raddr, node, r)
		default:
			d.DebugLogger.Debugf("DHT: non-implemented handler for type %v", r.Q)
		}
	default:
		d.DebugLogger.Debugf("DHT: Bogus DHT query from %v.", p.Raddr)
	}
}

func (d *DHT) ping(address string) {
	r, err := d.routingTable.GetOrCreateNode("", address, d.config.UDPProto)
	if err != nil {
		d.DebugLogger.Debugf("ping error for address %v: %v", address, err)
		return
	}
	d.pingNode(r)
}

func (d *DHT) pingNode(r *remoteNode.RemoteNode) {
	d.DebugLogger.Debugf("DHT: ping => %+v", r.Address)
	t := r.NewQuery("ping")

	queryArguments := map[string]interface{}{"id": d.nodeId}
	query := remoteNode.QueryMessage{t, "q", "ping", queryArguments}
	remoteNode.SendMsg(d.conn, r.Address, query, d.DebugLogger)
	totalSentPing.Add(1)
}

func (d *DHT) getPeersFrom(r *remoteNode.RemoteNode, ih util.InfoHash) {
	if r == nil {
		return
	}
	totalSentGetPeers.Add(1)
	ty := "get_peers"
	transId := r.NewQuery(ty)
	if _, ok := r.PendingQueries[transId]; ok {
		r.PendingQueries[transId].IH = ih
	} else {
		r.PendingQueries[transId] = &remoteNode.QueryType{IH: ih}
	}
	queryArguments := map[string]interface{}{
		"id":        d.nodeId,
		"info_hash": ih,
	}
	query := remoteNode.QueryMessage{transId, "q", ty, queryArguments}
	d.DebugLogger.Debugf("DHT sending get_peers. nodeID: %x@%v, InfoHash: %x , distance: %x", r.ID, r.Address, ih, util.HashDistance(util.InfoHash(r.ID), ih))
	r.LastSearchTime = time.Now()
	remoteNode.SendMsg(d.conn, r.Address, query, d.DebugLogger)
}

func (d *DHT) findNodeFrom(r *remoteNode.RemoteNode, id string) {
	if r == nil {
		return
	}
	totalSentFindNode.Add(1)
	ty := "find_node"
	transId := r.NewQuery(ty)
	ih := util.InfoHash(id)
	d.DebugLogger.Debugf("findNodeFrom adding pendingQueries transId=%v ih=%x", transId, ih)
	if _, ok := r.PendingQueries[transId]; ok {
		r.PendingQueries[transId].IH = ih
	} else {
		r.PendingQueries[transId] = &remoteNode.QueryType{IH: ih}
	}
	queryArguments := map[string]interface{}{
		"id":     d.nodeId,
		"target": id,
	}
	query := remoteNode.QueryMessage{transId, "q", ty, queryArguments}
	d.DebugLogger.Debugf("DHT sending find_node. nodeID: %x@%v, target ID: %x , distance: %x", r.ID, r.Address, id, util.HashDistance(util.InfoHash(r.ID), ih))
	r.LastSearchTime = time.Now()
	remoteNode.SendMsg(d.conn, r.Address, query, d.DebugLogger)
}

// announcePeer sends a message to the destination address to advertise that
// our node is a peer for this infohash, using the provided token to
// 'authenticate'.
func (d *DHT) announcePeer(address net.UDPAddr, ih util.InfoHash, port int, token string) {
	r, err := d.routingTable.GetOrCreateNode("", address.String(), d.config.UDPProto)
	if err != nil {
		d.DebugLogger.Debugf("announcePeer error: %v", err)
		return
	}
	ty := "announce_peer"
	d.DebugLogger.Debugf("DHT: announce_peer => address: %v, ih: %x, token: %x", address, ih, token)
	transId := r.NewQuery(ty)
	queryArguments := map[string]interface{}{
		"id":        d.nodeId,
		"info_hash": ih,
		"port":      port,
		"token":     token,
	}
	query := remoteNode.QueryMessage{transId, "q", ty, queryArguments}
	remoteNode.SendMsg(d.conn, address, query, d.DebugLogger)
}

func (d *DHT) hostToken(addr net.UDPAddr, secret string) string {
	h := sha1.New()
	io.WriteString(h, addr.String())
	io.WriteString(h, secret)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (d *DHT) checkToken(addr net.UDPAddr, token string) bool {
	match := false
	for _, secret := range d.tokenSecrets {
		if d.hostToken(addr, secret) == token {
			match = true
			break
		}
	}
	d.DebugLogger.Debugf("checkToken for %v, %q matches? %v", addr, token, match)
	return match
}

func (d *DHT) replyAnnouncePeer(addr net.UDPAddr, node *remoteNode.RemoteNode, r remoteNode.ResponseType) {
	ih := util.InfoHash(r.A.InfoHash)
	d.DebugLogger.Debugf("DHT: announce_peer. Host %v, nodeID: %x, infoHash: %x, peerPort %d, distance to me %x",
		addr, r.A.Id, ih, r.A.Port, util.HashDistance(ih, util.InfoHash(d.nodeId)),
	)
	// node can be nil if, for example, the server just restarted and received an announce_peer
	// from a node it doesn't yet know about.
	if node != nil && d.checkToken(addr, r.A.Token) {
		peerAddr := net.TCPAddr{IP: addr.IP, Port: r.A.Port}
		d.peerStore.AddContact(ih, util.DottedPortToBinary(peerAddr.String()))
		// Allow searching this node immediately, since it's telling us
		// it has an infohash. Enables faster upgrade of other nodes to
		// "peer" of an infohash, if the announcement is valid.
		node.LastResponseTime = time.Now().Add(-remoteNode.SearchRetryPeriod)
		port := d.peerStore.HasLocalDownload(ih)
		if port != 0 {
			d.PeersRequestResults <- map[util.InfoHash][]string{ih: {util.DottedPortToBinary(peerAddr.String())}}
		}
	}
	// Always reply positively. jech says this is to avoid "back-tracking", not sure what that means.
	reply := remoteNode.ReplyMessage{
		T: r.T,
		Y: "r",
		R: map[string]interface{}{"id": d.nodeId},
	}
	remoteNode.SendMsg(d.conn, addr, reply, d.DebugLogger)
}

func (d *DHT) replyGetPeers(addr net.UDPAddr, r remoteNode.ResponseType) {
	totalRecvGetPeers.Add(1)
	d.DebugLogger.Debugf("DHT get_peers. Host: %v , nodeID: %x , InfoHash: %x , distance to me: %x",
		addr, r.A.Id, util.InfoHash(r.A.InfoHash), util.HashDistance(r.A.InfoHash, util.InfoHash(d.nodeId)))

	if d.Logger != nil {
		d.Logger.GetPeers(addr, r.A.Id, r.A.InfoHash)
	}

	ih := r.A.InfoHash
	r0 := map[string]interface{}{"id": d.nodeId, "token": d.hostToken(addr, d.tokenSecrets[0])}
	reply := remoteNode.ReplyMessage{
		T: r.T,
		Y: "r",
		R: r0,
	}

	if peerContacts := d.peersForInfoHash(ih); len(peerContacts) > 0 {
		reply.R["values"] = peerContacts
	} else {
		reply.R["nodes"] = d.nodesForInfoHash(ih)
	}
	remoteNode.SendMsg(d.conn, addr, reply, d.DebugLogger)
}

func (d *DHT) nodesForInfoHash(ih util.InfoHash) string {
	n := make([]string, 0, util.KNodes)
	for _, r := range d.routingTable.Lookup(ih) {
		// r is nil when the node was filtered.
		if r != nil {
			binaryHost := r.ID + util.DottedPortToBinary(r.Address.String())
			if binaryHost == "" {
				d.DebugLogger.Debugf("killing node with bogus address %v", r.Address.String())
				d.routingTable.Kill(r, d.peerStore)
			} else {
				n = append(n, binaryHost)
			}
		}
	}
	d.DebugLogger.Debugf("replyGetPeers: Nodes only. Giving %d", len(n))
	return strings.Join(n, "")
}

func (d *DHT) peersForInfoHash(ih util.InfoHash) []string {
	peerContacts := d.peerStore.PeerContacts(ih)
	if len(peerContacts) > 0 {
		d.DebugLogger.Debugf("replyGetPeers: Giving peers! %x was requested, and we knew %d peers!", ih, len(peerContacts))
	}
	return peerContacts
}

func (d *DHT) replyFindNode(addr net.UDPAddr, r remoteNode.ResponseType) {
	totalRecvFindNode.Add(1)
	d.DebugLogger.Debugf("DHT find_node. Host: %v , nodeId: %x , target ID: %x , distance to me: %x",
		addr, r.A.Id, r.A.Target, util.HashDistance(util.InfoHash(r.A.Target), util.InfoHash(d.nodeId)))

	node := util.InfoHash(r.A.Target)
	r0 := map[string]interface{}{"id": d.nodeId}
	reply := remoteNode.ReplyMessage{
		T: r.T,
		Y: "r",
		R: r0,
	}

	neighbors := d.routingTable.LookupFiltered(node)
	if len(neighbors) < util.KNodes {
		neighbors = append(neighbors, d.routingTable.Lookup(node)...)
	}
	n := make([]string, 0, util.KNodes)
	for _, r := range neighbors {
		n = append(n, r.ID+r.AddressBinaryFormat)
		if len(n) == util.KNodes {
			break
		}
	}
	d.DebugLogger.Debugf("replyFindNode: Nodes only. Giving %d", len(n))
	reply.R["nodes"] = strings.Join(n, "")
	remoteNode.SendMsg(d.conn, addr, reply, d.DebugLogger)
}

func (d *DHT) replyPing(addr net.UDPAddr, response remoteNode.ResponseType) {
	d.DebugLogger.Debugf("DHT: reply ping => %v", addr)
	reply := remoteNode.ReplyMessage{
		T: response.T,
		Y: "r",
		R: map[string]interface{}{"id": d.nodeId},
	}
	remoteNode.SendMsg(d.conn, addr, reply, d.DebugLogger)
}

// Process another node's response to a get_peers query. If the response
// contains peers, send them to the Torrent engine, our client, using the
// DHT.PeersRequestResults channel. If it contains closest nodes, query
// them if we still need it. Also announce ourselves as a peer for that node,
// unless we are in supernode mode.
func (d *DHT) processGetPeerResults(node *remoteNode.RemoteNode, resp remoteNode.ResponseType) {
	totalRecvGetPeersReply.Add(1)

	query, _ := node.PendingQueries[resp.T]
	port := d.peerStore.HasLocalDownload(query.IH)
	if port != 0 {
		d.announcePeer(node.Address, query.IH, port, resp.R.Token)
	}
	if resp.R.Values != nil {
		peers := make([]string, 0)
		for _, peerContact := range resp.R.Values {
			// send peer even if we already have it in store
			// the underlying client does/should handle dupes
			d.peerStore.AddContact(query.IH, peerContact)
			peers = append(peers, peerContact)
		}
		if len(peers) > 0 {
			// Finally, new peers.
			result := map[util.InfoHash][]string{query.IH: peers}
			totalPeers.Add(int64(len(peers)))
			d.DebugLogger.Debugf("DHT: processGetPeerResults, totalPeers: %v", totalPeers.String())
			select {
			case d.PeersRequestResults <- result:
			case <-d.stop:
				// if we're closing down and the caller has stopped reading
				// from PeersRequestResults, drop the result.
			}
		}
	}
	var nodelist string

	if d.config.UDPProto == "udp4" {
		nodelist = resp.R.Nodes
	} else if d.config.UDPProto == "udp6" {
		nodelist = resp.R.Nodes6
	}
	d.DebugLogger.Debugf("DHT: handling get_peers results len(nodelist)=%d", len(nodelist))
	if nodelist != "" {
		for id, address := range remoteNode.ParseNodesString(nodelist, d.config.UDPProto, d.DebugLogger) {
			if id == d.nodeId {
				d.DebugLogger.Debugf("DHT got reference of self for get_peers, id %x", id)
				continue
			}

			// If it's in our routing table already, ignore it.
			_, addr, existed, err := d.routingTable.HostPortToNode(address, d.config.UDPProto)
			if err != nil {
				d.DebugLogger.Debugf("DHT error parsing get peers node: %v", err)
				continue
			}
			if addr == node.Address.String() {
				// This smartass is probably trying to
				// sniff the network, or attract a lot
				// of traffic to itself. Ignore all
				// their results.
				totalSelfPromotions.Add(1)
				continue
			}
			if existed {
				d.DebugLogger.Debugf("DHT: processGetPeerResults DUPE node reference: %x@%v from %x@%v. Distance: %x.",
					id, address, node.ID, node.Address, util.HashDistance(query.IH, util.InfoHash(node.ID)))
				totalGetPeersDupes.Add(1)
			} else {
				// And it is actually new. Interesting.
				d.DebugLogger.Debugf("DHT: Got new node reference: %x@%v from %x@%v. Distance: %x.",
					id, address, node.ID, node.Address, util.HashDistance(query.IH, util.InfoHash(node.ID)))
				if _, err := d.routingTable.GetOrCreateNode(id, addr, d.config.UDPProto); err == nil && d.needMorePeers(query.IH) {
					// Re-add this request to the queue. This would in theory
					// batch similar requests, because new nodes are already
					// available in the routing table and will be used at the
					// next opportunity - before this particular channel send is
					// processed. As soon we reach target number of peers these
					// channel sends become noops.
					//
					// Setting the announce parameter to false because it's not
					// needed here: if this node is downloading that particular
					// infohash, that has already been recorded with
					// peerStore.addLocalDownload(). The announcement itself is
					// sent not when get_peers is sent, but when processing the
					// reply to get_peers.
					//
					select {
					case d.peersRequest <- ihReq{ih: query.IH}:
					default:
						// The channel is full, so drop this item. The node
						// was added to the routing table already, so it
						// will be used next time getPeers() is called -
						// assuming it's close enough to the ih.
					}
				}
			}
		}
	}
}

// Process another node's response to a find_node query.
func (d *DHT) processFindNodeResults(node *remoteNode.RemoteNode, resp remoteNode.ResponseType) {
	var nodelist string
	totalRecvFindNodeReply.Add(1)

	query, _ := node.PendingQueries[resp.T]
	if d.config.UDPProto == "udp4" {
		nodelist = resp.R.Nodes
	} else if d.config.UDPProto == "udp6" {
		nodelist = resp.R.Nodes6
	}
	d.DebugLogger.Debugf("processFindNodeResults find_node = %s len(nodelist)=%d", util.BinaryToDottedPort(node.AddressBinaryFormat), len(nodelist))

	if nodelist != "" {
		for id, address := range remoteNode.ParseNodesString(nodelist, d.config.UDPProto, d.DebugLogger) {
			_, addr, existed, err := d.routingTable.HostPortToNode(address, d.config.UDPProto)
			if err != nil {
				d.DebugLogger.Debugf("DHT error parsing node from find_find response: %v", err)
				continue
			}
			if id == d.nodeId {
				d.DebugLogger.Debugf("DHT got reference of self for find_node, id %x", id)
				continue
			}
			if addr == node.Address.String() {
				// SelfPromotions are more common for find_node. They are
				// happening even for router.bittorrent.com
				totalSelfPromotions.Add(1)
				continue
			}
			if existed {
				d.DebugLogger.Debugf("DHT: processFindNodeResults DUPE node reference, query %x: %x@%v from %x@%v. Distance: %x.",
					query.IH, id, address, node.ID, node.Address, util.HashDistance(query.IH, util.InfoHash(node.ID)))
				totalFindNodeDupes.Add(1)
			} else {
				d.DebugLogger.Debugf("DHT: Got new node reference, query %x: %x@%v from %x@%v. Distance: %x.",
					query.IH, id, address, node.ID, node.Address, util.HashDistance(query.IH, util.InfoHash(node.ID)))
				// Includes the node in the routing table and ignores errors.
				//
				// Only continue the search if we really have to.
				r, err := d.routingTable.GetOrCreateNode(id, addr, d.config.UDPProto)
				if err != nil {
					d.DebugLogger.Debugf("processFindNodeResults calling getOrCreateNode: %v. Id=%x, Address=%q", err, id, addr)
					continue
				}
				if d.needMoreNodes() {
					select {
					case d.nodesRequest <- ihReq{ih: query.IH}:
					default:
						// Too many find_node commands queued up. Dropping
						// this. The node has already been added to the
						// routing table so we're not losing any
						// information.
					}
				}
				d.getMorePeers(r)
			}
		}
	}
}

var (
	totalNodesReached            = expvar.NewInt("totalNodesReached")
	totalGetPeersDupes           = expvar.NewInt("totalGetPeersDupes")
	totalFindNodeDupes           = expvar.NewInt("totalFindNodeDupes")
	totalSelfPromotions          = expvar.NewInt("totalSelfPromotions")
	totalPeers                   = expvar.NewInt("totalPeers")
	totalSentPing                = expvar.NewInt("totalSentPing")
	totalSentGetPeers            = expvar.NewInt("totalSentGetPeers")
	totalSentFindNode            = expvar.NewInt("totalSentFindNode")
	totalRecvGetPeers            = expvar.NewInt("totalRecvGetPeers")
	totalRecvGetPeersReply       = expvar.NewInt("totalRecvGetPeersReply")
	totalRecvPingReply           = expvar.NewInt("totalRecvPingReply")
	totalRecvFindNode            = expvar.NewInt("totalRecvFindNode")
	totalRecvFindNodeReply       = expvar.NewInt("totalRecvFindNodeReply")
	totalPacketsFromBlockedHosts = expvar.NewInt("totalPacketsFromBlockedHosts")
	totalDroppedPackets          = expvar.NewInt("totalDroppedPackets")
	totalRecv                    = expvar.NewInt("totalRecv")
)
