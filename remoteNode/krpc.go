package remoteNode

import (
	"bytes"
	"crypto/rand"
	"expvar"
	"net"
	"strconv"
	"time"

	"dht/logger"
	"dht/util"
	"dht/util/arena"

	bencode "github.com/jackpal/bencode-go"
)

// Search a node again after some time.
var SearchRetryPeriod = 15 * time.Second

type QueryType struct {
	Type    string
	IH      util.InfoHash
	srcNode string
}

const (
	// Once in a while I get a few bigger ones, but meh.
	MaxUDPPacketSize = 4096
	V4nodeContactLen = 26
	V6nodeContactLen = 38 // some clients seem to send multiples of 38
	NodeIdLen        = 20
)

var (
	TotalSent         = expvar.NewInt("totalSent")
	TotalReadBytes    = expvar.NewInt("totalReadBytes")
	TotalWrittenBytes = expvar.NewInt("totalWrittenBytes")
)

// The 'nodes' response is a string with fixed length contacts concatenated arbitrarily.
func ParseNodesString(nodes string, proto string, log logger.DebugLogger) (parsed map[string]string) {
	var nodeContactLen int
	if proto == "udp4" {
		nodeContactLen = V4nodeContactLen
	} else if proto == "udp6" {
		nodeContactLen = V6nodeContactLen
	} else {
		return
	}
	parsed = make(map[string]string)
	if len(nodes)%nodeContactLen > 0 {
		log.Debugf("DHT: len(NodeString) = %d, INVALID LENGTH, should be a multiple of %d", len(nodes), nodeContactLen)
		log.Debugf("%T %#v\n", nodes, nodes)
		return
	} else {
		log.Debugf("DHT: len(NodeString) = %d, had %d nodes, nodeContactLen=%d\n", len(nodes), len(nodes)/nodeContactLen, nodeContactLen)
	}
	for i := 0; i < len(nodes); i += nodeContactLen {
		id := nodes[i : i+NodeIdLen]
		address := util.BinaryToDottedPort(nodes[i+NodeIdLen : i+nodeContactLen])
		parsed[id] = address
	}
	return

}

type GetPeersResponse struct {
	// TODO: argh, values can be a string depending on the client (e.g: original bittorrent).
	Values []string "values"
	Id     string   "id"
	Nodes  string   "nodes"
	Nodes6 string   "nodes6"
	Token  string   "token"
}

type AnswerType struct {
	Id       string        "id"
	Target   string        "target"
	InfoHash util.InfoHash "info_hash" // should probably be a string.
	Port     int           "port"
	Token    string        "token"
}

// Generic stuff we read from the wire, not knowing what it is. This is as generic as can be.
type ResponseType struct {
	T string           "t"
	Y string           "y"
	Q string           "q"
	R GetPeersResponse "r"
	E []string         "e"
	A AnswerType       "a"
	// Unsupported mainline extension for client identification.
	// V string(?)	"v"
}

// sendMsg bencodes the data in 'query' and sends it to the remote node.
func SendMsg(conn *net.UDPConn, raddr net.UDPAddr, query interface{}, log logger.DebugLogger) {
	TotalSent.Add(1)
	var b bytes.Buffer
	if err := bencode.Marshal(&b, query); err != nil {
		return
	}
	if n, err := conn.WriteToUDP(b.Bytes(), &raddr); err != nil {
		log.Debugf("DHT: node write failed to %+v, error=%s", raddr, err)
	} else {
		TotalWrittenBytes.Add(int64(n))
	}
}

// Read responses from bencode-speaking nodes. Return the appropriate data structure.
func ReadResponse(p PacketType, log logger.DebugLogger) (response ResponseType, err error) {
	// The calls to bencode.Unmarshal() can be fragile.
	defer func() {
		if x := recover(); x != nil {
			log.Debugf("DHT: !!! Recovering from panic() after bencode.Unmarshal %q, %v", string(p.B), x)
		}
	}()
	if e2 := bencode.Unmarshal(bytes.NewBuffer(p.B), &response); e2 == nil {
		err = nil
		return
	} else {
		log.Debugf("DHT: unmarshal error, odd or partial data during UDP read? %v, err=%s", string(p.B), e2)
		return response, e2
	}
}

// Message to be sent out in the wire. Must not have any extra fields.
type QueryMessage struct {
	T string                 "t"
	Y string                 "y"
	Q string                 "q"
	A map[string]interface{} "a"
}

type ReplyMessage struct {
	T string                 "t"
	Y string                 "y"
	R map[string]interface{} "r"
}

type PacketType struct {
	B     []byte
	Raddr net.UDPAddr
}

func Listen(addr string, listenPort int, proto string, log logger.DebugLogger) (socket *net.UDPConn, err error) {
	log.Debugf("DHT: Listening for peers on IP: %s port: %d Protocol=%s\n", addr, listenPort, proto)
	listener, err := net.ListenPacket(proto, addr+":"+strconv.Itoa(listenPort))
	if err != nil {
		log.Debugf("DHT: Listen failed:%s\n", err)
	}
	if listener != nil {
		socket = listener.(*net.UDPConn)
	}
	return
}

// Read from UDP socket, writes slice of byte into channel.
func ReadFromSocket(socket *net.UDPConn, conChan chan PacketType, bytesArena arena.Arena, stop chan bool, log logger.DebugLogger) {
	for {
		b := bytesArena.Pop()
		n, addr, err := socket.ReadFromUDP(b)
		if err != nil {
			log.Debugf("DHT: readResponse error:%s\n", err)
		}
		b = b[0:n]
		if n == MaxUDPPacketSize {
			log.Debugf("DHT: Warning. Received packet with len >= %d, some data may have been discarded.\n", MaxUDPPacketSize)
		}
		TotalReadBytes.Add(int64(n))
		if n > 0 && err == nil {
			p := PacketType{b, *addr}
			select {
			case conChan <- p:
				continue
			case <-stop:
				return
			}
		}
		// Do a non-blocking read of the stop channel and stop this goroutine if the channel
		// has been closed.
		select {
		case <-stop:
			return
		default:
		}
	}
}

func BogusId(id string) bool {
	return len(id) != 20
}

func NewTransactionId() int {
	n, err := rand.Read(make([]byte, 1))
	if err != nil {
		return time.Now().Second()
	}
	return n
}
