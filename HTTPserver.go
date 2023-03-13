package dht

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

// para: UPD host&port and TCP host&port
// receive from tcp, then send to node's udp port?
// or just insert to dht's routing table
func (d *DHT) StartHTTPServer(host, port string) {
	serviceAddr := fmt.Sprintf("%s:%s", host, port)
	// register router
	http.Handle("/update", d)
	var srv http.Server
	srv.Addr = serviceAddr
	log.Println(srv.Addr)
	log.Println(srv.ListenAndServe())
}

func (d *DHT) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Println("Request received")
	switch r.Method {
	case http.MethodGet:
		w.Header().Add("Content-Type", "application/json")
		// TODO
	case http.MethodPost:
		dec := json.NewDecoder(r.Body)
		var r Registration
		err := dec.Decode(&r)
		if err != nil {
			d.DebugLogger.Errorf("error parsing add node post:%v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		log.Println(r)
		err = d.ADDHonestPeer(r.Nodeid, r.NodeAddr)
		if err != nil {
			d.DebugLogger.Errorf("error parsing add node post:%v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	// TODO
	// case http.MethodDelete:
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
}
