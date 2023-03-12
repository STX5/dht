package dht

import (
	"fmt"
	"log"
	"net/http"
)

func (d *DHT) StartHTTPServer(host, port string) {
	serviceAddr := fmt.Sprintf("http://%s:%s", host, port)
	RegisterHandlers()
	var srv http.Server
	srv.Addr = serviceAddr
	log.Println(srv.ListenAndServe())
}

func RegisterHandlers() {
	http.Handle("/update", RegistryPeerService{})
}

// implement a http.Handler
type RegistryPeerService struct{}

func (s RegistryPeerService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Println("Request received")
	switch r.Method {
	case http.MethodGet:

	case http.MethodPost:

	case http.MethodDelete:
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
}
