// Runs a node on a random UDP port that attempts to collect 10 peers for an
// infohash, then keeps running as a passive DHT node.
//
// IMPORTANT: if the UDP port is not reachable from the public internet, you
// may see very few results.
//
// To collect 10 peers, it usually has to contact some 1k nodes. It's much easier
// to find peers for popular infohashes. This process is not instant and should
// take a minute or two, depending on your network connection.
//
// There is a builtin web server that can be used to collect debugging stats
// from http://localhost:8711/debug/vars.
package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"dht"
	"dht/util"
)

const (
	httpPortTCP = 8711
	numTarget   = 10
	exampleIH   = "99c82bb73505a3c0b453f9fa0e881d6e5a32a0c1"
)

func main() {
	flag.Parse()
	// To see logs, use the -logtostderr flag and change the verbosity with
	// -v 0 (less verbose) up to -v 5 (more verbose).
	if len(flag.Args()) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: %v <infohash>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example infohash: %v\n", exampleIH)
		flag.PrintDefaults()
		os.Exit(1)
	}
	ih, err := util.DecodeInfoHash(flag.Args()[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "DecodeInfoHash error: %v\n", err)
		os.Exit(1)
	}
	// Starts a DHT node with the default options. It picks a random UDP port. To change this, see dht.NewConfig.
	d, err := dht.New(nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "New DHT error: %v", err)
		os.Exit(1)

	}
	// For debugging.
	go http.ListenAndServe(fmt.Sprintf(":%d", httpPortTCP), nil)

	if err = d.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "DHT start error: %v", err)
		os.Exit(1)
	}
	go drainresults(d)

	for {
		d.PeersRequest(string(ih), false)
		time.Sleep(5 * time.Second)
	}
}

// drainresults loops, printing the address of nodes it has found.
func drainresults(n *dht.DHT) {
	count := 0
	fmt.Println("=========================== DHT")
	fmt.Println("Note that there are many bad nodes that reply to anything you ask.")
	fmt.Println("Peers found:")
	for r := range n.PeersRequestResults {
		for _, peers := range r {
			for _, x := range peers {
				fmt.Printf("%d: %v\n", count, util.DecodePeerAddress(x))
				count++
				if count >= numTarget {
					os.Exit(0)
				}
			}
		}
	}
}
