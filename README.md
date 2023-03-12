This is a fork of golang Kademlia/Bittorrent [DHT library](https://github.com/nictuku/dht) that implements [BEP5](http://www.bittorrent.org/beps/bep_0005.html).

To improve dht's avalibility, we build each dht node a http interface to accept peer routing information. It can also cooperate with a ETCD cluster that act as an external coordinator.

The DHT performs well and supports the most important features despite its simple API.
Besides, the security of a DHT overlay network can be significantly imporved.

A full example is at:
[find_infohash_and_wait](examples/find_infohash_and_wait/main.go)

