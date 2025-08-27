package main

import (
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type Client struct {
	rpcClient *rpc.Client
}

func Dial(addr string) *Client {
	rc, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	return &Client{rc}
}

func (c *Client) Get(key string) string {
	req := kvs.GetRequest{Key: key}
	var resp kvs.GetResponse
	if err := c.rpcClient.Call("KVService.Get", &req, &resp); err != nil {
		log.Fatal(err)
	}
	return resp.Value
}

func (c *Client) Put(key, value string) {
	req := kvs.PutRequest{Key: key, Value: value}
	var resp kvs.PutResponse
	if err := c.rpcClient.Call("KVService.Put", &req, &resp); err != nil {
		log.Fatal(err)
	}
}

func fnv64a(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

func route(clients []*Client, key string) *Client {
	idx := int(fnv64a(key) % uint64(len(clients)))
	return clients[idx]
}

func runClient(id int, clients []*Client, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64) {
	value := strings.Repeat("x", 128)
	const batchSize = 1024

	var opsCompleted uint64

	for !done.Load() {
		for j := 0; j < batchSize; j++ {
			op := workload.Next()
			key := fmt.Sprintf("%d", op.Key)
			c := route(clients, key)
			if op.IsRead {
				_ = c.Get(key)
			} else {
				c.Put(key, value)
			}
			opsCompleted++
		}
	}

	fmt.Printf("Client %d finished operations.\n", id)
	resultsCh <- opsCompleted
}

type HostList []string

func (h *HostList) String() string { return strings.Join(*h, ",") }
func (h *HostList) Set(value string) error {
	*h = strings.Split(value, ",")
	return nil
}

func main() {
	var hosts HostList
	theta := flag.Float64("theta", 0.99, "Zipfian distribution skew parameter")
	workloadName := flag.String("workload", "YCSB-B", "Workload type (YCSB-A, YCSB-B, YCSB-C)")
	secs := flag.Int("secs", 30, "Duration seconds")
	flag.Var(&hosts, "hosts", "Comma-separated host:port list (all used for sharding)")
	flag.Parse()

	if len(hosts) == 0 {
		hosts = append(hosts, "localhost:8080")
	}

	// Build one RPC client per server host
	var clients []*Client
	for _, h := range hosts {
		clients = append(clients, Dial(h))
	}

	fmt.Printf("hosts %v\ntheta %.2f\nworkload %s\nsecs %d\n",
		hosts, *theta, *workloadName, *secs)

	start := time.Now()
	done := atomic.Bool{}
	resultsCh := make(chan uint64)

	// Single worker (as before); you can spawn more if you want
	go func(clientId int) {
		wl := kvs.NewWorkload(*workloadName, *theta)
		runClient(clientId, clients, &done, wl, resultsCh)
	}(0)

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)

	opsCompleted := <-resultsCh
	elapsed := time.Since(start)
	opsPerSec := float64(opsCompleted) / elapsed.Seconds()
	fmt.Printf("throughput %.2f ops/s\n", opsPerSec)
}
