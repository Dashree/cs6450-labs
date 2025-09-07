package main

import (
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

var reqBatchsize uint32
var numHosts uint64
var workloadsPerHost uint32

type KeyHash struct {
	shardIdx int
	keyHash  uint64
}
type CacheKeyHasher struct {
	muCache sync.RWMutex // lock for concurrent access
	cache   map[string]*KeyHash
}

func newCache() *CacheKeyHasher {
	return &CacheKeyHasher{
		cache: make(map[string]*KeyHash, 100_000),
	}
}

func (c *CacheKeyHasher) getShardIndex(k string) *KeyHash {
	c.muCache.RLock()
	kHash, found := c.cache[k]
	c.muCache.RUnlock()
	if found {
		return kHash
	} else {
		h := fnv.New64a()
		h.Write([]byte(k))
		khashVal := h.Sum64()
		idx := int(khashVal % uint64(numHosts))
		kHash := &KeyHash{shardIdx: idx, keyHash: khashVal}
		c.muCache.Lock()
		c.cache[k] = kHash
		c.muCache.Unlock()
		return kHash
	}

}

var keyHasherCache = newCache()

func getShardIndexCached(k string) *KeyHash {
	return keyHasherCache.getShardIndex(k)
}

type Client struct {
	rpcClient *rpc.Client
}

func Dial(addr string) *Client {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	return &Client{rpcClient}
}

func (client *Client) Get(key []uint64) []string {
	request := kvs.GetRequest{
		Key: key,
	}
	response := kvs.GetResponse{}
	err := client.rpcClient.Call("KVService.Get", &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	return response.Value
}

func (client *Client) Put(key uint64, value string) {
	request := kvs.PutRequest{
		Key:   key,
		Value: value,
	}
	response := kvs.PutResponse{}
	err := client.rpcClient.Call("KVService.Put", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
}

func runClient(id int, addrs []string, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64) {
	clients := []*Client{}
	for _, addr := range addrs {
		clients = append(clients, Dial(addr))
	}

	value := strings.Repeat("x", 128)
	const batchSize = 1024

	opsCompleted := uint64(0)
	reqBatch := make([][]uint64, numHosts)

	for !done.Load() {
		for j := 0; j < batchSize; j++ {
			op := workload.Next()
			key := fmt.Sprintf("%d", op.Key)
			kHost := getShardIndexCached(key)

			if op.IsRead {
				reqBatch[kHost.shardIdx] = append(reqBatch[kHost.shardIdx], kHost.keyHash)

				if len(reqBatch[kHost.shardIdx]) >= int(reqBatchsize) {
					clients[kHost.shardIdx].Get(reqBatch[kHost.shardIdx])
					reqBatch[kHost.shardIdx] = nil
				}

			} else {
				if len(reqBatch[kHost.shardIdx]) > 0 {
					clients[kHost.shardIdx].Get(reqBatch[kHost.shardIdx])
					reqBatch[kHost.shardIdx] = nil
				}
				clients[kHost.shardIdx].Put(kHost.keyHash, value)
			}
			opsCompleted++
		}
		for idx := range addrs {
			clients[idx].Get(reqBatch[idx])
			reqBatch[idx] = nil
		}
	}
	resultsCh <- opsCompleted

}

type HostList []string

func (h *HostList) String() string {
	return strings.Join(*h, ",")
}

func (h *HostList) Set(value string) error {
	*h = strings.Split(value, ",")
	return nil
}

func main() {
	hosts := HostList{}

	flag.Var(&hosts, "hosts", "Comma-separated list of host:ports to connect to")
	theta := flag.Float64("theta", 0.99, "Zipfian distribution skew parameter")
	workload := flag.String("workload", "YCSB-B", "Workload type (YCSB-A, YCSB-B, YCSB-C)")
	secs := flag.Int("secs", 30, "Duration in seconds for each client to run")
	clientID := flag.Int("clientid", -1, "Relative client ID starting at 0")
	reqBatchsize = uint32(*flag.Uint64("batch-size", 8, "Batch for Get Requests"))
	workloadsPerHost = uint32(*flag.Uint64("thrds-per-host", 8, "Number of go routines per hosts"))

	flag.Parse()

	if numHosts == 0 {
		hosts = append(hosts, "localhost:8080")
	}

	numHosts = uint64(len(hosts))
	fmt.Println(numHosts)

	fmt.Printf(
		"hosts %v\n"+
			"theta %.2f\n"+
			"workload %s\n"+
			"secs %d\n",
		hosts, *theta, *workload, *secs,
	)

	start := time.Now()

	done := atomic.Bool{}
	resultsCh := make(chan uint64)
	tltOpsCompleted := uint64(0)

	var numberOfClientsPerHost = runtime.NumCPU() * int(workloadsPerHost)
	for j := 0; j < numberOfClientsPerHost; j++ {
		go func(clientId int) {
			workload := kvs.NewWorkload(*workload, *theta)
			runClient(clientId, hosts, &done, workload, resultsCh)
		}(*clientID)
	}

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)

	totalWorkloads := numberOfClientsPerHost
	for i := 0; i < totalWorkloads; i++ {
		tltOpsCompleted += <-resultsCh
	}

	elapsed := time.Since(start)

	opsPerSec := float64(tltOpsCompleted) / elapsed.Seconds()
	fmt.Printf("throughput %.2f ops/s\n", opsPerSec)
}
