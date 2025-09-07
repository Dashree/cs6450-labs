package main

import (
	"flag"
	"fmt"
	"hash"
	"hash/fnv"
	"log"
	"net/rpc"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"encoding/binary"

	"github.com/rstutsman/cs6450-labs/kvs"
)

var reqBatchsize uint32
var numHosts uint64
var workloadsPerHost uint32

type KeyHash struct {
	shardIdx int
	keyHash  uint64
}


var hasherPool = sync.Pool{
	New: func() any {
		return fnv.New64a()
	},
}

// cache keyed by the raw uint64 hash input (not string)
type CacheKeyHasher struct {
	cache sync.Map // map[uint64]*KeyHash
}

func newCache() *CacheKeyHasher {
	return &CacheKeyHasher{}
}

// getShardIndexCached expects the original numeric key (uint64).
// It hashes the numeric key bytes (no fmt.Sprintf allocations).
func (c *CacheKeyHasher) getShardIndex(key uint64) *KeyHash {
	// Try fast path: compute a small key for cache lookup (we can use the numeric key itself)
	// If workload.Key is already a unique uint64 ID we can use it as the cache key.
	if v, ok := c.cache.Load(key); ok {
		return v.(*KeyHash)
	}

	// Compute hash using pooled hasher
	h := hasherPool.Get().(hash.Hash64)
	h.Reset()
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], key)
	_, _ = h.Write(buf[:]) // always succeeds
	khash := h.Sum64()
	hasherPool.Put(h)

	idx := int(khash % uint64(numHosts))
	kHash := &KeyHash{shardIdx: idx, keyHash: khash}

	// Use LoadOrStore to avoid races/stamps
	if actual, loaded := c.cache.LoadOrStore(key, kHash); loaded {
		// Another goroutine stored it first — use that one
		return actual.(*KeyHash)
	}
	return kHash
}

var keyHasherCache = newCache()

func getShardIndexCachedFromUint64(k uint64) *KeyHash {
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

func runClient(id int, clients []*Client, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64) {
	value := strings.Repeat("x", 128)
	const batchSize = 1024

	opsCompleted := uint64(0)
	reqBatch := make([][]uint64, numHosts)

	for !done.Load() {
		for j := 0; j < batchSize; j++ {
			op := workload.Next()
			// key := fmt.Sprintf("%d", op.Key)
			kHost := getShardIndexCachedFromUint64(op.Key)

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
		for idx := range clients {
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

	if len(hosts) == 0 {
		hosts = append(hosts, "localhost:8080")
	}

	numHosts = uint64(len(hosts))

	// create shared RPC clients once
	clients := make([]*Client, len(hosts))
	for i, addr := range hosts {
		clients[i] = Dial(addr)
	}

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
			runClient(clientId, clients, &done, workload, resultsCh)
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
