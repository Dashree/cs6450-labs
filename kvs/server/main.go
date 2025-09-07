package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

var numShards uint64
var enableCache bool

type Stats struct {
	puts uint64
	gets uint64
}

type Shard struct {
	mu   sync.RWMutex
	mp   map[uint64]string
}

type ShardMap struct {
	shards []*Shard
}

func getLocalShardIndex(key uint64) uint64 { return key % numShards }

func NewShardedMap(shardCount uint64, mapAllocCount uint64) *ShardMap {
	m := &ShardMap{shards: make([]*Shard, shardCount)}
	for i := uint64(0); i < shardCount; i++ {
		m.shards[i] = &Shard{
			mp: make(map[uint64]string, mapAllocCount),
		}
	}
	return m
}

type KVService struct {
	shardmp *ShardMap
	stats   Stats
}

func NewKVService(shardCount uint64, mapAllocCount uint64) *KVService {
	return &KVService{
		shardmp: NewShardedMap(shardCount, mapAllocCount),
	}
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	atomic.AddUint64(&kv.stats.gets, uint64(len(request.Key)))

	resBatch := make([]string, len(request.Key))
	for i, key := range request.Key {
		sh := kv.shardmp.shards[getLocalShardIndex(key)]
		sh.mu.RLock()
		val, found := sh.mp[key] // store actual key
		sh.mu.RUnlock()
		if found {
			resBatch[i] = val
		} else {
			resBatch[i] = ""
		}
	}
	response.Value = resBatch
	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	atomic.AddUint64(&kv.stats.puts, 1)
	sh := kv.shardmp.shards[getLocalShardIndex(request.Key)]
	sh.mu.Lock()
	sh.mp[request.Key] = request.Value
	sh.mu.Unlock()
	return nil
}

func (kv *KVService) printStats() {
	gets := atomic.LoadUint64(&kv.stats.gets)
	puts := atomic.LoadUint64(&kv.stats.puts)
	fmt.Printf("get/s %0.2f | put/s %0.2f | ops/s %0.2f\n",
		float64(gets), float64(puts), float64(gets+puts))
}

func main() {
	port := flag.String("port", "8080", "Port to run the server on")
	numShards = *flag.Uint64("num-shards", 64, "Number of Shards in the KVStore")
	mapAllocCount := *flag.Uint64("alloc", 400_000, "Expected keys per shard")
	enableCache = *flag.Bool("cache", false, "Use cached values for string storage")
	flag.Parse()

	kvs := NewKVService(numShards, mapAllocCount)
	rpc.Register(kvs)
	rpc.HandleHTTP()

	l, err := net.Listen("tcp", fmt.Sprintf(":%v", *port))
	if err != nil {
		log.Fatal("listen error:", err)
	}

	fmt.Printf("Starting KVS server on :%s | cache=%t\n", *port, enableCache)

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			kvs.printStats()
		}
	}()

	log.Fatal(http.Serve(l, nil))
}
