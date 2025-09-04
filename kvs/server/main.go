package main

import (
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

var numShards uint64
var enableCache bool

type KeyHash struct {
	shardIdx uint64
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
		keyHash := h.Sum64()
		kHash := &KeyHash{shardIdx: uint64(keyHash % numShards), keyHash: keyHash}
		c.muCache.Lock()
		c.cache[k] = kHash
		c.muCache.Unlock()
		return kHash
	}

}

var keyHasherCache = newCache()

func getShardIndexCached(k string) *KeyHash {
	if enableCache {
		return keyHasherCache.getShardIndex(k)
	} else {
		h := fnv.New64a()
		h.Write([]byte(k))
		keyHash := h.Sum64()
		return &KeyHash{(keyHash % numShards), keyHash}

	}
}

type Stats struct {
	puts uint64
	gets uint64
}

func (s *Stats) Sub(prev *Stats) Stats {
	r := Stats{}
	r.puts = s.puts - prev.puts
	r.gets = s.gets - prev.gets
	return r
}

type Shard struct {
	muShard sync.RWMutex
	mp      map[uint64]string
}

type ShardMap struct {
	shards map[uint64]*Shard
}

// Constructor
func NewShardedMap(shardCount uint64, mapAllocCount uint64) *ShardMap {
	m := &ShardMap{shards: make(map[uint64]*Shard, shardCount)}
	for i := uint64(0); i < shardCount; i++ {
		m.shards[i] = &Shard{
			mp: make(map[uint64]string, mapAllocCount),
		}
	}
	return m
}

// func getShardIndex(key string) (uint64, uint64) {

// }

type KVService struct {
	muStatsGets sync.Mutex
	muStatsPuts sync.Mutex
	shardmp     *ShardMap
	stats       Stats
	prevStats   Stats
	lastPrint   time.Time
}

func NewKVService(shardCount uint64, mapAllocCount uint64) *KVService {
	kvs := &KVService{}
	kvs.shardmp = NewShardedMap(shardCount, mapAllocCount)
	kvs.lastPrint = time.Now()
	return kvs
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	kv.muStatsGets.Lock()
	kv.stats.gets += uint64(len(request.Key))
	kv.muStatsGets.Unlock()
	var resBatch []string

	resValue := ""
	for _, key := range request.Key {
		resValue = ""
		kHash := getShardIndexCached(key)
		sh := kv.shardmp.shards[kHash.shardIdx]
		sh.muShard.RLock()
		val, found := sh.mp[kHash.keyHash]
		sh.muShard.RUnlock()
		if found {
			resValue = val
		}
		resBatch = append(resBatch, resValue)

	}
	response.Value = resBatch

	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	kv.muStatsPuts.Lock()
	kv.stats.puts++
	kv.muStatsPuts.Unlock()

	kHash := getShardIndexCached(request.Key)
	sh := kv.shardmp.shards[kHash.shardIdx]
	sh.muShard.Lock()
	sh.mp[kHash.keyHash] = request.Value
	sh.muShard.Unlock()

	return nil
}

func (kv *KVService) printStats() {
	kv.muStatsGets.Lock()
	kv.muStatsPuts.Lock()
	stats := kv.stats
	prevStats := kv.prevStats
	kv.prevStats = stats
	now := time.Now()
	lastPrint := kv.lastPrint
	kv.lastPrint = now
	kv.muStatsPuts.Unlock()
	kv.muStatsGets.Unlock()

	diff := stats.Sub(&prevStats)
	deltaS := now.Sub(lastPrint).Seconds()

	fmt.Printf("get/s %0.2f\nput/s %0.2f\nops/s %0.2f\n\n",
		float64(diff.gets)/deltaS,
		float64(diff.puts)/deltaS,
		float64(diff.gets+diff.puts)/deltaS)
}

func main() {
	port := flag.String("port", "8080", "Port to run the server on")
	numShards = *flag.Uint64("num-shards", 64, "Number of Shards in the KVStore")
	mapAllocCount := *flag.Uint64("alloc", 400_000, "Number expected for keys per shard")
	enableCache = *flag.Bool("cache", false, "Use cached values for string storage")
	flag.Parse()

	kvs := NewKVService(numShards, mapAllocCount)
	rpc.Register(kvs)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", fmt.Sprintf(":%v", *port))
	if e != nil {
		log.Fatal("listen error:", e)
	}

	fmt.Printf("Starting KVS server on :%s %t\n", *port, enableCache)

	go func() {
		for {
			kvs.printStats()
			time.Sleep(1 * time.Second)
		}
	}()

	http.Serve(l, nil)
}
