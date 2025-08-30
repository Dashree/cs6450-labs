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

const numShards = 8

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

type Entry struct {
	sync.RWMutex
	value string
}

type Shard struct {
	muShard sync.RWMutex
	mp      map[string]*Entry
}

type ShardMap struct {
	shards [numShards]*Shard
}

// Constructor
func NewShardedMap() *ShardMap {
	m := &ShardMap{}
	for i := 0; i < numShards; i++ {
		m.shards[i] = &Shard{
			mp: make(map[string]*Entry),
		}
	}
	return m
}

func getShardIndex(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() % numShards
}

type KVService struct {
	muStatsGets   sync.Mutex
	muStatsPuts   sync.Mutex
	shardmp   *ShardMap
	stats     Stats
	prevStats Stats
	lastPrint time.Time
}

func NewKVService() *KVService {
	kvs := &KVService{}
	kvs.shardmp = NewShardedMap()
	kvs.lastPrint = time.Now()
	return kvs
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	kv.muStatsGets.Lock()
	kv.stats.gets++
	kv.muStatsGets.Unlock()

	idx := getShardIndex(request.Key)
	sh := kv.shardmp.shards[idx]
	entry, found := sh.mp[request.Key]
	if found {
		entry.RLock()
		response.Value = entry.value
		entry.RUnlock()
	}

	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	kv.muStatsPuts.Lock()
	kv.stats.puts++
	kv.muStatsPuts.Unlock()

	idx := getShardIndex(request.Key)
	sh := kv.shardmp.shards[idx]
	entry, found := sh.mp[request.Key]
	if found {
		entry.Lock()
		entry.value = request.Value
		entry.Unlock()
	} else {
		entry := Entry{value:request.Value}
		entry.Lock() // Not sure if we need it
		sh.mp[request.Key] = &entry
		entry.Unlock()

	}


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
	flag.Parse()

	kvs := NewKVService()
	rpc.Register(kvs)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", fmt.Sprintf(":%v", *port))
	if e != nil {
		log.Fatal("listen error:", e)
	}

	fmt.Printf("Starting KVS server on :%s\n", *port)

	go func() {
		for {
			kvs.printStats()
			time.Sleep(1 * time.Second)
		}
	}()

	http.Serve(l, nil)
}
