//go:build ignore
// +build ignore

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
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type Stats struct {
	puts uint64
	gets uint64
}

func (s *Stats) Sub(prev *Stats) Stats {
	return Stats{
		puts: s.puts - prev.puts,
		gets: s.gets - prev.gets,
	}
}

type shard struct {
	mu sync.RWMutex
	m  map[string]string
}

type KVService struct {
	shards    []shard
	stats     Stats
	prevStats Stats
	lastPrint time.Time
}

func NewKVService(shardCount int) *KVService {
	if shardCount <= 0 {
		// shardCount = 64
		shardCount = 64
	}
	ss := make([]shard, shardCount)
	for i := range ss {
		ss[i].m = make(map[string]string)
	}
	return &KVService{
		shards:    ss,
		lastPrint: time.Now(),
	}
}

func fnv64a(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

func (kv *KVService) shardFor(key string) *shard {
	idx := int(fnv64a(key) % uint64(len(kv.shards)))
	// faster
	// idx := int(fnv64a(key) & uint64(len(kv.shards)-1))
	return &kv.shards[idx]
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	atomic.AddUint64(&kv.stats.gets, 1)

	s := kv.shardFor(request.Key)
	s.mu.RLock()
	v, ok := s.m[request.Key]
	s.mu.RUnlock()

	if ok {
		response.Value = v
	}
	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	s := kv.shardFor(request.Key)
	s.mu.Lock()
	s.m[request.Key] = request.Value
	s.mu.Unlock()

	atomic.AddUint64(&kv.stats.puts, 1)
	return nil
}

func (kv *KVService) printStats() {
	snap := Stats{
		puts: atomic.LoadUint64(&kv.stats.puts),
		gets: atomic.LoadUint64(&kv.stats.gets),
	}
	prev := kv.prevStats
	kv.prevStats = snap
	now := time.Now()
	deltaS := now.Sub(kv.lastPrint).Seconds()
	kv.lastPrint = now

	diff := snap.Sub(&prev)

	fmt.Printf("get/s %0.2f\nput/s %0.2f\nops/s %0.2f\n\n",
		float64(diff.gets)/deltaS,
		float64(diff.puts)/deltaS,
		float64(diff.gets+diff.puts)/deltaS)
}

func main() {
	port := flag.String("port", "8080", "Port to run the server on")
	shards := flag.Int("shards", 64, "Number of in-process shards per server")
	flag.Parse()

	kvsrv := NewKVService(*shards)
	_ = rpc.Register(kvsrv)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", fmt.Sprintf(":%v", *port))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	fmt.Printf("Starting KVS server on :%s with %d shards\n", *port, *shards)

	go func() {
		for {
			kvsrv.printStats()
			time.Sleep(1 * time.Second)
		}
	}()

	_ = http.Serve(l, nil)
}
