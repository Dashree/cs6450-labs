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
		shardCount = 512
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

// func fnv32a(s string) uint32 {
// 	h := fnv.New32a()
// 	_, _ = h.Write([]byte(s))
// 	return h.Sum32()
// }

func (kv *KVService) shardFor(key string) *shard {
	n := uint64(len(kv.shards))
	idx := int(fnv64a(key) % n)
	return &kv.shards[idx]
	// n := len(kv.shards)
	// idx := int(fnv32a(key) % uint32(n))
	// return &kv.shards[idx]
}

// ---- RPC: Get ----
func (kv *KVService) Get(req *kvs.GetRequest, resp *kvs.GetResponse) error {
	atomic.AddUint64(&kv.stats.gets, 1)

	s := kv.shardFor(req.Key)
	s.mu.RLock()
	v, ok := s.m[req.Key]
	s.mu.RUnlock()

	if ok {
		resp.Value = v
		resp.Found = true
	} else {
		resp.Value = ""
		resp.Found = false
	}
	return nil
}

// ---- RPC: Put ----
func (kv *KVService) Put(req *kvs.PutRequest, resp *kvs.PutResponse) error {
	s := kv.shardFor(req.Key)
	s.mu.Lock()
	s.m[req.Key] = req.Value
	s.mu.Unlock()

	atomic.AddUint64(&kv.stats.puts, 1)
	return nil
}

// ---- RPC: Batch (calls Get/Put directly) ----
func (kv *KVService) Batch(req *kvs.BatchRequest, resp *kvs.BatchResponse) error {
	resp.Results = make([]kvs.BatchItem, len(req.Ops))
	for i, op := range req.Ops {
		if op.IsRead {
			var gr kvs.GetResponse
			_ = kv.Get(&kvs.GetRequest{Key: op.Key}, &gr)
			resp.Results[i] = kvs.BatchItem{Found: gr.Found, Value: gr.Value}
		} else {
			var pr kvs.PutResponse
			_ = kv.Put(&kvs.PutRequest{Key: op.Key, Value: op.Value}, &pr)
			resp.Results[i] = kvs.BatchItem{Found: false}
		}
	}
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
	// shards := flag.Int("shards", 64, "Number of in-process shards per server")
	shards := flag.Int("shards", 32, "Number of in-process shards per server")
	flag.Parse()

	kvsrv := NewKVService(*shards)
	if err := rpc.Register(kvsrv); err != nil {
		log.Fatal(err)
	}
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
