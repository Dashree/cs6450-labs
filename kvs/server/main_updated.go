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

// entry holds the value for a single key and its own lock.
// We never delete entries in this lab, so it's safe to keep pointers stable.
type entry struct {
	mu sync.RWMutex
	v  string
}

// shard now protects only the *map structure* with mu.
// Each key has its own entry (and lock) to avoid contending on the whole shard.
type shard struct {
	mu sync.RWMutex            // protects m (map structure only: insert of new keys)
	m  map[string]*entry       // key -> entry (per-key lock inside entry)
}

type KVService struct {
	shards    []shard
	stats     Stats
	prevStats Stats
	lastPrint time.Time
}

func NewKVService(shardCount int) *KVService {
	if shardCount <= 0 {
		shardCount = 16
	}
	ss := make([]shard, shardCount)
	for i := range ss {
		ss[i].m = make(map[string]*entry)
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
	n := uint64(len(kv.shards))
	idx := int(fnv64a(key) % n)
	return &kv.shards[idx]
}

// ---- RPC: Get ----
// Fast path: RLock shard to safely read map pointer, then RLock the per-key entry.
func (kv *KVService) Get(req *kvs.GetRequest, resp *kvs.GetResponse) error {
	atomic.AddUint64(&kv.stats.gets, 1)

	s := kv.shardFor(req.Key)

	// Lookup pointer under shard read lock (safe against concurrent inserts).
	s.mu.RLock()
	e, ok := s.m[req.Key]
	s.mu.RUnlock()

	if !ok {
		resp.Value = ""
		resp.Found = false
		return nil
	}

	// Read value under per-key lock.
	e.mu.RLock()
	v := e.v
	e.mu.RUnlock()

	resp.Value = v
	resp.Found = true
	return nil
}

// ---- RPC: Put ----
// Double-checked pattern:
// 1) Try read lookup under shard RLock.
// 2) If absent, Lock shard to create/find entry (avoids duplicate entries).
// 3) Lock only the key's entry to write value.
func (kv *KVService) Put(req *kvs.PutRequest, resp *kvs.PutResponse) error {
	s := kv.shardFor(req.Key)

	// 1) Try fast lookup
	s.mu.RLock()
	e, ok := s.m[req.Key]
	s.mu.RUnlock()

	// 2) Create entry if missing
	if !ok {
		s.mu.Lock()
		// Check again in case another goroutine inserted it.
		if e, ok = s.m[req.Key]; !ok {
			e = &entry{}
			s.m[req.Key] = e
		}
		s.mu.Unlock()
	}

	// 3) Write under per-key lock
	e.mu.Lock()
	e.v = req.Value
	e.mu.Unlock()

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
