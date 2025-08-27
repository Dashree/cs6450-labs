//go:build ignore
// +build ignore

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

type KVService struct {
	sync.RWMutex            // ← RW mutex instead of sync.Mutex
	mp        map[string]string
	stats     Stats         // counters updated with atomics
	prevStats Stats         // previous snapshot (for print)
	lastPrint time.Time
}

func NewKVService() *KVService {
	return &KVService{
		mp:        make(map[string]string),
		lastPrint: time.Now(),
	}
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	atomic.AddUint64(&kv.stats.gets, 1)

	kv.RLock() // read lock: allow many concurrent readers
	v, ok := kv.mp[request.Key]
	kv.RUnlock()

	if ok {
		response.Value = v
	}
	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	kv.Lock() // write lock: exclusive
	kv.mp[request.Key] = request.Value
	kv.Unlock()

	atomic.AddUint64(&kv.stats.puts, 1)
	return nil
}

func (kv *KVService) printStats() {
	// Take an atomic snapshot of counters, and update timing atomically under a short write lock
	snap := Stats{
		puts: atomic.LoadUint64(&kv.stats.puts),
		gets: atomic.LoadUint64(&kv.stats.gets),
	}

	kv.Lock()
	prev := kv.prevStats
	kv.prevStats = snap
	now := time.Now()
	last := kv.lastPrint
	kv.lastPrint = now
	kv.Unlock()

	diff := snap.Sub(&prev)
	deltaS := now.Sub(last).Seconds()

	fmt.Printf("get/s %0.2f\nput/s %0.2f\nops/s %0.2f\n\n",
		float64(diff.gets)/deltaS,
		float64(diff.puts)/deltaS,
		float64(diff.gets+diff.puts)/deltaS)
}

func main() {
	port := flag.String("port", "8080", "Port to run the server on")
	flag.Parse()

	kvsrv := NewKVService()
	rpc.Register(kvsrv)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", fmt.Sprintf(":%v", *port))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	fmt.Printf("Starting KVS server on :%s\n", *port)

	go func() {
		for {
			kvsrv.printStats()
			time.Sleep(1 * time.Second)
		}
	}()

	http.Serve(l, nil)
}
