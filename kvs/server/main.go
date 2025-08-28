package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"sync"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

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

type hashShard struct {
	sync.RWMutex
	mp map[string]string
}

type KVService struct {
	shards    [16]hashShard
	statsLock sync.Mutex
	stats     Stats
	prevStats Stats
	lastPrint time.Time
}

func NewKVService() *KVService {
	kvs := &KVService{}
	for i := range kvs.shards {
		kvs.shards[i] = hashShard{mp: make(map[string]string)}
	}
	kvs.lastPrint = time.Now()
	return kvs
}

func bucket16(s string) int {
	//AI helped write this hash function
	norm := strings.ToLower(strings.TrimSpace(s))
	sum := sha256.Sum256([]byte(norm))
	return int(sum[0] & 0x0F)
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	id := bucket16(request.Key)

	kv.shards[id].RLock()
	defer kv.shards[id].RUnlock()

	kv.stats.gets++

	if value, found := kv.shards[id].mp[request.Key]; found {
		response.Value = value
	}

	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	id := bucket16(request.Key)
	kv.shards[id].Lock()
	defer kv.shards[id].Unlock()

	kv.stats.puts++

	kv.shards[id].mp[request.Key] = request.Value

	return nil
}

func (kv *KVService) printStats() {
	kv.statsLock.Lock()
	stats := kv.stats
	prevStats := kv.prevStats
	kv.prevStats = stats
	now := time.Now()
	lastPrint := kv.lastPrint
	kv.lastPrint = now
	kv.statsLock.Unlock()

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
