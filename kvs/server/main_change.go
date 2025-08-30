// cmd/kvsserver/main.go
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

type KVService struct {
	mu   sync.RWMutex
	data map[string]string

	ops uint64 // op counter for throughput logging
}

func NewKVService() *KVService {
	return &KVService{data: make(map[string]string)}
}

func (s *KVService) Get(req *kvs.GetRequest, resp *kvs.GetResponse) error {
	s.mu.RLock()
	resp.Value = s.data[req.Key]
	s.mu.RUnlock()
	atomic.AddUint64(&s.ops, 1)
	return nil
}

func (s *KVService) Put(req *kvs.PutRequest, resp *kvs.PutResponse) error {
	s.mu.Lock()
	s.data[req.Key] = req.Value
	s.mu.Unlock()
	atomic.AddUint64(&s.ops, 1)
	return nil
}

func (s *KVService) Batch(req *kvs.BatchRequest, resp *kvs.BatchResponse) error {
	results := make([]kvs.BatchItem, 0, len(req.Ops))

	// Two-pass lock strategy to minimize contention:
	// small batches => simple per-op lock is fine and easiest.
	for _, op := range req.Ops {
		if op.IsRead {
			s.mu.RLock()
			val := s.data[op.Key]
			s.mu.RUnlock()
			results = append(results, kvs.BatchItem{Key: op.Key, Value: val})
		} else {
			s.mu.Lock()
			s.data[op.Key] = op.Value
			s.mu.Unlock()
			results = append(results, kvs.BatchItem{Key: op.Key})
		}
	}
	resp.Results = results
	atomic.AddUint64(&s.ops, uint64(len(req.Ops)))
	return nil
}

func main() {
	port := flag.Int("port", 8080, "listen port")
	flag.Parse()

	svc := NewKVService()
	if err := rpc.RegisterName("KVService", svc); err != nil {
		log.Fatal(err)
	}
	rpc.HandleHTTP()

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("listen :%d: %v", *port, err)
	}

	// Periodic server-side throughput log (picked up by report-tput.py)
	go func() {
		tick := time.NewTicker(1 * time.Second)
		defer tick.Stop()
		var last uint64
		for range tick.C {
			cur := atomic.LoadUint64(&svc.ops)
			ops := cur - last
			last = cur
			// keep the token "ops/s" in the line so your script can parse it
			log.Printf("ops/s %d", ops)
		}
	}()

	log.Printf("kvsserver listening on :%d", *port)
	log.Fatal(http.Serve(l, nil))
}
