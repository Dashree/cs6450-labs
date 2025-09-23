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

const numShards = 64

// ---------------- Stats ----------------

type Stats struct {
	gets        uint64
	puts        uint64
	commitsLead uint64
	abortsLead  uint64
}

func (s *Stats) Sub(prev *Stats) Stats {
	return Stats{
		gets:        s.gets - prev.gets,
		puts:        s.puts - prev.puts,
		commitsLead: s.commitsLead - prev.commitsLead,
		abortsLead:  s.abortsLead - prev.abortsLead,
	}
}

// ---------------- Data + Locks ----------------

type keyLock struct {
	readers map[string]struct{} // txids holding S lock
	writer  string              // txid holding X lock (if any)
}

type Shard struct {
	mu    sync.Mutex
	mp    map[string]string   // committed K/V
	locks map[string]*keyLock // per-key locks
}

type ShardMap struct {
	shards [numShards]*Shard
}

func NewShardedMap() *ShardMap {
	m := &ShardMap{}
	for i := 0; i < numShards; i++ {
		m.shards[i] = &Shard{
			mp:    make(map[string]string, 1_000_000),
			locks: make(map[string]*keyLock),
		}
	}
	return m
}

func getShardIndex(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32() % numShards
}

func (s *Shard) lk(key string) *keyLock {
	l, ok := s.locks[key]
	if !ok {
		l = &keyLock{readers: make(map[string]struct{})}
		s.locks[key] = l
	}
	return l
}

// ---------------- Txn state (per server) ----------------

type Tx struct {
	id         string
	writeSet   map[string]string // key -> value (to apply on commit)
	readLocks  map[string]bool   // keys held with S lock
	writeLocks map[string]bool   // keys held with X lock
}

type KVService struct {
	shards *ShardMap

	// tx table
	muTx sync.Mutex
	txs  map[string]*Tx

	// stats
	muStats sync.Mutex
	stats   Stats
	prev    Stats
	last    time.Time
}

func NewKVService() *KVService {
	return &KVService{
		shards: NewShardedMap(),
		txs:    make(map[string]*Tx),
		last:   time.Now(),
	}
}

func (kv *KVService) ensureTx(txid string) *Tx {
	kv.muTx.Lock()
	defer kv.muTx.Unlock()
	if tx, ok := kv.txs[txid]; ok {
		return tx
	}
	tx := &Tx{
		id:         txid,
		writeSet:   make(map[string]string),
		readLocks:  make(map[string]bool),
		writeLocks: make(map[string]bool),
	}
	kv.txs[txid] = tx
	return tx
}

func (kv *KVService) releaseLocks(tx *Tx) {
	// Release S locks
	for key := range tx.readLocks {
		idx := getShardIndex(key)
		sh := kv.shards.shards[idx]
		sh.mu.Lock()
		l := sh.lk(key)
		delete(l.readers, tx.id)
		sh.mu.Unlock()
	}
	// Release X locks
	for key := range tx.writeLocks {
		idx := getShardIndex(key)
		sh := kv.shards.shards[idx]
		sh.mu.Lock()
		l := sh.lk(key)
		if l.writer == tx.id {
			l.writer = ""
		}
		sh.mu.Unlock()
	}
}

func (kv *KVService) deleteTx(txid string) {
	kv.muTx.Lock()
	delete(kv.txs, txid)
	kv.muTx.Unlock()
}

// ---------------- RPC: Get / Put ----------------

func (kv *KVService) Get(req *kvs.GetRequest, resp *kvs.GetResponse) error {
	tx := kv.ensureTx(req.Txid)

	idx := getShardIndex(req.Key)
	sh := kv.shards.shards[idx]

	sh.mu.Lock()
	defer sh.mu.Unlock()

	l := sh.lk(req.Key)

	// no-wait S-lock
	if l.writer != "" && l.writer != req.Txid {
		resp.Granted = false
		return nil
	}
	// grant S-lock
	l.readers[req.Txid] = struct{}{}
	tx.readLocks[req.Key] = true

	// read-your-writes: check tx writeSet first
	if v, ok := tx.writeSet[req.Key]; ok {
		resp.Value = v
	} else {
		resp.Value = sh.mp[req.Key]
	}
	resp.Granted = true

	kv.muStats.Lock()
	kv.stats.gets++
	kv.muStats.Unlock()
	return nil
}

func (kv *KVService) Put(req *kvs.PutRequest, resp *kvs.PutResponse) error {
	tx := kv.ensureTx(req.Txid)

	idx := getShardIndex(req.Key)
	sh := kv.shards.shards[idx]

	sh.mu.Lock()
	defer sh.mu.Unlock()

	l := sh.lk(req.Key)

	// no-wait X-lock
	if l.writer == req.Txid {
		// already have X, fine
	} else if l.writer != "" && l.writer != req.Txid {
		resp.Granted = false
		return nil
	} else if len(l.readers) > 0 {
		// if readers exist and any reader is not me -> deny
		if _, onlyMe := l.readers[req.Txid]; !onlyMe || len(l.readers) > 1 {
			resp.Granted = false
			return nil
		}
		// upgrade S->X: remove my S
		delete(l.readers, req.Txid)
		l.writer = req.Txid
	} else {
		// free: take X
		l.writer = req.Txid
	}

	tx.writeSet[req.Key] = req.Value
	tx.writeLocks[req.Key] = true
	delete(tx.readLocks, req.Key) // upgraded if present

	resp.Granted = true

	kv.muStats.Lock()
	kv.stats.puts++
	kv.muStats.Unlock()
	return nil
}

// ---------------- RPC: Commit / Abort ----------------

func (kv *KVService) Commit(req *kvs.CommitRequest, _ *kvs.CommitResponse) error {
	kv.muTx.Lock()
	tx, ok := kv.txs[req.Txid]
	kv.muTx.Unlock()
	if !ok {
		// nothing to do; idempotent
		return nil
	}

	// apply writes
	for key, val := range tx.writeSet {
		idx := getShardIndex(key)
		sh := kv.shards.shards[idx]
		sh.mu.Lock()
		sh.mp[key] = val
		sh.mu.Unlock()
	}

	// release locks and clean
	kv.releaseLocks(tx)
	kv.deleteTx(req.Txid)

	if req.Lead {
		kv.muStats.Lock()
		kv.stats.commitsLead++
		kv.muStats.Unlock()
	}
	return nil
}

func (kv *KVService) Abort(req *kvs.AbortRequest, _ *kvs.AbortResponse) error {
	kv.muTx.Lock()
	tx, ok := kv.txs[req.Txid]
	kv.muTx.Unlock()
	if ok {
		kv.releaseLocks(tx)
		kv.deleteTx(req.Txid)
	}
	if req.Lead {
		kv.muStats.Lock()
		kv.stats.abortsLead++
		kv.muStats.Unlock()
	}
	return nil
}

// ---------------- Stats printer ----------------

func (kv *KVService) printStats() {
    kv.muStats.Lock()
    stats := kv.stats
    prev := kv.prev
    kv.prev = stats
    now := time.Now()
    last := kv.last
    kv.last = now
    kv.muStats.Unlock()

    diff := stats.Sub(&prev)
    secs := now.Sub(last).Seconds()

    getRate    := float64(diff.gets)        / secs
    putRate    := float64(diff.puts)        / secs
    opRate     := float64(diff.gets+diff.puts) / secs
    commitRate := float64(diff.commitsLead) / secs
    abortRate  := float64(diff.abortsLead)  / secs

    // IMPORTANT: include "ops/s" so the report script can find it
    fmt.Printf("get/s %.2f put/s %.2f ops/s %.2f commit/s %.2f abort/s %.2f\n",
        getRate, putRate, opRate, commitRate, abortRate)
}


func main() {
	port := flag.String("port", "8080", "Port to run the server on")
	flag.Parse()

	svc := NewKVService()
	_ = rpc.Register(svc)
	rpc.HandleHTTP()

	l, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
	if err != nil {
		log.Fatal("listen error:", err)
	}
	fmt.Printf("Starting KVS server on :%s\n", *port)

	go func() {
		for {
			svc.printStats()
			time.Sleep(1 * time.Second)
		}
	}()

	log.Fatal(http.Serve(l, nil))
}
