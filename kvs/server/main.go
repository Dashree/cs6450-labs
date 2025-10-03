package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

type TxStatus int

const (
	Pending TxStatus = iota
	Committed
	Aborted
)

type Transaction struct {
	TxID     string
	ReadSet  map[string]bool
	WriteSet map[string]string
	Status   TxStatus
}

type KeyEntry struct {
	Value   string
	Readers map[string]bool // txID -> true
	Writer  string          // txID
}


type Stats struct {
	commits uint64
	aborts  uint64
}

func (s *Stats) Sub(prev *Stats) Stats {
	r := Stats{}
	r.commits = s.commits - prev.commits
	r.aborts = s.aborts - prev.aborts
	return r
}

type transaction struct {
	id  uint32
	ops []kvs.TransactionOperation
}

type Value struct {
	value   string
	writer  *transaction
	readers map[uint32]*transaction
}

type KVService struct {
	sync.Mutex
	mp           map[string]Value
	transactions map[uint32]*transaction


	stats        Stats
	prevStats    Stats
	lastPrint    time.Time
}

func NewKVService() *KVService {
	kvs := &KVService{}
	kvs.mp = make(map[string]Value)
	kvs.transactions = make(map[uint32]*transaction)
	kvs.lastPrint = time.Now()
	return kvs
}

func (kv *KVService) addToTransaction(transactionId uint32, op kvs.TransactionOperation) *transaction {
	var retval *transaction
	t, found := kv.transactions[transactionId]
	if !found {
		t = &transaction{
			id:  transactionId,
			ops: make([]kvs.TransactionOperation, 0),
		}
		t.ops = append(t.ops, op)
		kv.transactions[transactionId] = t
		retval = t
	} else {
		t.ops = append(t.ops, op)
		retval = t
	}
	return retval
}

func (kv *KVService) getValue(Key string) Value {
	value, found := kv.mp[Key]
	if !found {
		value = Value{
			value:   "1000",
			writer:  nil,
			readers: make(map[uint32]*transaction),
		}
		kv.mp[Key] = value
	}
	return value
}

func (kv *KVService) TxnGet(req *kvs.TxnRequest, res *kvs.TxnResponse) error {
	kv.Lock()
	defer kv.Unlock()

	tx, ok := kv.transactions[req.TxID]
	if !ok {
		tx = &Transaction{
			TxID: req.TxID,
			ReadSet: make(map[string]bool),
			WriteSet: make(map[string]string),
			Status: Pending,
		}
		kv.transactions[req.TxID] = tx
	}

	// Check if client already wrote this key
	if val, found := tx.WriteSet[req.Key]; found {
		res.Value = val
		res.Ok = true
		return nil
	}

	entry, found := kv.data[req.Key]
	if !found {
		entry = &KeyEntry{Value: "", Readers: make(map[string]bool)}
		kv.data[req.Key] = entry
	}

	// Try to acquire shared lock
	if entry.Writer != "" && entry.Writer != req.TxID {
		res.Ok = false
		return nil // lock held by another txn
	}

	entry.Readers[req.TxID] = true
	tx.ReadSet[req.Key] = true
	res.Value = entry.Value
	res.Ok = true
	kv.stats.gets++
	return nil
}


func (kv *KVService) TxnPut(req *kvs.TxnRequest, res *kvs.TxnResponse) error {
	kv.Lock()
	defer kv.Unlock()

	tx, ok := kv.transactions[req.TxID]
	if !ok {
		tx = &Transaction{
			TxID: req.TxID,
			ReadSet: make(map[string]bool),
			WriteSet: make(map[string]string),
			Status: Pending,
		}
		kv.transactions[req.TxID] = tx
	}

	entry, found := kv.data[req.Key]
	if !found {
		entry = &KeyEntry{Value: "", Readers: make(map[string]bool)}
		kv.data[req.Key] = entry
	}

	// Try to acquire exclusive lock
	if (entry.Writer != "" && entry.Writer != req.TxID) ||
		(len(entry.Readers) > 0 && !(len(entry.Readers) == 1 && entry.Readers[req.TxID])) {
		res.Ok = false
		return nil // Cannot acquire exclusive lock
	}

	entry.Writer = req.TxID
	delete(entry.Readers, req.TxID) // Upgrade from shared to exclusive if needed
	tx.WriteSet[req.Key] = req.Value
	res.Ok = true
	kv.stats.puts++
	return nil
}

func (kv *KVService) Commit(req *kvs.CommitRequest, res *kvs.CommitResponse) error {
	kv.Lock()
	defer kv.Unlock()

	tx, ok := kv.transactions[req.TxID]
	if !ok || tx.Status != Pending {
		res.Ok = false
		return nil
	}

	// Apply all writes
	for k, v := range tx.WriteSet {
		entry := kv.data[k]
		entry.Value = v
	}

	// Release all locks
	for k := range tx.ReadSet {
		entry := kv.data[k]
		delete(entry.Readers, req.TxID)
	}
	for k := range tx.WriteSet {
		entry := kv.data[k]
		if entry.Writer == req.TxID {
			entry.Writer = ""
		}
	}

	tx.Status = Committed
	if req.Lead {
		kv.commits++
	}
	res.Ok = true
	return nil
}

func (kv *KVService) Abort(req *kvs.CommitRequest, res *kvs.CommitResponse) error {
	kv.Lock()
	defer kv.Unlock()

	tx, ok := kv.transactions[req.TxID]
	if !ok || tx.Status != Pending {
		res.Ok = false
		return nil
	}

	// Just release all locks
	for k := range tx.ReadSet {
		entry := kv.data[k]
		delete(entry.Readers, req.TxID)
	}
	for k := range tx.WriteSet {
		entry := kv.data[k]
		if entry.Writer == req.TxID {
			entry.Writer = ""
		}
	}

	tx.Status = Aborted
	if req.Lead {
		kv.aborts++
	}
	res.Ok = true
	return nil
}


func (kv *KVService) GetAccountBalance(request *kvs.GetSumRequest, response *kvs.GetSumResponse) error {
	kv.Lock()
	defer kv.Unlock()
	if v, found := kv.mp[request.Key]; found {
		response.Value = v.value
	}

	return nil
}

func (kv *KVService) printStats() {
	kv.Lock()
	stats := kv.stats
	prevStats := kv.prevStats
	commits := kv.commits
	aborts := kv.aborts
	kv.prevStats = stats
	now := time.Now()
	lastPrint := kv.lastPrint
	kv.lastPrint = now
	kv.Unlock()

	diff := stats.Sub(&prevStats)
	deltaS := now.Sub(lastPrint).Seconds()

	fmt.Printf("commits/s %.2f\naborts/s %.2f\nops/s %.2f\ncommit/s %.2f\nabort/s %.2f\n\n",
		float64(diff.commits)/deltaS,
		float64(diff.aborts)/deltaS,
		float64(diff.commits+diff.aborts)/deltaS,
		float64(commits)/deltaS,
		float64(aborts)/deltaS)
}


func main() {
	port := flag.String("port", "8080", "Port to run the server on")
	// numShards = *flag.Uint64("num-shards", 1, "Number of Shards in the KVStore")
	//mapAllocCount := *flag.Uint64("alloc", 400_000, "Number expected for keys per shard")
	// enableCache = *flag.Bool("cache", false, "Use cached values for string storage")
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
