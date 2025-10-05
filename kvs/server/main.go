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
	ClientID string
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

type KVService struct {
	sync.Mutex
	data         map[string]*KeyEntry
	transactions map[string]*Transaction
	clients      map[string]map[string]*Transaction // clientID -> txID -> Transaction

	stats     Stats
	prevStats Stats
	lastPrint time.Time
}

func NewKVService() *KVService {
	kv := &KVService{}
	kv.data = make(map[string]*KeyEntry)
	kv.transactions = make(map[string]*Transaction)
	kv.clients = make(map[string]map[string]*Transaction)
	kv.lastPrint = time.Now()
	return kv
}

// Helper: get or create a transaction for a given client
func (kv *KVService) getOrCreateTxn(clientID, txID string) *Transaction {
	tx, ok := kv.transactions[txID]
	if ok {
		return tx
	}
	tx = &Transaction{
		TxID:     txID,
		ClientID: clientID,
		ReadSet:  make(map[string]bool),
		WriteSet: make(map[string]string),
		Status:   Pending,
	}
	kv.transactions[txID] = tx

	if _, ok := kv.clients[clientID]; !ok {
		kv.clients[clientID] = make(map[string]*Transaction)
	}
	kv.clients[clientID][txID] = tx

	return tx
}

func (kv *KVService) TxnGet(req *kvs.TxnRequest, res *kvs.TxnResponse) error {
	kv.Lock()
	defer kv.Unlock()

	tx := kv.getOrCreateTxn(req.ClientID, req.TxID)

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
		return nil
	}

	entry.Readers[req.TxID] = true
	tx.ReadSet[req.Key] = true
	res.Value = entry.Value
	res.Ok = true
	return nil
}

func (kv *KVService) TxnPut(req *kvs.TxnRequest, res *kvs.TxnResponse) error {
	kv.Lock()
	defer kv.Unlock()

	tx := kv.getOrCreateTxn(req.ClientID, req.TxID)

	entry, found := kv.data[req.Key]
	if !found {
		entry = &KeyEntry{Value: "", Readers: make(map[string]bool)}
		kv.data[req.Key] = entry
	}

	// Try to acquire exclusive lock
	if (entry.Writer != "" && entry.Writer != req.TxID) ||
		(len(entry.Readers) > 0 && !(len(entry.Readers) == 1 && entry.Readers[req.TxID])) {
		res.Ok = false
		return nil
	}

	entry.Writer = req.TxID
	delete(entry.Readers, req.TxID) // upgrade if needed
	tx.WriteSet[req.Key] = req.Value
	res.Ok = true
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

	// Release locks
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
		kv.stats.commits++
	}
	res.Ok = true

	// cleanup
	delete(kv.transactions, tx.TxID)
	delete(kv.clients[tx.ClientID], tx.TxID)
	if len(kv.clients[tx.ClientID]) == 0 {
		delete(kv.clients, tx.ClientID)
	}

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

	// Release locks
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
		kv.stats.aborts++
	}
	res.Ok = true

	// cleanup
	delete(kv.transactions, tx.TxID)
	delete(kv.clients[tx.ClientID], tx.TxID)
	if len(kv.clients[tx.ClientID]) == 0 {
		delete(kv.clients, tx.ClientID)
	}

	return nil
}

// Utility to get all active transactions for a client
func (kv *KVService) GetClientTransactions(clientID string) []*Transaction {
	kv.Lock()
	defer kv.Unlock()
	var result []*Transaction
	for _, tx := range kv.clients[clientID] {
		result = append(result, tx)
	}
	return result
}

func (kv *KVService) printStats() {
	kv.Lock()
	stats := kv.stats
	prevStats := kv.prevStats
	commits := kv.stats.commits
	aborts := kv.stats.aborts
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
	flag.Parse()

	kvsService := NewKVService()
	rpc.Register(kvsService)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", fmt.Sprintf(":%v", *port))
	if e != nil {
		log.Fatal("listen error:", e)
	}

	fmt.Printf("Starting KVS server on :%s\n", *port)

	go func() {
		for {
			kvsService.printStats()
			time.Sleep(1 * time.Second)
		}
	}()

	http.Serve(l, nil)
}
