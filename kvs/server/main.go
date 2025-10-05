package main

import (
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
	Prepared
	Committed
	Aborted
)

type Transaction struct {
	ClientID string
	TxID     string
	ReadSet  map[string]bool
	WriteSet map[string]string
	Status   TxStatus
}

type KeyEntry struct {
	Value   string
	Readers map[string]bool // clientID-txID
	Writer  string          // clientID-txID
}

type Stats struct {
	Commits uint64
	Aborts  uint64
}

func (s *Stats) Sub(prev *Stats) Stats {
	return Stats{Commits: s.Commits - prev.Commits, Aborts: s.Aborts - prev.Aborts}
}

type KVService struct {
	sync.Mutex
	Data         map[string]*KeyEntry
	Transactions map[string]*Transaction // clientID-txID -> Transaction
	Stats        Stats
	PrevStats    Stats
	LastPrint    time.Time
}

func NewKVService() *KVService {
	return &KVService{
		Data:         make(map[string]*KeyEntry),
		Transactions: make(map[string]*Transaction),
		LastPrint:    time.Now(),
	}
}

func lockID(clientID, txID string) string {
	return fmt.Sprintf("%s-%s", clientID, txID)
}

// -------------------- TxnGet --------------------
func (kv *KVService) TxnGet(req *kvs.TxnRequest, res *kvs.TxnResponse) error {
	kv.Lock()
	defer kv.Unlock()

	lID := lockID(req.ClientID, req.TxID)
	tx, ok := kv.Transactions[lID]
	if !ok {
		tx = &Transaction{
			ClientID: req.ClientID,
			TxID:     req.TxID,
			ReadSet:  make(map[string]bool),
			WriteSet: make(map[string]string),
			Status:   Pending,
		}
		kv.Transactions[lID] = tx
	}

	entry, found := kv.Data[req.Key]
	if !found {
		entry = &KeyEntry{Value: "", Readers: make(map[string]bool)}
		kv.Data[req.Key] = entry
	}

	if entry.Writer != "" && entry.Writer != lID {
		res.Ok = false
		return nil
	}

	entry.Readers[lID] = true
	tx.ReadSet[req.Key] = true
	res.Value = entry.Value
	res.Ok = true
	return nil
}

// -------------------- TxnPut --------------------
func (kv *KVService) TxnPut(req *kvs.TxnRequest, res *kvs.TxnResponse) error {
	kv.Lock()
	defer kv.Unlock()

	lID := lockID(req.ClientID, req.TxID)
	tx, ok := kv.Transactions[lID]
	if !ok {
		tx = &Transaction{
			ClientID: req.ClientID,
			TxID:     req.TxID,
			ReadSet:  make(map[string]bool),
			WriteSet: make(map[string]string),
			Status:   Pending,
		}
		kv.Transactions[lID] = tx
	}

	entry, found := kv.Data[req.Key]
	if !found {
		entry = &KeyEntry{Value: "", Readers: make(map[string]bool)}
		kv.Data[req.Key] = entry
	}

	if (entry.Writer != "" && entry.Writer != lID) ||
		(len(entry.Readers) > 0 && !(len(entry.Readers) == 1 && entry.Readers[lID])) {
		res.Ok = false
		return nil
	}

	entry.Writer = lID
	delete(entry.Readers, lID)
	tx.WriteSet[req.Key] = req.Value
	res.Ok = true
	return nil
}

// -------------------- 2PC Prepare --------------------
func (kv *KVService) Prepare(req *kvs.CommitRequest, res *kvs.CommitResponse) error {
	kv.Lock()
	defer kv.Unlock()
	lID := lockID(req.ClientID, req.TxID)
	tx, ok := kv.Transactions[lID]
	if !ok || tx.Status != Pending {
		res.Ok = false
		return nil
	}
	tx.Status = Prepared
	res.Ok = true
	return nil
}

// -------------------- 2PC Commit --------------------
func (kv *KVService) Commit(req *kvs.CommitRequest, res *kvs.CommitResponse) error {
	kv.Lock()
	defer kv.Unlock()
	lID := lockID(req.ClientID, req.TxID)
	tx, ok := kv.Transactions[lID]
	if !ok || tx.Status != Prepared {
		res.Ok = false
		return nil
	}

	for k, v := range tx.WriteSet {
		entry := kv.Data[k]
		entry.Value = v
	}

	for k := range tx.ReadSet {
		delete(kv.Data[k].Readers, lID)
	}
	for k := range tx.WriteSet {
		if kv.Data[k].Writer == lID {
			kv.Data[k].Writer = ""
		}
	}

	tx.Status = Committed
	if req.Lead {
		kv.Stats.Commits++
	}
	res.Ok = true
	return nil
}

// -------------------- 2PC Abort --------------------
func (kv *KVService) Abort(req *kvs.CommitRequest, res *kvs.CommitResponse) error {
	kv.Lock()
	defer kv.Unlock()
	lID := lockID(req.ClientID, req.TxID)
	tx, ok := kv.Transactions[lID]
	if !ok || (tx.Status != Pending && tx.Status != Prepared) {
		res.Ok = false
		return nil
	}

	for k := range tx.ReadSet {
		delete(kv.Data[k].Readers, lID)
	}
	for k := range tx.WriteSet {
		if kv.Data[k].Writer == lID {
			kv.Data[k].Writer = ""
		}
	}

	tx.Status = Aborted
	if req.Lead {
		kv.Stats.Aborts++
	}
	res.Ok = true
	return nil
}

// -------------------- Stats --------------------
func (kv *KVService) printStats() {
	kv.Lock()
	stats := kv.Stats
	prev := kv.PrevStats
	kv.PrevStats = stats
	last := kv.LastPrint
	kv.LastPrint = time.Now()
	kv.Unlock()

	diff := stats.Sub(&prev)
	delta := time.Since(last).Seconds()
	fmt.Printf("commits/s %.2f, aborts/s %.2f, ops/s %.2f\n",
		float64(diff.Commits)/delta,
		float64(diff.Aborts)/delta,
		float64(diff.Commits+diff.Aborts)/delta)
}

// -------------------- Main --------------------
func main() {
	kv := NewKVService()
	rpc.Register(kv)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("KV server started on :8080")

	go func() {
		for {
			kv.printStats()
			time.Sleep(time.Second)
		}
	}()

	http.Serve(l, nil)
}
