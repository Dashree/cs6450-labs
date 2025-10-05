package main

import (
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"net/rpc"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

var workloadsPerHost uint32
var numberOfAccountsperClient int

type Txn struct {
	ID         string
	ShardTxIDs map[int]string
	WriteSet   map[string]string
}

type Client struct {
	ID         int
	RPCClients map[int]*rpc.Client
	NumShards  int
}

// ------------------- Sharding -------------------
func ShardForKey(key string, numShards int) int {
	h := fnv.New64a()
	h.Write([]byte(key))
	return int(h.Sum64() % uint64(numShards))
}

// ------------------- Client -------------------
func NewClient(id int, addrs []string) *Client {
	clients := make(map[int]*rpc.Client)
	for i, addr := range addrs {
		c, err := rpc.Dial("tcp", addr)
		if err != nil {
			log.Fatal(err)
		}
		clients[i] = c
	}
	return &Client{ID: id, RPCClients: clients, NumShards: len(addrs)}
}

func (c *Client) Begin() *Txn {
	return &Txn{
		ID:         fmt.Sprintf("%d-%d", c.ID, time.Now().UnixNano()),
		ShardTxIDs: make(map[int]string),
		WriteSet:   make(map[string]string),
	}
}

// ------------------- TxnGet -------------------
func (c *Client) TxnGet(tx *Txn, key string) (int, bool) {
	shard := ShardForKey(key, c.NumShards)
	txID := fmt.Sprintf("%s-%d", tx.ID, shard)
	tx.ShardTxIDs[shard] = txID

	req := kvs.TxnRequest{ClientID: strconv.Itoa(c.ID), TxID: txID, Key: key}
	var res kvs.TxnResponse
	err := c.RPCClients[shard].Call("KVService.TxnGet", &req, &res)
	if err != nil {
		log.Fatal(err)
	}
	val, _ := strconv.Atoi(res.Value)
	return val, res.Ok
}

// ------------------- TxnPut -------------------
func (c *Client) TxnPut(tx *Txn, key, value string) bool {
	shard := ShardForKey(key, c.NumShards)
	txID := fmt.Sprintf("%s-%d", tx.ID, shard)
	tx.ShardTxIDs[shard] = txID
	tx.WriteSet[key] = value

	req := kvs.TxnRequest{ClientID: strconv.Itoa(c.ID), TxID: txID, Key: key, Value: value}
	var res kvs.TxnResponse
	err := c.RPCClients[shard].Call("KVService.TxnPut", &req, &res)
	if err != nil {
		log.Fatal(err)
	}
	return res.Ok
}

// ------------------- Two-phase commit -------------------
func (c *Client) TwoPhaseCommit(tx *Txn) bool {
	// Phase 1: Prepare
	for shard, txID := range tx.ShardTxIDs {
		req := kvs.CommitRequest{ClientID: strconv.Itoa(c.ID), TxID: txID, Lead: false}
		var res kvs.CommitResponse
		err := c.RPCClients[shard].Call("KVService.Prepare", &req, &res)
		if err != nil || !res.Ok {
			// Abort all
			for s, t := range tx.ShardTxIDs {
				ab := kvs.CommitRequest{ClientID: strconv.Itoa(c.ID), TxID: t, Lead: false}
				var r kvs.CommitResponse
				c.RPCClients[s].Call("KVService.Abort", &ab, &r)
			}
			return false
		}
	}

	// Phase 2: Commit
	for shard, txID := range tx.ShardTxIDs {
		req := kvs.CommitRequest{ClientID: strconv.Itoa(c.ID), TxID: txID, Lead: false}
		var res kvs.CommitResponse
		err := c.RPCClients[shard].Call("KVService.Commit", &req, &res)
		if err != nil || !res.Ok {
			log.Fatal("Commit failed after prepare!")
		}
	}
	return true
}

// ------------------- RunTransaction -------------------
func (c *Client) RunTransaction(keys []string, value string) {
	for {
		tx := c.Begin()
		allOk := true
		for _, key := range keys {
			if !c.TxnPut(tx, key, value) {
				allOk = false
				break
			}
		}
		if allOk && c.TwoPhaseCommit(tx) {
			return
		}
	}
}

// ------------------- RunBankTransfer -------------------
func (c *Client) RunBankTransfer(src, dst int, amount int) {
	for {
		tx := c.Begin()
		srcKey := fmt.Sprintf("%d", src)
		dstKey := fmt.Sprintf("%d", dst)

		srcBal, ok := c.TxnGet(tx, srcKey)
		if !ok || srcBal < amount {
			continue
		}
		dstBal, ok := c.TxnGet(tx, dstKey)
		if !ok {
			continue
		}

		c.TxnPut(tx, srcKey, fmt.Sprintf("%d", srcBal-amount))
		c.TxnPut(tx, dstKey, fmt.Sprintf("%d", dstBal+amount))

		if c.TwoPhaseCommit(tx) {
			return
		}
	}
}

// ------------------- runClient routine -------------------
func runClient(id int, addrs []string, done *atomic.Bool, keysPerTx int, resultsCh chan<- uint64) {
	client := NewClient(id, addrs)
	value := strings.Repeat("x", 64)
	opsCompleted := uint64(0)

	for !done.Load() {
		var keys []string
		for i := 0; i < keysPerTx; i++ {
			keys = append(keys, fmt.Sprintf("%d", rand.Intn(1000)))
		}
		client.RunTransaction(keys, value)
		opsCompleted += uint64(keysPerTx)
	}
	resultsCh <- opsCompleted
}

// ------------------- runBankClient routine -------------------
func runBankClient(id int, addrs []string, done *atomic.Bool, resultsCh chan<- uint64) {
	client := NewClient(id, addrs)
	count := uint64(0)
	for !done.Load() {
		src := rand.Intn(numberOfAccountsperClient)
		dst := rand.Intn(numberOfAccountsperClient)
		if src == dst {
			dst = (dst + 1) % numberOfAccountsperClient
		}
		client.RunBankTransfer(src, dst, 100)
		count++
	}
	resultsCh <- count
}

// ------------------- Main -------------------
func main() {
	addrs := []string{"localhost:8080", "localhost:8081", "localhost:8082"} // multiple shards
	workloadsPerHost = 4
	numberOfAccountsperClient = 10
	keysPerTx := 3
	durationSecs := 8

	start := time.Now()
	done := atomic.Bool{}
	resultsCh := make(chan uint64)

	for i := 0; i < int(workloadsPerHost)*len(addrs); i++ {
		go runClient(i, addrs, &done, keysPerTx, resultsCh)
		// Or runBankClient(i, addrs, &done, resultsCh) for bank transfers
	}

	time.Sleep(time.Duration(durationSecs) * time.Second)
	done.Store(true)

	totalOps := uint64(0)
	for i := 0; i < int(workloadsPerHost)*len(addrs); i++ {
		totalOps += <-resultsCh
	}

	elapsed := time.Since(start)
	fmt.Printf("Total throughput: %.2f ops/s\n", float64(totalOps)/elapsed.Seconds())
}
