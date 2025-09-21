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

var numShards uint64
var enableCache bool

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

type transaction struct {
	clientID uint32
	ops      []kvs.TransactionOperation
}

type Value struct {
	value   string
	writer  *transaction
	readers map[uint32]*transaction
	lock    sync.Mutex
}

type KVService struct {
	mp              map[string]*Value
	mpLock          sync.Mutex
	transactionLock sync.Mutex
	transactions    map[uint32]*transaction
	stats           Stats
	prevStats       Stats
	lastPrint       time.Time
}

func NewKVService() *KVService {
	kvs := &KVService{}
	kvs.mp = make(map[string]*Value)
	kvs.lastPrint = time.Now()
	return kvs
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	fmt.Printf("Get %s for transaction %d\n", request.Key, request.TransactionId)
	kv.transactionLock.Lock()
	t, ok := kv.transactions[request.TransactionId]
	if !ok {
		t = &transaction{
			clientID: request.TransactionId,
			ops:      make([]kvs.TransactionOperation, 0),
		}
		kv.transactions[request.TransactionId] = t
	} else {
		t.ops = append(t.ops, kvs.TransactionOperation{
			IsRead: true,
			Key:    request.Key,
		})
		kv.transactions[request.TransactionId] = t
	}

	kv.transactionLock.Unlock()
	//check if write locked
	if v, ok := kv.mp[request.Key]; ok && v.writer != nil {
		response.Yes = false
		fmt.Printf("Key %s is write locked for transaction %d\n", request.Key, request.TransactionId)
		fmt.Printf("Current writer: %v\n", v.writer)
		return nil
	}
	//get read lock
	kv.mp[request.Key].lock.Lock()
	defer kv.mp[request.Key].lock.Unlock()
	if v, ok := kv.mp[request.Key]; ok {
		v.readers[request.TransactionId] = t
	}

	if v, found := kv.mp[request.Key]; found {
		response.Value = v.value
		response.Yes = true
	}
	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	fmt.Printf("Put %s for transaction %d\n", request.Key, request.TransactionId)
	//save transaction operation
	kv.transactionLock.Lock()
	t := kv.transactions[request.TransactionId]
	t.ops = append(t.ops, kvs.TransactionOperation{
		IsRead: false,
		Key:    request.Key,
		Value:  request.Value,
	})
	kv.transactions[request.TransactionId] = t
	kv.transactionLock.Unlock()

	//check if write locked
	if v, ok := kv.mp[request.Key]; ok && v.writer != nil && len(v.readers) <= 1 && v.readers[request.TransactionId] != nil {
		//someone else has the write lock
		response.Yes = false
		fmt.Printf("Key %s is write locked for transaction %d\n", request.Key, request.TransactionId)
		fmt.Printf("Current writer: %v\n", v.writer)
		return nil
	} else {
		//get write lock
		if v, ok := kv.mp[request.Key]; ok {
			v.writer = t
		}
		fmt.Printf("Put %s : %s\n", request.Key, request.Value)
	}

	response.Yes = true

	return nil
}

func (kv *KVService) Commit(request *kvs.CommitRequest, response *kvs.CommitResponse) error {
	transactionId := request.TransactionId
	fmt.Printf("Committing transaction %d\n", transactionId)
	kv.transactionLock.Lock()
	defer kv.transactionLock.Unlock()
	transaction, ok := kv.transactions[transactionId]

	if ok {
		for _, op := range transaction.ops {
			if op.IsRead {
				kv.mp[op.Key].lock.Lock()
				kv.mp[op.Key].readers[transactionId] = nil
				kv.mp[op.Key].lock.Unlock()
			} else {
				kv.mp[op.Key].value = op.Value
				kv.mp[op.Key].writer = nil
			}
		}
		delete(kv.transactions, transactionId)
		response.Ack = true
		return nil
	}

	return nil
}

func (kv *KVService) Abort(request *kvs.CommitRequest, response *kvs.AbortResponse) error {
	transactionId := request.TransactionId
	fmt.Printf("Aborting transaction %d\n", transactionId)
	kv.transactionLock.Lock()
	defer kv.transactionLock.Unlock()
	transaction, ok := kv.transactions[request.TransactionId]
	if ok {
		for _, op := range transaction.ops {
			if op.IsRead {
				fmt.Printf("Aborting read lock on %s for tran id %d\n", op.Key, transactionId)
				kv.mp[op.Key].lock.Lock()
				kv.mp[op.Key].readers[transactionId] = nil
				kv.mp[op.Key].lock.Unlock()
			} else {
				kv.mp[op.Key].writer = nil
			}
		}
		delete(kv.transactions, request.TransactionId)
		response.Ack = true
		return nil
	}
	return nil
}

func (kv *KVService) InitializeAccount(request *kvs.InitializeAccountRequest, response *kvs.InitializeAccountResponse) error {
	kv.mpLock.Lock()
	defer kv.mpLock.Unlock()

	kv.mp[request.Key] = &Value{
		value:   request.Value,
		writer:  nil,
		readers: make(map[uint32]*transaction),
	}
	kv.transactions = make(map[uint32]*transaction)

	fmt.Printf("Initialized account %s with value %s\n", request.Key, request.Value)

	response.Ack = true
	return nil
}

func (kv *KVService) GetAccountBalance(request *kvs.GetSumRequest, response *kvs.GetSumResponse) error {
	kv.mpLock.Lock()
	kv.stats.gets++

	if value, found := kv.mp[request.Key]; found {
		response.Value = value.value
	}
	kv.mpLock.Unlock()

	return nil
}

// func (kv *KVService) printStats() {
// 	kv.Lock()
// 	stats := kv.stats
// 	prevStats := kv.prevStats
// 	kv.prevStats = stats
// 	now := time.Now()
// 	lastPrint := kv.lastPrint
// 	kv.lastPrint = now
// 	kv.Unlock()

// 	diff := stats.Sub(&prevStats)
// 	deltaS := now.Sub(lastPrint).Seconds()

// 	fmt.Printf("get/s %0.2f\nput/s %0.2f\nops/s %0.2f\n\n",
// 		float64(diff.gets)/deltaS,
// 		float64(diff.puts)/deltaS,
// 		float64(diff.gets+diff.puts)/deltaS)
// }

func main() {
	port := flag.String("port", "8080", "Port to run the server on")
	numShards = *flag.Uint64("num-shards", 1, "Number of Shards in the KVStore")
	//mapAllocCount := *flag.Uint64("alloc", 400_000, "Number expected for keys per shard")
	enableCache = *flag.Bool("cache", false, "Use cached values for string storage")
	flag.Parse()

	kvs := NewKVService()
	rpc.Register(kvs)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", fmt.Sprintf(":%v", *port))
	if e != nil {
		log.Fatal("listen error:", e)
	}

	fmt.Printf("Starting KVS server on :%s %t\n", *port, enableCache)

	go func() {
		for {
			// kvs.printStats()
			time.Sleep(1 * time.Second)
		}
	}()

	http.Serve(l, nil)
}
