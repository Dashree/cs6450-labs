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

type Transaction struct {
	clientID uint32
	ops      []kvs.TransactionOperation
	lock     sync.Mutex
}

type Value struct {
	value   string
	writer  *Transaction
	readers sync.Map //map[uint32]*transaction
	lock    sync.Mutex
}

type KVService struct {
	mp           sync.Map
	mpLock       sync.Mutex
	transactions sync.Map //map[uint32]*transaction
	lastPrint    time.Time
}

func NewKVService() *KVService {
	kvs := &KVService{}
	kvs.lastPrint = time.Now()
	return kvs
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	fmt.Printf("Get %s for transaction %d\n", request.Key, request.TransactionId)
	t, ok := kv.transactions.Load(request.TransactionId)

	if !ok {
		kv.transactions.Store(request.TransactionId, &Transaction{
			clientID: request.TransactionId,
			ops:      make([]kvs.TransactionOperation, 0),
		})
	} else {
		transaction := t.(*Transaction)
		transaction.lock.Lock()
		transaction.ops = append(transaction.ops, kvs.TransactionOperation{
			IsRead: true,
			Key:    request.Key,
		})
		transaction.lock.Unlock()
	}

	//check if write locked
	v, ok := kv.mp.Load(request.Key)
	value := v.(*Value)
	if ok && value.writer != nil {
		response.Yes = false
		fmt.Printf("Key %s is write locked for transaction %d\n", request.Key, request.TransactionId)
		fmt.Printf("Current writer: %v\n", value.writer)
		return nil
	}

	//get read lock
	if ok {
		t, _ := kv.transactions.Load(request.TransactionId)
		Transaction := t.(*Transaction)
		value.readers.Store(request.TransactionId, Transaction) // now valid
		Transaction.lock.Lock()
		defer Transaction.lock.Unlock()
		response.Value = value.value
		response.Yes = true
	}
	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	fmt.Printf("Put %s for transaction %d\n", request.Key, request.TransactionId)
	//save transaction operation
	t, ok := kv.transactions.Load(request.TransactionId)
	transaction := t.(*Transaction)
	transaction.lock.Lock()
	if !ok {
		kv.transactions.Store(request.TransactionId, Transaction{
			clientID: request.TransactionId,
			ops:      make([]kvs.TransactionOperation, 0),
		})
	} else {
		transaction.ops = append(transaction.ops, kvs.TransactionOperation{
			IsRead: false,
			Key:    request.Key,
			Value:  request.Value,
		})
	}
	transaction.lock.Unlock()

	//check if write locked
	v, ok := kv.mp.Load(request.Key)
	value := v.(*Value)

	if ok && value.writer != nil && value.writer.clientID != request.TransactionId {
		//someone else has the write lock
		response.Yes = false
		fmt.Printf("Key %s is write locked for transaction %d\n", request.Key, request.TransactionId)
		fmt.Printf("Current writer: %v\n", value.writer)
		return nil
	} else {
		//get write lock
		value.writer = transaction
		fmt.Printf("Put %s : %s\n", request.Key, request.Value)
	}

	response.Yes = true

	return nil
}

func (kv *KVService) Commit(request *kvs.CommitRequest, response *kvs.CommitResponse) error {
	transactionId := request.TransactionId
	fmt.Printf("Committing transaction %d\n", transactionId)
	t, ok := kv.transactions.Load(transactionId)
	transaction := t.(*Transaction)
	transaction.lock.Lock()
	defer transaction.lock.Unlock()

	if ok {
		fmt.Printf("transaction ops: %v\n", transaction.ops)
		for _, op := range transaction.ops {
			v, _ := kv.mp.Load(op.Key)
			Value := v.(*Value)
			if op.IsRead {
				Value.lock.Lock()
				Value.readers.Delete(transactionId)
				Value.lock.Unlock()
			} else {
				fmt.Printf("Setting key %s to value %s in commit\n", op.Key, op.Value)
				Value.value = op.Value
				Value.writer = nil
			}
		}
		kv.transactions.Delete(request.TransactionId)
		response.Ack = true
		return nil
	}

	return nil
}

func (kv *KVService) Abort(request *kvs.CommitRequest, response *kvs.AbortResponse) error {
	transactionId := request.TransactionId
	fmt.Printf("Aborting transaction %d\n", transactionId)
	t, ok := kv.transactions.Load(transactionId)
	transaction := t.(*Transaction)
	transaction.lock.Lock()
	defer transaction.lock.Unlock()
	if ok {
		for _, op := range transaction.ops {
			v, _ := kv.mp.Load(op.Key)
			Value := v.(*Value)
			if op.IsRead {
				Value.lock.Lock()
				Value.readers.Delete(transactionId)
				Value.lock.Unlock()
			} else {
				Value.writer = nil
			}
		}
		kv.transactions.Delete(request.TransactionId)
		response.Ack = true
		return nil
	}
	return nil
}

func (kv *KVService) InitializeAccount(request *kvs.InitializeAccountRequest, response *kvs.InitializeAccountResponse) error {

	kv.mp.Store(request.Key, &Value{
		value:  request.Value,
		writer: nil,
	})
	kv.transactions = sync.Map{}

	fmt.Printf("Initialized account %s with value %s\n", request.Key, request.Value)

	response.Ack = true
	return nil
}

func (kv *KVService) GetAccountBalance(request *kvs.GetSumRequest, response *kvs.GetSumResponse) error {

	if v, found := kv.mp.Load(request.Key); found {
		value := v.(*Value)
		response.Value = value.value
	}

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
