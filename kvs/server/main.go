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

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	kv.Lock()
	defer kv.Unlock()
	transaction := kv.addToTransaction(uint32(request.TrasactionId), kvs.TransactionOperation{IsRead: true, Key: request.Key})

	value := kv.getValue(request.Key)
	if value.writer != nil && value.writer.id != uint32(request.TrasactionId) {
		response.Ack = false
		return nil
	} else {
		//get read lock
		value.readers[transaction.id] = transaction
		response.Ack = true
		response.Value = value.value
	}
	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	kv.Lock()
	defer kv.Unlock()

	transaction := kv.addToTransaction(uint32(request.TrasactionId), kvs.TransactionOperation{IsRead: false, Key: request.Key, Value: request.Value})

	value := kv.getValue(request.Key)
	_, found := value.readers[request.TrasactionId]
	if value.writer != nil && value.writer.id != request.TrasactionId {
		fmt.Printf("writer taken by someone else")
		//if writer is held by someone else
		response.Ack = false
	} else if (found && len(value.readers) > 1) || (!found && len(value.readers) > 0) {
		// fmt.Printf("reader in the way\n")
		// fmt.Printf("len of readers %d", len(value.readers))
		//if readers contains someone else other than transaction
		response.Ack = false
	} else {
		//get write lock
		value.writer = transaction
		response.Ack = true
		kv.mp[request.Key] = value
	}

	return nil
}

func (kv *KVService) Commit(request *kvs.CommitRequest, response *kvs.CommitResponse) error {
	kv.Lock()
	defer kv.Unlock()
	if request.Lead {
		kv.stats.commits++
	}

	transactionId := request.TransactionId
	transaction := kv.transactions[transactionId]
	for _, op := range transaction.ops {
		v := kv.mp[op.Key]
		if op.IsRead {
			//skip
		} else {
			v.writer = nil
			v.value = op.Value
		}
		delete(v.readers, transactionId)
		kv.mp[op.Key] = v
	}
	response.Ack = true
	delete(kv.transactions, transactionId)
	//free locks

	return nil
}

func (kv *KVService) Abort(request *kvs.CommitRequest, response *kvs.AbortResponse) error {
	kv.Lock()
	defer kv.Unlock()
	if request.Lead {
		kv.stats.aborts++
	}

	transactionId := request.TransactionId
	transaction := kv.transactions[transactionId]
	for _, op := range transaction.ops {
		v := kv.mp[op.Key]
		if op.IsRead {
			//skip
		} else {
			v.writer = nil
		}
		delete(v.readers, transactionId)
		kv.mp[op.Key] = v
	}
	response.Ack = true
	delete(kv.transactions, transactionId)
	//free locks

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
	commits := kv.stats
	prevCommits := kv.prevStats
	kv.prevStats = commits
	now := time.Now()
	lastPrint := kv.lastPrint
	kv.lastPrint = now
	kv.Unlock()

	diff := commits.Sub(&prevCommits)
	deltaS := now.Sub(lastPrint).Seconds()

	fmt.Printf("aborts/s %0.2f\ncommits/s %0.2f\nops/s %0.2f\n\n",
		float64(diff.aborts)/deltaS,
		float64(diff.commits)/deltaS,
		float64(diff.aborts+diff.commits)/deltaS)
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
