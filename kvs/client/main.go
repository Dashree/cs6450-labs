package main

import (
	"flag"
	"fmt"
	"hash/maphash"
	"log"
	"net/rpc"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rstutsman/cs6450-labs/kvs"
)

var reqBatchsize uint32
var workloadsPerHost uint32

func getHostForKey(key string, numHosts int) int {
	if numHosts <= 0 {
		panic("n must be > 0")
	}
	var h maphash.Hash
	h.WriteString(key)
	return int(h.Sum64() % uint64(numHosts))
}

type Clients struct {
	clients []*Client
}

type Client struct {
	rpcClient *rpc.Client
}

func Dial(addr string) *Client {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	return &Client{rpcClient}
}

func (client *Client) Get(key string) bool {
	request := kvs.GetRequest{
		Key: key,
	}
	response := kvs.GetResponse{}
	err := client.rpcClient.Call("KVService.Get", &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	return response.Yes
}

func (client *Client) Put(key string, value string) bool {
	request := kvs.PutRequest{
		Key:   key,
		Value: value,
	}
	response := kvs.PutResponse{}
	err := client.rpcClient.Call("KVService.Put", &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	return response.Yes
}

func (clients *Clients) Begin(clientId int, operations []kvs.TransactionOperation) {
	//creates and enters a transaction.
	//Generate transaction ID
	transactionId := uuid.New()
	//include Client ID
	//server list
	//Keep a structure to track all servers that gets/puts are sent to. since we need to send commit or aborts to them
	//Track the writeset for the transaction.
	//This writeset is for when the client calls a get on something they already put
	for {
		serverList := []int{} //keeps track of index into clients.clients
		for _, op := range operations {
			index := getHostForKey(op.Key, len(clients.clients))
			serverList = append(serverList, index)
			var response bool
			if op.IsRead {
				response = clients.clients[index].Get(op.Key)
			} else {
				response = clients.clients[index].Put(op.Key, op.Value)
			}
			if response == false {
				clients.Abort()
				continue
			}
		}
		clients.Commit()
		break
	}
	return
}

func (clients *Clients) Commit() {
	//Contact all servers involved in transaction
	//server should do all puts that are pending
	//server should drop all locks
}

func (clients *Clients) Abort() {
	//calling abort is illegal unless a transaction has been entered
	//Contact all servers involved in transaction
	//server should discard all puts that are pending
	//server should drop all locks
}

func runClient(clientId int, addrs []string, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64) {
	clients := Clients{clients: []*Client{}}
	for _, addr := range addrs {
		client := Dial(addr)
		clients.clients = append(clients.clients, client)
	}

	value := strings.Repeat("x", 128)
	const batchSize = 1024

	opsCompleted := uint64(0)

	for !done.Load() {
		for j := 0; j < batchSize; j++ {
			var transactionOps []kvs.TransactionOperation
			for i := 0; i < 3; i++ {
				op := workload.Next()
				key := fmt.Sprintf("%d", op.Key)
				transactionOps = append(transactionOps, kvs.TransactionOperation{IsRead: op.IsRead, Key: key, Value: value})
			}
			//Begin Transaction
			clients.Begin(clientId, transactionOps)
		}
	}
	resultsCh <- opsCompleted

}

type HostList []string

func (h *HostList) String() string {
	return strings.Join(*h, ",")
}

func (h *HostList) Set(value string) error {
	*h = strings.Split(value, ",")
	return nil
}

func main() {
	hosts := HostList{}

	flag.Var(&hosts, "hosts", "Comma-separated list of host:ports to connect to")
	theta := flag.Float64("theta", 0.99, "Zipfian distribution skew parameter")
	workload := flag.String("workload", "YCSB-B", "Workload type (YCSB-A, YCSB-B, YCSB-C)")
	secs := flag.Int("secs", 30, "Duration in seconds for each client to run")
	clientID := flag.Int("clientid", -1, "Relative client ID starting at 0")
	reqBatchsize = uint32(*flag.Uint64("batch-size", 8, "Batch for Get Requests"))
	workloadsPerHost = uint32(*flag.Uint64("thrds-per-host", 8, "Number of go routines per hosts"))

	flag.Parse()

	if len(hosts) == 0 {
		hosts = append(hosts, "localhost:8080")
	}

	fmt.Printf(
		"hosts %v\n"+
			"theta %.2f\n"+
			"workload %s\n"+
			"secs %d\n",
		hosts, *theta, *workload, *secs,
	)

	start := time.Now()

	done := atomic.Bool{}
	resultsCh := make(chan uint64)
	tltOpsCompleted := uint64(0)

	var numberOfClientsPerHost = runtime.NumCPU() * int(workloadsPerHost)
	numberOfClientsPerHost = 1
	for j := 0; j < numberOfClientsPerHost; j++ {
		go func(clientId int) {
			workload := kvs.NewWorkload(*workload, *theta)
			runClient(clientId, hosts, &done, workload, resultsCh)
		}(*clientID)
	}

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)

	totalWorkloads := numberOfClientsPerHost
	for i := 0; i < totalWorkloads; i++ {
		tltOpsCompleted += <-resultsCh
	}

	elapsed := time.Since(start)

	opsPerSec := float64(tltOpsCompleted) / elapsed.Seconds()
	fmt.Printf("throughput %.2f ops/s\n", opsPerSec)
}
