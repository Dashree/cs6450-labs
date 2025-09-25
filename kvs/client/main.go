package main

import (
	"flag"
	"fmt"
	"hash/maphash"
	"log"
	"math/rand"
	"net/rpc"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rstutsman/cs6450-labs/kvs"
)

var reqBatchsize uint32
var workloadsPerHost uint32
var numberOfAccountsperClient int

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

func (client *Client) Get(key string, transactionId uuid.UUID) kvs.GetResponse {
	request := kvs.GetRequest{
		Key:           key,
		TransactionId: transactionId.ID(),
	}
	response := kvs.GetResponse{}
	err := client.rpcClient.Call("KVService.Get", &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	return response
}

func (client *Client) Put(key string, value string, transactionId uuid.UUID) kvs.PutResponse {
	request := kvs.PutRequest{
		Key:           key,
		Value:         value,
		TransactionId: transactionId.ID(),
	}
	response := kvs.PutResponse{}
	err := client.rpcClient.Call("KVService.Put", &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	return response
}

func (clients *Clients) Begin(clientId int, src int, dst int, operations []kvs.TransactionOperation) {
	//creates and enters a transaction.
	//Generate transaction ID
	transactionId := uuid.New()
	amount := rand.Intn(2)
	fmt.Printf("Account %d requesting %d from %d with transaction id %d\n", src, amount, dst, transactionId.ID())

	for {
		serverList := []int{} //keeps track of index into clients.clients
		for _, op := range operations {
			index := getHostForKey(op.Key, len(clients.clients))
			if !slices.Contains(serverList, index) {
				serverList = append(serverList, index)
			}
			var response kvs.GetResponse
			var response2 kvs.GetResponse
			var response3 kvs.PutResponse
			var response4 kvs.PutResponse
			//pretend all operations are a read AND write
			//do two reads
			response = clients.clients[index].Get(strconv.Itoa(src), transactionId)
			response2 = clients.clients[index].Get(strconv.Itoa(dst), transactionId)
			dstValue, _ := strconv.Atoi(response2.Value)
			srcValue, _ := strconv.Atoi(response.Value)
			// fmt.Printf("Account %d has value %d\n", src, srcValue)
			// fmt.Printf("Account %d has value %d\n", dst, dstValue)
			//if reads are okay do two writes
			if dstValue > amount && (response.Yes && response2.Yes) {
				response3 = clients.clients[index].Put(strconv.Itoa(src), strconv.Itoa(srcValue+amount), transactionId)
				response4 = clients.clients[index].Put(strconv.Itoa(dst), strconv.Itoa(dstValue-amount), transactionId)
				if response3.Yes && response4.Yes {
					clients.Commit(transactionId, serverList)
					fmt.Printf("Account %d successfully transferred %d to %d using transaction %d\n", src, amount, dst, transactionId.ID())
					return
				}
			}
			fmt.Printf("Account %d failed to transfer %d to %d. Transaction: %d Retrying...\n", src, amount, dst, transactionId.ID())
			clients.Abort(transactionId, serverList)
			time.Sleep(5 * time.Second)
			continue
		}
	}
}

func (clients *Clients) Commit(transactionId uuid.UUID, serverList []int) {
	//Contact all servers involved in transaction
	//server should do all puts that are pending
	//server should drop all locks
	for _, serverIdx := range serverList {
		request := kvs.CommitRequest{
			TransactionId: transactionId.ID(),
		}
		response := kvs.CommitResponse{}
		err := clients.clients[serverIdx].rpcClient.Call("KVService.Commit", &request, &response)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (clients *Clients) Abort(transactionId uuid.UUID, serverList []int) {
	//calling abort is illegal unless a transaction has been entered
	//Contact all servers involved in transaction
	//server should discard all puts that are pending
	//server should drop all locks
	for _, serverIdx := range serverList {
		request := kvs.AbortRequest{
			TransactionId: transactionId.ID(),
		}
		response := kvs.AbortResponse{}
		err := clients.clients[serverIdx].rpcClient.Call("KVService.Abort", &request, &response)
		if err != nil {
			log.Fatal(err)
		}

	}
}

func runClient(clientId int, accountId int, addrs []string, done *atomic.Bool, workload *kvs.Workload) {
	clients := Clients{clients: []*Client{}}
	for _, addr := range addrs {
		client := Dial(addr)
		clients.clients = append(clients.clients, client)
	}

	value := strings.Repeat("x", 128)
	const batchSize = 1024

	for !done.Load() {
		for j := 0; j < batchSize; j++ {
			var transactionOps []kvs.TransactionOperation
			for i := 0; i < 2; i++ {
				op := workload.Next()
				key := fmt.Sprintf("%d", op.Key)
				transactionOps = append(transactionOps, kvs.TransactionOperation{IsRead: op.IsRead, Key: key, Value: value})
			}
			n := rand.Intn(numberOfAccountsperClient)
			if n == accountId {
				n = (n + 1) % numberOfAccountsperClient
			}
			//Begin Transaction
			clients.Begin(clientId, accountId, n, transactionOps)
		}
	}
}

func initialize(addrs []string, value string) {
	clients := Clients{clients: []*Client{}}
	for _, addr := range addrs {
		client := Dial(addr)
		clients.clients = append(clients.clients, client)
	}
	for i := 0; i < numberOfAccountsperClient; i++ {
		key := fmt.Sprintf("%d", numberOfAccountsperClient-1-i)
		clients.clients[getHostForKey(key, len(clients.clients))].initializeAccount(key, value)
	}
}

func (client *Client) initializeAccount(key string, value string) bool {
	request := kvs.InitializeAccountRequest{
		Key:   key,
		Value: value,
	}
	response := kvs.InitializeAccountResponse{}
	err := client.rpcClient.Call("KVService.InitializeAccount", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
	return response.Ack
}

func getTotal(addrs []string) {
	clients := Clients{clients: []*Client{}}
	for _, addr := range addrs {
		client := Dial(addr)
		clients.clients = append(clients.clients, client)
	}
	sum := 0
	for i := 0; i < numberOfAccountsperClient; i++ {
		key := fmt.Sprintf("%d", numberOfAccountsperClient-1-i)
		fmt.Printf("Getting sum for key %s\n", key)
		value := clients.clients[getHostForKey(key, len(clients.clients))].getSum(key)
		fmt.Printf("Total sum for key: %d\n", value)
		sum += value
	}
	fmt.Printf("Total sum across all accounts: %d\n", sum)

}

func (client *Client) getSum(key string) int {
	request := kvs.GetSumRequest{
		Key: key,
	}
	response := kvs.GetSumResponse{}
	err := client.rpcClient.Call("KVService.GetAccountBalance", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
	ret, _ := strconv.Atoi(response.Value)
	return ret
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
	secs := flag.Int("secs", 1, "Duration in seconds for each client to run")
	clientID := flag.Int("clientid", -1, "Relative client ID starting at 0")
	reqBatchsize = uint32(*flag.Uint64("batch-size", 8, "Batch for Get Requests"))
	workloadsPerHost = uint32(*flag.Uint64("thrds-per-host", 8, "Number of go routines per hosts"))
	numberOfAccountsperClient = *flag.Int("accounts-per-client", 3, "Number of accounts each client manages")

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
	// resultsCh := make(chan uint64)
	tltOpsCompleted := uint64(0)
	initialize(hosts, fmt.Sprintf("%d", 1000))

	for j := 0; j < numberOfAccountsperClient; j++ {
		go func(clientId int) {
			workload := kvs.NewWorkload(*workload, *theta)
			runClient(clientId, j, hosts, &done, workload)
		}(*clientID)
	}

	time.Sleep(2000 * time.Millisecond) // wait for final stats to be printed
	done.Store(true)

	elapsed := time.Since(start)

	opsPerSec := float64(tltOpsCompleted) / elapsed.Seconds()
	time.Sleep(1 * time.Second) // wait for final stats to be printed
	fmt.Printf("throughput %.2f ops/s\n", opsPerSec)
	getTotal(hosts)
}
