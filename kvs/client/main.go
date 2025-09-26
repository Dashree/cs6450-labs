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

func toString(integer int) string {
	return fmt.Sprintf("%d", integer)
}

func toInteger(str string) int {
	var integer int
	_, err := fmt.Sscanf(str, "%d", &integer)
	if err != nil {
		log.Fatalf("failed to parse integer from string %q: %v", str, err)
	}
	return integer
}

type Client struct {
	rpcClient *rpc.Client
}

type Clients struct {
	Clients []*Client
}

func Dial(addr string) *Client {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	return &Client{rpcClient}
}

func (client *Client) Get(key string, clientId int, transactionId uuid.UUID) kvs.GetResponse {
	request := kvs.GetRequest{
		Key:          key,
		TrasactionId: int(transactionId.ID()),
		ClientId:     clientId,
	}
	response := kvs.GetResponse{}
	err := client.rpcClient.Call("KVService.Get", &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	return response
}

func (client *Client) Put(key string, value string, clientId int, transactionId uuid.UUID) kvs.PutResponse {
	request := kvs.PutRequest{
		Key:          key,
		Value:        value,
		TrasactionId: transactionId.ID(),
		ClientId:     clientId,
	}
	response := kvs.PutResponse{}
	err := client.rpcClient.Call("KVService.Put", &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	return response
}

func (clients *Clients) Begin(clientId int, src int, dst int, amountToTransfer int) {
	//creates and enters a transaction.
	transactionId := uuid.New()
	fmt.Printf("Begin for %d \n", transactionId.ID())

	//Generate transaction ID
	//include Client ID
	for {
		serverList := []int{} //keeps track of index into clients.clients
		srcindex := getHostForKey(toString(src), len(clients.Clients))
		dstindex := getHostForKey(toString(dst), len(clients.Clients))
		serverList = append(serverList, srcindex)
		if !slices.Contains(serverList, dstindex) {
			serverList = append(serverList, dstindex)
		}
		var srcresponse kvs.GetResponse
		var dstresponse kvs.GetResponse
		srcresponse = clients.Clients[srcindex].Get(toString(src), clientId, transactionId)
		dstresponse = clients.Clients[dstindex].Get(toString(dst), clientId, transactionId)
		fmt.Printf("src get: %t \n", srcresponse.Ack)
		fmt.Printf("dst get: %t \n", dstresponse.Ack)

		if (!srcresponse.Ack) || (!dstresponse.Ack) || toInteger(dstresponse.Value) < amountToTransfer {
			fmt.Printf("aborting on get \n")
			clients.Abort(transactionId, serverList)
			return
		}

		srcputresponse := clients.Clients[srcindex].Put(toString(src), toString(toInteger(srcresponse.Value)+amountToTransfer), clientId, transactionId)
		if !srcputresponse.Ack {
			fmt.Printf("aborting on put src\n")

			clients.Abort(transactionId, serverList)
			return
		}
		dstputresponse := clients.Clients[dstindex].Put(toString(dst), toString(toInteger(dstresponse.Value)-amountToTransfer), clientId, transactionId)
		if !dstputresponse.Ack {
			fmt.Printf("aborting on put dst\n")

			clients.Abort(transactionId, serverList)
			return
		}
		fmt.Printf("Commiting \n")
		clients.Commit(transactionId, serverList)
		return
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
		err := clients.Clients[serverIdx].rpcClient.Call("KVService.Commit", &request, &response)
		if err != nil {
			log.Fatal(err)
		}
	}
	// time.Sleep(1 * time.Second)
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
		err := clients.Clients[serverIdx].rpcClient.Call("KVService.Abort", &request, &response)
		if err != nil {
			log.Fatal(err)
		}

	}
	// time.Sleep(1 * time.Second)
}

func getTotal(addrs []string) {
	clients := Clients{Clients: []*Client{}}
	for _, addr := range addrs {
		client := Dial(addr)
		clients.Clients = append(clients.Clients, client)
	}
	sum := 0
	for i := 0; i < numberOfAccountsperClient; i++ {
		key := fmt.Sprintf("%d", numberOfAccountsperClient-1-i)
		fmt.Printf("Getting sum for key %s\n", key)
		value := clients.Clients[getHostForKey(key, len(clients.Clients))].getSum(key)
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

func runClient(clientId int, addrs []string, done *atomic.Bool) {
	clients := Clients{Clients: []*Client{}}
	for _, addr := range addrs {
		client := Dial(addr)
		clients.Clients = append(clients.Clients, client)
	}

	const batchSize = 1024

	for !done.Load() {
		for j := 0; j < batchSize; j++ {
			src := rand.Intn(numberOfAccountsperClient)
			dst := rand.Intn(numberOfAccountsperClient)
			if src == dst {
				dst = (dst + 1) % numberOfAccountsperClient
			}
			amountToTransfer := rand.Intn(20)

			//Begin Transaction
			clients.Begin(clientId, src, dst, amountToTransfer)
		}
	}
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
	secs := flag.Int("secs", 3, "Duration in seconds for each client to run")
	clientID := flag.Int("clientid", -1, "Relative client ID starting at 0")
	reqBatchsize = uint32(*flag.Uint64("batch-size", 8, "Batch for Get Requests"))
	workloadsPerHost = uint32(*flag.Uint64("thrds-per-host", 8, "Number of go routines per hosts"))
	numberOfAccountsperClient = *flag.Int("accounts-per-client", 10, "Number of accounts each client manages")

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

	//start := time.Now()

	done := atomic.Bool{}

	for j := 0; j < numberOfAccountsperClient; j++ {
		go func(clientId int) {
			// workload := kvs.NewWorkload(*workload, *theta)
			runClient(clientId, hosts, &done)
		}(*clientID)
	}

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)

	time.Sleep(1 * time.Second) // wait one second
	getTotal(hosts)
	time.Sleep(1 * time.Second) // wait one second

}
