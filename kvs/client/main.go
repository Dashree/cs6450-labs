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
var continueAborting bool

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
	id        int
	rpcClient *rpc.Client
}

type Clients struct {
	Clients []*Client
}

func Dial(int clientID, addr string) *Client {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	return &Client{id: clientID, rpcClient: rpcClient}
}

func (c *Client) Begin() {
	// Generate a unique TxID per transaction
	c.txID = fmt.Sprintf("%d-%d", c.id, rand.Int63())
}

func (c *Client) TxnGet(key string) (string, bool) {
	request := kvs.TxnRequest{
		TxID: c.txID,
		Key:  key,
	}
	response := kvs.TxnResponse{}
	err := c.rpcClient.Call("KVService.Get", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
	return response.Value, response.Ok
}

func (c *Client) TxnPut(key, value string) bool {
	request := kvs.TxnRequest{
		TxID:  c.txID,
		Key:   key,
		Value: value,
	}
	response := kvs.TxnResponse{}
	err := c.rpcClient.Call("KVService.Put", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
	return response.Ok
}

func (c *Client) Commit(lead bool) bool {
	req := kvs.CommitRequest{
		TxID: c.txID,
		Lead: lead,
	}
	res := kvs.CommitResponse{}
	err := c.rpcClient.Call("KVService.Commit", &req, &res)
	if err != nil {
		log.Fatal(err)
	}
	return res.Ok
}

func (c *Client) Abort(lead bool) {
	req := kvs.CommitRequest{
		TxID: c.txID,
		Lead: lead,
	}
	res := kvs.CommitResponse{}
	err := c.rpcClient.Call("KVService.Abort", &req, &res)
	if err != nil {
		log.Fatal(err)
	}
}

func (c *Client) RunTransaction(ops []kvs.Operation, value string) {
	for {
		c.Begin()

		allOk := true
		for _, op := range ops {
			key := fmt.Sprintf("%d", op.Key)
			if op.IsRead {
				_, ok := c.TxnGet(key)
				if !ok {
					allOk = false
					break
				}
			} else {
				ok := c.TxnPut(key, value)
				if !ok {
					allOk = false
					break
				}
			}
		}

		if allOk {
			if c.Commit(true) {
				return // success
			}
		}

		// Transaction failed — must abort and retry
		c.Abort(true)
	}
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

func runClient(id int, addr string, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64) {
	client := Dial(id, addr)

	value := strings.Repeat("x", 128)
	opsCompleted := uint64(0)

	for !done.Load() {
		// Generate 3-op transaction
		var ops []kvs.Operation
		for i := 0; i < 3; i++ {
			op := workload.Next()
			ops = append(ops, op)
		}

		client.RunTransaction(ops, value)
		opsCompleted += 3
	}

	fmt.Printf("Client %d finished operations.\n", id)
	resultsCh <- opsCompleted
}

func (c *Client) RunXferTransaction() {
	for {
		c.Begin()

		// Each client transfers $100 from their account (src) to the next (dst)
		src := c.id
		dst := (c.id + 1) % 10

		// 1. Get the balance for src
		srcBal, ok := c.TxnGet(fmt.Sprintf("%d", src))
		if !ok || srcBal < 100 {
			c.Abort(true)
			continue
		}

		// 2. Get the balance for dst
		dstBal, ok := c.TxnGet(fmt.Sprintf("%d", dst))
		if !ok {
			c.Abort(true)
			continue
		}

		// 3. Perform the transfer
		c.TxnPut(fmt.Sprintf("%d", src), fmt.Sprintf("%d", srcBal-100)) // Decrease src balance
		c.TxnPut(fmt.Sprintf("%d", dst), fmt.Sprintf("%d", dstBal+100)) // Increase dst balance

		// 4. Commit the transaction
		if c.Commit(true) {
			return // transaction success
		}

		// If transaction fails, abort and retry
		c.Abort(true)
	}
}

func runXferClient(id int, addr string, done *atomic.Bool, resultsCh chan<- uint64) {
	client := Dial(id, addr)

	// Run transfers for a specific duration or until aborted
	for !done.Load() {
		client.RunXferTransaction()
	}

	resultsCh <- 1 // One successful transfer per iteration
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
	secs := flag.Int("secs", 8, "Duration in seconds for each client to run")
	clientID := rand.Int63()
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

	start := time.Now()
	done := atomic.Bool{}
	resultsCh := make(chan uint64)


	if isBank {
				// Run multiple clients for xfer workload
	for clientId := 0; clientId < 10; clientId++ {
		go runXferClient(clientId, host, &done, resultsCh)
	}
		} else {
		go func(clientId int) {
		workload := kvs.NewWorkload(*workload, *theta)
		runClient(clientId, host, &done, workload, resultsCh)
	}(clientId)

		}


	// Run for the specified time
	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)


	elapsed := time.Since(start)
	opsPerSec := float64(opsCompleted) / elapsed.Seconds()
	fmt.Printf("Total throughput: %.2f ops/s\n", opsPerSec)

	
	if isBank {
		// Collect results
		opsCompleted := uint64(0)
		for i := 0; i < 10; i++ {
			opsCompleted += <-resultsCh
		}
		
		// Perform final balance check
		totalBalance := int64(0)
		for i := 0; i < 10; i++ {
			balance, _ := strconv.Atoi(client.Get(fmt.Sprintf("%d", i)))
			totalBalance += int64(balance)
		}

		// Assert total balance is correct (should be $10000)
		if totalBalance == 10000 {
			fmt.Println("Total balance check passed.")
		} else {
			fmt.Println("Total balance check failed.")
		}

	} else {
		opsCompleted := <-resultsCh

		elapsed := time.Since(start)

		opsPerSec := float64(opsCompleted) / elapsed.Seconds()
		fmt.Printf("throughput %.2f ops/s\n", opsPerSec)
	}

}