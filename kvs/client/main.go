package main

import (
	"flag"
	"fmt"
	"hash/maphash"
	"log"
	"math/rand"
	"net/rpc"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	// "github.com/google/uuid"
	"github.com/rstutsman/cs6450-labs/kvs"
)

var workloadsPerHost uint32
var numberOfAccountsperClient int


type Client struct {
	id        int
	txID      string
	rpcClient *rpc.Client
}

// NewClient creates a new client instance and connects to the server at addr.
func NewClient(id int, addr string) *Client {
	rpcClient, err := rpc.Dial("tcp", addr)
	if err != nil {
		log.Fatal("Dialing:", err)
	}
	return &Client{id: id, rpcClient: rpcClient}
}

func Dial(clientID int, addr string) *Client {
	rpcClient, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	return &Client{id: clientID, rpcClient: rpcClient}
}

// Begin starts a new transaction by generating a fresh TxID.
func (c *Client) Begin() {
	rand.Seed(time.Now().UnixNano())
	c.txID = fmt.Sprintf("%d-%d", c.id, rand.Int())
}

// TxnGet performs a transactional Get.
func (c *Client) TxnGet(key string) (int, bool) {
	request := kvs.TxnRequest{
		ClientID: fmt.Sprintf("%d", c.id),
		TxID:     c.txID,
		Key:      key,
	}
	response := kvs.TxnResponse{}
	err := c.rpcClient.Call("KVService.TxnGet", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
	val, _ := strconv.Atoi(response.Value)
	return val, response.Ok
}

// TxnPut performs a transactional Put.
func (c *Client) TxnPut(key, value string) bool {
	request := kvs.TxnRequest{
		ClientID: fmt.Sprintf("%d", c.id),
		TxID:     c.txID,
		Key:      key,
		Value:    value,
	}
	response := kvs.TxnResponse{}
	err := c.rpcClient.Call("KVService.TxnPut", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
	return response.Ok
}

// Commit attempts to commit the current transaction.
func (c *Client) Commit(lead bool) bool {
	req := kvs.CommitRequest{
		ClientID: fmt.Sprintf("%d", c.id),
		TxID:     c.txID,
		Lead:     lead,
	}
	res := kvs.CommitResponse{}
	err := c.rpcClient.Call("KVService.Commit", &req, &res)
	if err != nil {
		log.Fatal(err)
	}
	return res.Ok
}

// Abort aborts the current transaction.
func (c *Client) Abort(lead bool) {
	req := kvs.CommitRequest{
		ClientID: fmt.Sprintf("%d", c.id),
		TxID:     c.txID,
		Lead:     lead,
	}
	res := kvs.CommitResponse{}
	err := c.rpcClient.Call("KVService.Abort", &req, &res)
	if err != nil {
		log.Fatal(err)
	}
}

func (c *Client) RunTransaction(ops []kvs.WorkloadOp, value string) {
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
		if allOk && c.Commit(true) {
			return
		}
		c.Abort(true)
	}
}

func (c *Client) RunXferTransaction() {
	for {
		c.Begin()
		src := c.id
		dst := (c.id + 1) % 10

		srcBal, ok := c.TxnGet(fmt.Sprintf("%d", src))
		if !ok || srcBal < 100 {
			c.Abort(true)
			continue
		}
		dstBal, ok := c.TxnGet(fmt.Sprintf("%d", dst))
		if !ok {
			c.Abort(true)
			continue
		}

		c.TxnPut(fmt.Sprintf("%d", src), fmt.Sprintf("%d", srcBal-100))
		c.TxnPut(fmt.Sprintf("%d", dst), fmt.Sprintf("%d", dstBal+100))

		if c.Commit(true) {
			return
		}
		c.Abort(true)
	}
}


func runClient(id int, addr string, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64) {
	client := Dial(id, addr)
	value := strings.Repeat("x", 128)
	opsCompleted := uint64(0)

	for !done.Load() {
		var ops []kvs.WorkloadOp
		for i := 0; i < 3; i++ {
			op := workload.Next()
			ops = append(ops, op)
		}
		client.RunTransaction(ops, value)
		opsCompleted += 3
	}
	resultsCh <- opsCompleted
}

func runXferClient(id int, addr string, done *atomic.Bool, resultsCh chan<- uint64) {
	client := Dial(id, addr)
	count := uint64(0)
	for !done.Load() {
		client.RunXferTransaction()
		count++
	}
	resultsCh <- count
}

func getHostForKey(key string, numHosts int) int {
	var h maphash.Hash
	h.WriteString(key)
	return int(h.Sum64() % uint64(numHosts))
}
func (c *Client) getSum(keys []string) int {
	total := 0
	c.Begin()
	allOk := true

	for _, key := range keys {
		val, ok := c.TxnGet(key)
		if !ok {
			allOk = false
			break
		}
		total += val
	}

	if allOk {
		c.Commit(false)
	} else {
		c.Abort(false)
	}
	return total
}
func getTotal(addrs []string) {
	clients := []*Client{}
	for i, addr := range addrs {
		clients = append(clients, Dial(i, addr))
	}

	sum := 0
	for i := 0; i < numberOfAccountsperClient; i++ {
		key := fmt.Sprintf("%d", i)
		client := clients[getHostForKey(key, len(clients))]
		value := client.getSum([]string{key})
		fmt.Printf("Sum for account %s = %d\n", key, value)
		sum += value
	}

	fmt.Printf("Total sum across all accounts: %d\n", sum)
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
	var hosts HostList
	flag.Var(&hosts, "hosts", "Comma-separated host:port list")
	theta := flag.Float64("theta", 0.99, "Zipfian skew parameter")
	workloadName := flag.String("workload", "YCSB-B", "Workload type (YCSB-A/B/C)")
	secs := flag.Int("secs", 8, "Duration in seconds")
	isBank := flag.Bool("bank", false, "Run bank workload")
	workloadsPerHost = uint32(*flag.Uint64("thrds-per-host", 8, "Threads per host"))
	numberOfAccountsperClient = *flag.Int("accounts-per-client", 10, "Accounts per client")
	flag.Parse()

	if len(hosts) == 0 {
		hosts = append(hosts, "localhost:8080")
	}

	fmt.Printf("hosts: %v\ntheta: %.2f\nworkload: %s\nsecs: %d\n", hosts, *theta, *workloadName, *secs)

	start := time.Now()
	done := atomic.Bool{}
	resultsCh := make(chan uint64)

	for _, host := range hosts {
		for i := 0; i < int(workloadsPerHost); i++ {
			if *isBank {
				go runXferClient(i, host, &done, resultsCh)
			} else {
				workload := kvs.NewWorkload(*workloadName, *theta)
				go runClient(i, host, &done, workload, resultsCh)
			}
		}
	}

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)

	totalOps := uint64(0)
	for i := 0; i < int(workloadsPerHost)*len(hosts); i++ {
		totalOps += <-resultsCh
	}

	elapsed := time.Since(start)
	fmt.Printf("Total throughput: %.2f ops/s\n", float64(totalOps)/elapsed.Seconds())

	if *isBank {
		getTotal(hosts)
	}
}
