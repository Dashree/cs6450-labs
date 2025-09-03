package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

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

func (client *Client) Get(keys []string) []string {
	request := kvs.GetRequest{
		Keys: keys,
	}
	response := kvs.GetResponse{}
	err := client.rpcClient.Call("KVService.Get", &request, &response)

	if err != nil {
		log.Fatal(err)
	}
	return response.Values
}

func (client *Client) Put(key string, value string) {
	request := kvs.PutRequest{
		Key:   key,
		Value: value,
	}
	response := kvs.PutResponse{}
	err := client.rpcClient.Call("KVService.Put", &request, &response)
	if err != nil {
		log.Fatal(err)
	}
}

type operationCount struct {
	mu  sync.Mutex
	sum uint64
}

func (c *operationCount) incrementer(ops uint64) {
	c.mu.Lock()
	c.sum = c.sum + ops
	c.mu.Unlock()
}

func runClient(opsCount *operationCount, id int, addr string, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64) {
	client := Dial(addr)

	value := strings.Repeat("x", 128)
	const batchSize = 8

	opsCompleted := uint64(0)

	for !done.Load() {
		var keys = make([]string, 0)
		for j := 0; j < batchSize; j++ {
			op := workload.Next()
			key := fmt.Sprintf("%d", op.Key)

			if op.IsRead {
				keys = append(keys, key)
			} else {
				client.Get(keys)
				keys = make([]string, 0)
				client.Put(key, value)
				break
			}
			opsCompleted++
		}
		client.Get(keys)
	}

	fmt.Printf("Client %d finished operations.\n", id)
	opsCount.incrementer(opsCompleted)

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
	flag.Parse()

	fmt.Printf("Relative client ID: %d\n", *clientID)

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
	var opsCounter = operationCount{sum: 0}

	var wg sync.WaitGroup

	var numberOfHosts = 1
	var numberOfClientsPerHost = 32
	wg.Add(numberOfHosts * numberOfClientsPerHost)
	host := hosts[*clientID]
	for j := 0; j < numberOfClientsPerHost; j++ {
		go func(clientId int) {
			workload := kvs.NewWorkload(*workload, *theta)
			runClient(&opsCounter, clientId, host, &done, workload, resultsCh)
			wg.Done()
		}(*clientID)
	}

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)

	wg.Wait()
	opsCompleted := opsCounter.sum

	elapsed := time.Since(start)

	opsPerSec := float64(opsCompleted) / elapsed.Seconds()
	fmt.Printf("throughput %.2f ops/s\n", opsPerSec)
}
