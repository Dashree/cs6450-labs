package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"strings"
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

func (client *Client) Get(key string) string {
	request := kvs.GetRequest{
		Key: key,
	}
	response := kvs.GetResponse{}
	err := client.rpcClient.Call("KVService.Get", &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	return response.Value
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

// ExecuteBatch processes multiple operations in a single RPC call
func (client *Client) ExecuteBatch(operations []kvs.Operation) *kvs.BatchResponse {
	request := kvs.BatchRequest{
		Operations: operations,
	}
	response := kvs.BatchResponse{}

	err := client.rpcClient.Call("KVService.ExecuteBatch", &request, &response)
	if err != nil {
		log.Fatal(err)
	}

	return &response
}

func runClient(id int, addr string, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64) {
	client := Dial(addr)

	value := strings.Repeat("x", 128)
	const batchSize = 1024
	const rpcBatchSize = 64 // Number of operations per RPC call

	opsCompleted := uint64(0)

	for !done.Load() {
		// Process operations in batches
		for j := 0; j < batchSize; j += rpcBatchSize {
			// Determine how many operations to process in this batch
			remaining := batchSize - j
			currentBatchSize := rpcBatchSize
			if remaining < rpcBatchSize {
				currentBatchSize = remaining
			}

			// Create batch of operations
			operations := make([]kvs.Operation, currentBatchSize)
			for k := 0; k < currentBatchSize; k++ {
				op := workload.Next()
				operations[k] = kvs.Operation{
					Type:  kvs.OpGet,
					Key:   fmt.Sprintf("%d", op.Key),
					Value: "",
				}
				if !op.IsRead {
					operations[k].Type = kvs.OpPut
					operations[k].Value = value
				}
			}

			// Execute batch
			response := client.ExecuteBatch(operations)

			// Process results (for GET operations)
			for k, op := range operations {
				if op.Type == kvs.OpGet && k < len(response.Values) {
					// Store or use the retrieved value if needed
					_ = response.Values[k]
				}
			}

			opsCompleted += uint64(currentBatchSize)
		}
	}

	fmt.Printf("Client %d finished operations.\n", id)

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

	// host := hosts[0]
	// clientId := 0
	// go func(clientId int) {
	// 	workload := kvs.NewWorkload(*workload, *theta)
	// 	runClient(clientId, host, &done, workload, resultsCh)
	// }(clientId)
	for i, host := range hosts {
    go func(clientId int, h string) {
        wl := kvs.NewWorkload(*workload, *theta)
        runClient(clientId, h, &done, wl, resultsCh)
    }(i, host)
}

	// Run for N seconds, then signal all clients to stop
	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)

	// Collect one result per goroutine we launched
	var total uint64
	for i := 0; i < len(hosts); i++ {
    total += <-resultsCh
	}

	// opsCompleted := <-resultsCh
	// opsPerSec := float64(opsCompleted) / elapsed.Seconds()
	// fmt.Printf("throughput %.2f ops/s\n", opsPerSec)
	elapsed := time.Since(start)
	opsPerSec := float64(total) / elapsed.Seconds()
	fmt.Printf("throughput %.2f ops/s\n", opsPerSec)
}
