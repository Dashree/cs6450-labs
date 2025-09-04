package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

const reqBatchsize = 16

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

func (client *Client) Get(key []string) []string {
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

func runClient(id int, addr string, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64) {
	client := Dial(addr)

	value := strings.Repeat("x", 128)
	const batchSize = 1024

	opsCompleted := uint64(0)
	var reqBatch []string

	for !done.Load() {
		for j := 0; j < batchSize; j++ {
			op := workload.Next()
			key := fmt.Sprintf("%d", op.Key)
			if op.IsRead {
				reqBatch = append(reqBatch, key)
				if len(reqBatch) >= reqBatchsize {
					client.Get(reqBatch)
					reqBatch = nil
				}
			} else {
				if len(reqBatch) > 0 {
					client.Get(reqBatch)
					reqBatch = nil
				}
				client.Put(key, value)
			}
			opsCompleted++
		}
		client.Get(reqBatch)
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

	var numberOfClientsPerHost = runtime.NumCPU() * 4
	for _, host := range hosts {
		for j := 0; j < numberOfClientsPerHost; j++ {
			go func(clientId int) {
				workload := kvs.NewWorkload(*workload, *theta)
				runClient(clientId, host, &done, workload, resultsCh)
			}(*clientID)
		}
	}

	time.Sleep(time.Duration(*secs) * time.Second)
	done.Store(true)

	totalWorkloads := len(hosts) * numberOfClientsPerHost
	for i := 0; i < totalWorkloads; i++ {
		tltOpsCompleted += <-resultsCh
	}

	elapsed := time.Since(start)

	opsPerSec := float64(tltOpsCompleted) / elapsed.Seconds()
	fmt.Printf("throughput %.2f ops/s\n", opsPerSec)
}
