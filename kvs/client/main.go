//client-main
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

//Add a tiny TxClient wrapper
//entralizes transaction mechanics—begin/abort/commit, read-your-writes, participant tracking—so the workload loop stays simple.
type TxClient struct {
	addrs        []string
	clients      []*Client
	numHosts     int
	clientID     uint64
	tx           kvs.TxID
	writeSet     map[string]string
	participants map[int]struct{}
}

func NewTxClient(addrs []string, clientID uint64) *TxClient {
	cs := make([]*Client, 0, len(addrs))
	for _, a := range addrs {
		cs = append(cs, Dial(a))
	}
	return &TxClient{
		addrs:        addrs,
		clients:      cs,
		numHosts:     len(addrs),
		clientID:     uint64(clientID),
		writeSet:     make(map[string]string),
		participants: make(map[int]struct{}),
	}
}

func (c *TxClient) Begin() error {
	req := kvs.BeginRequest{ClientID: c.clientID}
	var resp kvs.BeginResponse
	// any server is fine for Begin; use index 0
	if err := c.clients[0].rpcClient.Call("KVService.Begin", &req, &resp); err != nil {
		return err
	}
	c.tx = resp.Tx
	c.writeSet = make(map[string]string)
	c.participants = make(map[int]struct{})
	return nil
}

func (c *TxClient) Abort() {
	if c.tx == "" { return }
	req := kvs.AbortRequest{Tx: c.tx}
	var resp kvs.AbortResponse
	for i := range c.participants {
		_ = c.clients[i].rpcClient.Call("KVService.Abort", &req, &resp)
	}
	c.tx = ""
	c.writeSet = make(map[string]string)
	c.participants = make(map[int]struct{})
}

func (c *TxClient) Commit() error {
	if c.tx == "" { return nil }
	req := kvs.CommitRequest{Tx: c.tx}
	var resp kvs.CommitResponse
	first := true
	for i := range c.participants {
		req.Lead = first
		first = false
		if err := c.clients[i].rpcClient.Call("KVService.Commit", &req, &resp); err != nil {
			return err
		}
	}
	c.tx = ""
	return nil
}

func (c *TxClient) TxGet(key string) (string, kvs.Status, error) {
	// read-your-writes locally
	if v, ok := c.writeSet[key]; ok {
		return v, kvs.StatusOK, nil
	}
	kHost := getHostForKey(key, c.numHosts)
	req := kvs.TxGetRequest{Tx: c.tx, Key: key}
	var resp kvs.TxGetResponse
	if err := c.clients[kHost].rpcClient.Call("KVService.TxGet", &req, &resp); err != nil {
		return "", kvs.StatusNotInTx, err
	}
	if resp.Status == kvs.StatusOK {
		c.participants[kHost] = struct{}{}
	}
	return resp.Value, resp.Status, nil
}

func (c *TxClient) TxPut(key, value string) (kvs.Status, error) {
	// stage locally for read-your-writes
	c.writeSet[key] = value
	kHost := getHostForKey(key, c.numHosts)
	req := kvs.TxPutRequest{Tx: c.tx, Key: key, Value: value}
	var resp kvs.TxPutResponse
	if err := c.clients[kHost].rpcClient.Call("KVService.TxPut", &req, &resp); err != nil {
		return kvs.StatusNotInTx, err
	}
	if resp.Status == kvs.StatusOK {
		c.participants[kHost] = struct{}{}
	}
	return resp.Status, nil
}
//END of wrapper

func runClient(id int, addrs []string, done *atomic.Bool, workload *kvs.Workload, resultsCh chan<- uint64) {
	// clients := []*Client{}
	// numHosts := len(addrs)
	// for _, addr := range addrs {
	// 	clients = append(clients, Dial(addr))
	// }

	// value := strings.Repeat("x", 128)
	// const batchSize = 1024

	// opsCompleted := uint64(0)

	// for !done.Load() {
	// 	for j := 0; j < batchSize; j++ {
	// 		op := workload.Next()
	// 		key := fmt.Sprintf("%d", op.Key)
	// 		kHost := getHostForKey(key, numHosts)
	// 		if op.IsRead {
	// 			clients[kHost].Get(key)

	// 		} else {
	// 			clients[kHost].Put(key, value)
	// 		}
	// 		opsCompleted++
	// 	}
	// }
	// resultsCh <- opsCompleted

	// transactional client wrapper
	//Guarantees every transaction is 3 ops and preserves the exact sequence on retry.
	// NOo-wait: any WouldBlock → Abort immediate → retry same 3 ops.
	txCli := NewTxClient(addrs, uint64(id))

	value := strings.Repeat("x", 128)
	opsCompleted := uint64(0)

	type Op struct {
		Key   string
		IsGet bool
	}

	for !done.Load() {
		// Build exactly 3 ops for this transaction
		ops := make([]Op, 0, 3)
		for len(ops) < 3 {
			op := workload.Next()
			key := fmt.Sprintf("%d", op.Key)
			ops = append(ops, Op{Key: key, IsGet: op.IsRead})
		}

		// Retry loop: replay the SAME 3 ops until they succeed
		for {
			if err := txCli.Begin(); err != nil {
				log.Fatal("Begin failed:", err)
			}

			abortAndRetry := false

			for _, o := range ops {
				if o.IsGet {
					_, st, err := txCli.TxGet(o.Key)
					if err != nil { log.Fatal("TxGet err:", err) }
					if st == kvs.StatusWouldBlock {
						txCli.Abort()
						abortAndRetry = true
						break
					}
				} else {
					st, err := txCli.TxPut(o.Key, value)
					if err != nil { log.Fatal("TxPut err:", err) }
					if st == kvs.StatusWouldBlock {
						txCli.Abort()
						abortAndRetry = true
						break
					}
				}
			}

			if abortAndRetry {
				// replay the SAME ops
				continue
			}

			// Try to commit
			if err := txCli.Commit(); err != nil {
				// conservative: abort and retry the same ops
				txCli.Abort()
				continue
			}

			// success: count exactly 3 ops
			opsCompleted += 3
			break
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

	//DEMO
	if *workload == "DEMO" {
		// Use the first host for Begin (any is fine)
		c := Dial(hosts[0])

		// 1) Begin
		br := kvs.BeginRequest{ClientID: uint64(time.Now().UnixNano())}
		var bs kvs.BeginResponse
		if err := c.rpcClient.Call("KVService.Begin", &br, &bs); err != nil {
			log.Fatal("Begin failed:", err)
		}

		// 2) TxPut("foo","bar")
		tpr := kvs.TxPutRequest{Tx: bs.Tx, Key: "foo", Value: "bar"}
		var tps kvs.TxPutResponse
		if err := c.rpcClient.Call("KVService.TxPut", &tpr, &tps); err != nil {
			log.Fatal("TxPut failed:", err)
		}
		if tps.Status != kvs.StatusOK {
			log.Fatal("TxPut status:", tps.Status)
		}

		// 3) TxGet("foo") -> should see staged "bar"
		tgr := kvs.TxGetRequest{Tx: bs.Tx, Key: "foo"}
		var tgs kvs.TxGetResponse
		if err := c.rpcClient.Call("KVService.TxGet", &tgr, &tgs); err != nil {
			log.Fatal("TxGet failed:", err)
		}
		if tgs.Status != kvs.StatusOK {
			log.Fatal("TxGet status:", tgs.Status)
		}
		fmt.Println("DEMO read within tx:", tgs.Value) // expect "bar"

		// 4) Commit (lead=true since only one participant in this demo)
		cr := kvs.CommitRequest{Tx: bs.Tx, Lead: true}
		var cs kvs.CommitResponse
		if err := c.rpcClient.Call("KVService.Commit", &cr, &cs); err != nil {
			log.Fatal("Commit failed:", err)
		}

		// 5) Plain non-tx Get should now return "bar"
		v := c.Get("foo")
		fmt.Println("DEMO committed value:", v) // expect "bar"
		return
	}
	//END DEMO


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
