package main

import (
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

// ---------------- Hash helpers ----------------

func shardIdxByHosts(key string, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(n))
}

// ---------------- Dial helpers (robust) ----------------

func mustDialHTTP(addr string) *rpc.Client {
	backoff := 100 * time.Millisecond
	deadline := time.Now().Add(90 * time.Second)
	for {
		if c, err := rpc.DialHTTP("tcp", addr); err == nil {
			return c
		}
		if time.Now().After(deadline) {
			log.Fatalf("kvsclient: timed out dialing %s", addr)
		}
		time.Sleep(backoff)
		// cap backoff
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}

// ---------------- Client with transactional API ----------------

type Client struct {
	hosts []string
	conns map[string]*rpc.Client

	clientID uint64

	// active tx
	active       bool
	txid         string
	participants map[string]bool // addr set
}

var globalSeq uint64

func DialAll(hosts []string, clientID uint64) *Client {
	conns := make(map[string]*rpc.Client, len(hosts))
	for _, addr := range hosts {
		conns[addr] = mustDialHTTP(addr)
	}
	return &Client{
		hosts:    hosts,
		conns:    conns,
		clientID: clientID,
	}
}

func (c *Client) newTxid() string {
	seq := atomic.AddUint64(&globalSeq, 1)
	return fmt.Sprintf("c%d-%d-%d", c.clientID, time.Now().UnixNano(), seq)
}

func (c *Client) Begin() {
	if c.active {
		log.Fatal("Begin() called while tx active")
	}
	c.txid = c.newTxid()
	c.participants = make(map[string]bool)
	c.active = true
}

func (c *Client) mustActive() {
	if !c.active {
		log.Fatal("Get/Put/Commit/Abort called without Begin()")
	}
}

func (c *Client) connForKey(key string) (addr string, rc *rpc.Client) {
	idx := shardIdxByHosts(key, len(c.hosts))
	addr = c.hosts[idx]
	rc = c.conns[addr]
	return
}

func (c *Client) Get(key string) (string, bool) {
	c.mustActive()
	addr, rc := c.connForKey(key)
	req := kvs.GetRequest{ClientID: c.clientID, Txid: c.txid, Key: key}
	var resp kvs.GetResponse
	if err := rc.Call("KVService.Get", &req, &resp); err != nil {
		log.Fatal(err)
	}
	if !resp.Granted {
		c.Abort() // implicit abort on lock denial
		return "", false
	}
	c.participants[addr] = true
	return resp.Value, true
}

func (c *Client) Put(key, value string) bool {
	c.mustActive()
	addr, rc := c.connForKey(key)
	req := kvs.PutRequest{ClientID: c.clientID, Txid: c.txid, Key: key, Value: value}
	var resp kvs.PutResponse
	if err := rc.Call("KVService.Put", &req, &resp); err != nil {
		log.Fatal(err)
	}
	if !resp.Granted {
		c.Abort()
		return false
	}
	c.participants[addr] = true
	return true
}

func (c *Client) Commit() {
	c.mustActive()
	for addr := range c.participants {
		rc := c.conns[addr]
		req := kvs.CommitRequest{ClientID: c.clientID, Txid: c.txid}
		var resp kvs.CommitResponse
		if err := rc.Call("KVService.Commit", &req, &resp); err != nil {
			log.Fatal(err)
		}
	}
	c.active = false
}

func (c *Client) Abort() {
	if !c.active {
		return
	}
	for addr := range c.participants {
		rc := c.conns[addr]
		req := kvs.AbortRequest{ClientID: c.clientID, Txid: c.txid}
		var resp kvs.AbortResponse
		if err := rc.Call("KVService.Abort", &req, &resp); err != nil {
			log.Fatal(err)
		}
	}
	c.active = false
}

// ---------------- Workloads ----------------

type HostList []string

func (h *HostList) String() string { return strings.Join(*h, ",") }
func (h *HostList) Set(v string) error {
	*h = strings.Split(v, ",")
	return nil
}

type txOp struct {
	get bool
	key string
}

// YCSB-B: 3 ops per tx, retry AS IS on abort
func runYCSBClients(hosts []string, theta float64, secs int, resultsCh chan<- uint64, id int) {
	client := DialAll(hosts, uint64(id))
	wl := kvs.NewWorkload("YCSB-B", theta)
	value := strings.Repeat("x", 128)

	opsCompleted := uint64(0)
	deadline := time.Now().Add(time.Duration(secs) * time.Second)

	for time.Now().Before(deadline) {
		// sample 3 ops AS IS
		ops := make([]txOp, 3)
		for i := 0; i < 3; i++ {
			op := wl.Next()
			k := fmt.Sprintf("%d", op.Key)
			ops[i] = txOp{get: op.IsRead, key: k}
		}

		// retry loop for this tx
		for {
			client.Begin()
			aborted := false

			for _, op := range ops {
				if op.get {
					if _, ok := client.Get(op.key); !ok {
						aborted = true
						break
					}
				} else {
					if ok := client.Put(op.key, value); !ok {
						aborted = true
						break
					}
				}
				opsCompleted++
			}

			if aborted {
				// Abort already sent; retry AS IS
				continue
			}
			client.Commit()
			break
		}
	}
	resultsCh <- opsCompleted
}

// Payment / strict-serializable workload ("xfer")
func runXferClients(hosts []string, secs int, id int, resultsCh chan<- uint64) {
	client := DialAll(hosts, uint64(id))
	opsCompleted := uint64(0)

	acctKey := func(i int) string { return fmt.Sprintf("acct:%d", i) }

	// client 0 initializes and sets start flag
	if id == 0 {
		client.Begin()
		for i := 0; i < 10; i++ {
			client.Put(acctKey(i), "1000")
			opsCompleted++
		}
		client.Commit()

		// set start flag
		client.Begin()
		client.Put("__start__", "1")
		client.Commit()
	}

	// wait for start flag
	for {
		client.Begin()
		v, ok := client.Get("__start__")
		if ok && v == "1" {
			client.Commit()
			break
		}
		client.Abort()
		time.Sleep(50 * time.Millisecond)
	}

	deadline := time.Now().Add(time.Duration(secs) * time.Second)
	for time.Now().Before(deadline) {
		src := id % 10
		dst := (id + 1) % 10

		// transfer tx: debit src, credit dst
		for {
			client.Begin()

			sBalStr, ok := client.Get(acctKey(src))
			if !ok {
				continue // aborted, retry AS IS
			}
			sBal, _ := strconv.Atoi(sBalStr)
			if sBal < 100 {
				client.Abort()
				break // nothing to do now; try again next loop
			}

			dBalStr, ok := client.Get(acctKey(dst))
			if !ok {
				continue
			}
			dBal, _ := strconv.Atoi(dBalStr)

			if ok := client.Put(acctKey(src), strconv.Itoa(sBal-100)); !ok {
				continue
			}
			if ok := client.Put(acctKey(dst), strconv.Itoa(dBal+100)); !ok {
				continue
			}

			opsCompleted += 4 // two gets + two puts
			client.Commit()
			break
		}

		// occasionally check invariant (sum==10000)
		if (opsCompleted % 200) == 0 {
			for {
				client.Begin()
				total := 0
				okAll := true
				for i := 0; i < 10; i++ {
					v, ok := client.Get(acctKey(i))
					if !ok {
						okAll = false
						break
					}
					n, _ := strconv.Atoi(v)
					total += n
					opsCompleted++
				}
				if !okAll {
					continue
				}
				client.Commit()
				if total != 10000 {
					log.Fatalf("Invariant violated: total=%d (expected 10000)", total)
				}
				break
			}
		}
	}
	resultsCh <- opsCompleted
}

// ---------------- main ----------------

func main() {
	rand.Seed(time.Now().UnixNano())

	var hosts HostList
	flag.Var(&hosts, "hosts", "Comma-separated list of host:ports to connect to")

	theta := flag.Float64("theta", 0.99, "Zipfian distribution skew parameter (used by YCSB workloads)")
	workloadName := flag.String("workload", "xfer", "Workload type: YCSB-B | xfer")
	secs := flag.Int("secs", 30, "Duration in seconds for clients to run")
	clientID := flag.Int("clientid", 0, "Relative client ID (set by run-cluster.sh)")

	flag.Parse()

	// Make this work out-of-the-box with run-cluster.sh (no extra args):
	if len(hosts) == 0 {
		if s := os.Getenv("KVS_HOSTS"); s != "" {
			for _, h := range strings.Split(s, ",") {
				hosts = append(hosts, strings.TrimSpace(h))
			}
		} else {
			// Fallback to common cluster names; your script uses node0,node1
			hosts = HostList{"node0:8080", "node1:8080"}
		}
	}
	fmt.Printf("kvsclient: connecting to hosts %v (workload=%s secs=%d id=%d)\n",
		hosts, *workloadName, *secs, *clientID)

	start := time.Now()
	resultsCh := make(chan uint64)
	total := uint64(0)

	switch *workloadName {
	case "xfer":
		for i := 0; i < 10; i++ {
			id := i
			go runXferClients(hosts, *secs, id, resultsCh)
		}
		for i := 0; i < 10; i++ {
			total += <-resultsCh
		}
	default:
		clientsPerHost := runtime.NumCPU() * 4
		for range hosts {
			for j := 0; j < clientsPerHost; j++ {
				id := j + (*clientID * 100000) // keep unique-ish ids across nodes
				go runYCSBClients(hosts, *theta, *secs, resultsCh, id)
			}
		}
		totalClients := len(hosts) * clientsPerHost
		for i := 0; i < totalClients; i++ {
			total += <-resultsCh
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("throughput %.2f ops/s\n", float64(total)/elapsed.Seconds())
}
