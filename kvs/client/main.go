package main

import (
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"runtime"
	"sort"
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

// ---------------- Client with transactional API ----------------

type Client struct {
	hosts []string
	conns map[string]*rpc.Client

	clientID int

	// active tx
	active       bool
	txid         string
	participants map[string]bool // addr set
}

var globalSeq uint64

func DialAll(hosts []string, clientID int) *Client {
	conns := make(map[string]*rpc.Client, len(hosts))
	for _, addr := range hosts {
		rc, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			log.Fatalf("dial %s: %v", addr, err)
		}
		conns[addr] = rc
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
	req := kvs.GetRequest{Txid: c.txid, Key: key}
	var resp kvs.GetResponse
	if err := rc.Call("KVService.Get", &req, &resp); err != nil {
		log.Fatal(err)
	}
	if !resp.Granted {
		// implicit abort (no-wait)
		c.Abort()
		return "", false
	}
	c.participants[addr] = true
	return resp.Value, true
}

func (c *Client) Put(key, value string) bool {
	c.mustActive()
	addr, rc := c.connForKey(key)
	req := kvs.PutRequest{Txid: c.txid, Key: key, Value: value}
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
	addrs := make([]string, 0, len(c.participants))
	for a := range c.participants {
		addrs = append(addrs, a)
	}
	sort.Strings(addrs) // pick deterministic lead

	for i, addr := range addrs {
		rc := c.conns[addr]
		req := kvs.CommitRequest{Txid: c.txid, Lead: i == 0}
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
	addrs := make([]string, 0, len(c.participants))
	for a := range c.participants {
		addrs = append(addrs, a)
	}
	sort.Strings(addrs) // pick deterministic lead

	for i, addr := range addrs {
		rc := c.conns[addr]
		req := kvs.AbortRequest{Txid: c.txid, Lead: i == 0}
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
	c := DialAll(hosts, id)
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
			c.Begin()
			aborted := false

			for _, op := range ops {
				if op.get {
					if _, ok := c.Get(op.key); !ok {
						aborted = true
						break
					}
				} else {
					if ok := c.Put(op.key, value); !ok {
						aborted = true
						break
					}
				}
				opsCompleted++
			}

			if aborted {
				// Abort already sent by Get/Put; just retry AS IS
				continue
			}
			c.Commit()
			break
		}
	}
	resultsCh <- opsCompleted
}

// Payment / strict-serializable workload ("xfer")
func runXferClients(hosts []string, secs int, id int, resultsCh chan<- uint64) {
	c := DialAll(hosts, id)
	opsCompleted := uint64(0)

	acctKey := func(i int) string { return fmt.Sprintf("acct:%d", i) }

	// client 0 initializes and sets start flag
	if id == 0 {
		c.Begin()
		for i := 0; i < 10; i++ {
			c.Put(acctKey(i), "1000")
			opsCompleted++
		}
		c.Commit()

		// set start flag
		c.Begin()
		c.Put("__start__", "1")
		c.Commit()
	}

	// wait for start flag
	for {
		c.Begin()
		v, ok := c.Get("__start__")
		if ok && v == "1" {
			c.Commit()
			break
		}
		c.Abort()
		time.Sleep(50 * time.Millisecond)
	}

	deadline := time.Now().Add(time.Duration(secs) * time.Second)
	for time.Now().Before(deadline) {
		src := id % 10
		dst := (id + 1) % 10

		// transfer tx: debit src, credit dst
		for {
			c.Begin()

			sBalStr, ok := c.Get(acctKey(src))
			if !ok {
				continue // aborted, retry AS IS
			}
			sBal, _ := strconv.Atoi(sBalStr)
			if sBal < 100 {
				c.Abort()
				break // nothing to do; try again later
			}

			dBalStr, ok := c.Get(acctKey(dst))
			if !ok {
				continue
			}
			dBal, _ := strconv.Atoi(dBalStr)

			// Put src (debit) and dst (credit)
			if ok := c.Put(acctKey(src), strconv.Itoa(sBal-100)); !ok {
				continue
			}
			if ok := c.Put(acctKey(dst), strconv.Itoa(dBal+100)); !ok {
				continue
			}

			opsCompleted += 4 // two gets + two puts
			c.Commit()
			break
		}

		// occasionally check invariant (sum==10000)
		if (opsCompleted%200) == 0 {
			for {
				c.Begin()
				total := 0
				okAll := true
				for i := 0; i < 10; i++ {
					v, ok := c.Get(acctKey(i))
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
				c.Commit()
				if total != 10000 {
					log.Fatalf("Invariant violated: total=%d (expected 10000)", total)
				}
			}
		}
	}
	resultsCh <- opsCompleted
}

// ---------------- main ----------------

func main() {
	var hosts HostList
	flag.Var(&hosts, "hosts", "Comma-separated list of host:ports to connect to")

	theta := flag.Float64("theta", 0.99, "Zipfian distribution skew parameter (used by YCSB workloads)")
	workloadName := flag.String("workload", "YCSB-B", "Workload type: YCSB-A | YCSB-B | YCSB-C | xfer")
	secs := flag.Int("secs", 30, "Duration in seconds for clients to run")
	clientID := flag.Int("clientid", -1, "Relative client ID starting at 0 (used in xfer)")

	flag.Parse()
	_ = clientID // keep the flag compiled-in even if not used below

	if len(hosts) == 0 {
		hosts = append(hosts, "localhost:8080")
	}

	fmt.Printf("hosts %v\ntheta %.2f\nworkload %s\nsecs %d\n",
		hosts, *theta, *workloadName, *secs)

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
				id := j
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