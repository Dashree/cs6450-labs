//server-main
package main

import (
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"github.com/rstutsman/cs6450-labs/kvs"
)

var numShards uint64
var enableCache bool

type KeyHash struct {
	shardIdx uint64
	keyHash  uint64
}

type CacheKeyHasher struct {
	muCache sync.RWMutex // lock for concurrent access
	cache   map[string]*KeyHash
}

func newCache() *CacheKeyHasher {
	return &CacheKeyHasher{
		cache: make(map[string]*KeyHash, 100_000),
	}
}

func (c *CacheKeyHasher) getShardIndex(k string) *KeyHash {
	c.muCache.RLock()
	kHash, found := c.cache[k]
	c.muCache.RUnlock()
	if found {
		return kHash
	} else {
		h := fnv.New64a()
		h.Write([]byte(k))
		keyHash := h.Sum64()
		kHash := &KeyHash{shardIdx: uint64(keyHash % numShards), keyHash: keyHash}
		c.muCache.Lock()
		c.cache[k] = kHash
		c.muCache.Unlock()
		return kHash
	}

}

var keyHasherCache = newCache()

func getShardIndexCached(k string) *KeyHash {
	if enableCache {
		return keyHasherCache.getShardIndex(k)
	} else {
		h := fnv.New64a()
		h.Write([]byte(k))
		keyHash := h.Sum64()
		return &KeyHash{(keyHash % numShards), keyHash}

	}
}

type Stats struct {
	puts uint64
	gets uint64
}

func (s *Stats) Sub(prev *Stats) Stats {
	r := Stats{}
	r.puts = s.puts - prev.puts
	r.gets = s.gets - prev.gets
	return r
}

type Shard struct {
	muShard sync.RWMutex
	mp      map[uint64]string
}

type ShardMap struct {
	shards map[uint64]*Shard
}

//KeyLock tracks who’s reading/writing each key.
type KeyLock struct {
	// Set of transactions holding Shared locks (S)
	Readers map[string]struct{} // txid -> {}
	// Transaction holding the Exclusive lock (X); "" means none
	Writer string
}

//TxState tracks a transaction’s in-flight updates and which locks it owns (so we can release them fast on commit/abort)
type TxState struct {
	Status     string              // "Active","Committed","Aborted"
	WriteSet   map[uint64]string   // staged writes: keyHash -> value
	HeldSLocks map[uint64]struct{} // keys this tx holds S on (by keyHash)
	HeldXLocks map[uint64]struct{} // keys this tx holds X on (by keyHash)
}

// Constructor
func NewShardedMap(shardCount uint64, mapAllocCount uint64) *ShardMap {
	m := &ShardMap{shards: make(map[uint64]*Shard, shardCount)}
	for i := uint64(0); i < shardCount; i++ {
		m.shards[i] = &Shard{
			mp: make(map[uint64]string, mapAllocCount),
		}
	}
	return m
}

type KVService struct {
	muStatsGets sync.Mutex
	muStatsPuts sync.Mutex
	shardmp     *ShardMap
	stats       Stats
	prevStats   Stats
	lastPrint   time.Time

	prevCommits uint64
	prevAborts  uint64


	//txTable stores per-tx state on this server.
	//shardLocks is the per-key lock table for each shard.
	//commits/aborts we’ll print later for the report.
	muTx       sync.Mutex
	txTable    map[string]*TxState               // txid(string) -> TxState
	shardLocks map[uint64]map[uint64]*KeyLock    // shardIdx -> (keyHash -> KeyLock)

	muCommits sync.Mutex
	commits   uint64
	muAborts  sync.Mutex
	aborts    uint64
}

func NewKVService(shardCount uint64, mapAllocCount uint64) *KVService {
	kvs := &KVService{}
	kvs.shardmp = NewShardedMap(shardCount, mapAllocCount)
	kvs.lastPrint = time.Now()

	kvs.txTable = make(map[string]*TxState)
	kvs.shardLocks = make(map[uint64]map[uint64]*KeyLock)
	for i := uint64(0); i < shardCount; i++ {
		kvs.shardLocks[i] = make(map[uint64]*KeyLock)
	}

	return kvs
}

// Helpers(string-based)

// Return existing or create a new KeyLock for this (shardIdx, keyHash).
func (kv *KVService) getOrMakeKeyLock(shardIdx, keyHash uint64) *KeyLock {
	lk, ok := kv.shardLocks[shardIdx][keyHash]
	if !ok {
		lk = &KeyLock{Readers: make(map[string]struct{}), Writer: ""}
		kv.shardLocks[shardIdx][keyHash] = lk
	}
	return lk
}

// No-wait Shared lock attempt allowed unless a different tx holds X.
func (kv *KVService) acquireS(tx string, shardIdx, keyHash uint64) bool {
	lk := kv.getOrMakeKeyLock(shardIdx, keyHash)
	// If someone else holds X, we can't read.
	if lk.Writer != "" && lk.Writer != tx {
		return false
	}
	// Grant/record S.
	lk.Readers[tx] = struct{}{}
	return true
}

// No-wait Exclusive(X) lock attempt: allowed only if no other holders.
// If the only S holder is the same tx, upgrade is allowed.
func (kv *KVService) acquireX(tx string, shardIdx, keyHash uint64) bool {
	lk := kv.getOrMakeKeyLock(shardIdx, keyHash)

	// Another writer?
	if lk.Writer != "" && lk.Writer != tx {
		return false
	}

	// Readers present?
	if len(lk.Readers) > 0 {
		// If the sole reader is me, upgrade; else conflict.
		if len(lk.Readers) == 1 {
			if _, ok := lk.Readers[tx]; ok {
				delete(lk.Readers, tx) // upgrade
			} else {
				return false
			}
		} else {
			// Multiple readers: conflict unless they are all me (impossible), so block.
			// (We only ever add our own tx once, so len>1 implies others exist.)
			return false
		}
	}

	// Grant X.
	lk.Writer = tx
	return true
}

// Release all locks held by tx (walk the recorded sets).
//releaseAll frees every lock the tx took, so we don’t leak locks on commit/abort.
func (kv *KVService) releaseAll(tx string) {
	kv.muTx.Lock()
	st := kv.txTable[tx]
	kv.muTx.Unlock()
	if st == nil {
		return
	}
	// For both S and X, we can recompute shardIdx from keyHash (same modulo).
	for keyHash := range st.HeldSLocks {
		shIdx := keyHash % numShards
		lk := kv.getOrMakeKeyLock(shIdx, keyHash)
		delete(lk.Readers, tx)
	}
	for keyHash := range st.HeldXLocks {
		shIdx := keyHash % numShards
		lk := kv.getOrMakeKeyLock(shIdx, keyHash)
		if lk.Writer == tx {
			lk.Writer = ""
		}
	}
}

func (kv *KVService) Get(request *kvs.GetRequest, response *kvs.GetResponse) error {
	kv.muStatsGets.Lock()
	kv.stats.gets++
	kv.muStatsGets.Unlock()

	resValue := ""
	kHash := getShardIndexCached(request.Key)
	sh := kv.shardmp.shards[kHash.shardIdx]
	sh.muShard.RLock()
	val, found := sh.mp[kHash.keyHash]
	sh.muShard.RUnlock()
	if found {
		resValue = val
	}
	response.Value = resValue
	return nil
}

func (kv *KVService) Put(request *kvs.PutRequest, response *kvs.PutResponse) error {
	kv.muStatsPuts.Lock()
	kv.stats.puts++
	kv.muStatsPuts.Unlock()

	kHash := getShardIndexCached(request.Key)
	sh := kv.shardmp.shards[kHash.shardIdx]
	sh.muShard.Lock()
	sh.mp[kHash.keyHash] = request.Value
	sh.muShard.Unlock()

	// kv.shards[id].mp[request.Key] = request.Value
	return nil
}

// Transactional RPC skeletons
// Minimal 2PL + 2PC: no-wait locking during Get/Put; atomic apply on Commit; clean release on both Commit/Abort.

// Begin: create tx state and return a unique TxID.
func (kv *KVService) Begin(req *kvs.BeginRequest, resp *kvs.BeginResponse) error {
	tx := fmt.Sprintf("%d-%d", req.ClientID, time.Now().UnixNano())

	kv.muTx.Lock()
	kv.txTable[tx] = &TxState{
		Status:     "Active",
		WriteSet:   make(map[uint64]string),
		HeldSLocks: make(map[uint64]struct{}),
		HeldXLocks: make(map[uint64]struct{}),
	}
	kv.muTx.Unlock()

	resp.Tx = kvs.TxID(tx)
	return nil
}

// TxGet: try S lock, then return staged value (if any) or committed value.
func (kv *KVService) TxGet(req *kvs.TxGetRequest, resp *kvs.TxGetResponse) error {
	tx := string(req.Tx)

	kv.muTx.Lock()
	st := kv.txTable[tx]
	kv.muTx.Unlock()
	if st == nil || st.Status != "Active" {
		resp.Status = kvs.StatusNotInTx
	return nil
}

kHash := getShardIndexCached(req.Key)
	// Try to acquire S (no-wait).
	if ok := kv.acquireS(tx, kHash.shardIdx, kHash.keyHash); !ok {
		resp.Status = kvs.StatusWouldBlock
		return nil
	}
	// Record that this tx holds S on this key.
	st.HeldSLocks[kHash.keyHash] = struct{}{}

	// Read-your-writes: if we staged it in this tx, return that.
	if v, ok := st.WriteSet[kHash.keyHash]; ok {
		resp.Value = v
		resp.Status = kvs.StatusOK
		return nil
	}

	// Otherwise, read committed value.
	sh := kv.shardmp.shards[kHash.shardIdx]
	sh.muShard.RLock()
	val, found := sh.mp[kHash.keyHash]
	sh.muShard.RUnlock()
	if found {
		resp.Value = val
	}
	resp.Status = kvs.StatusOK
	return nil
}

// TxPut: try X lock, then stage the write (do not apply to main map yet).
func (kv *KVService) TxPut(req *kvs.TxPutRequest, resp *kvs.TxPutResponse) error {
	tx := string(req.Tx)

	kv.muTx.Lock()
	st := kv.txTable[tx]
	kv.muTx.Unlock()
	if st == nil || st.Status != "Active" {
		resp.Status = kvs.StatusNotInTx
		return nil
	}

	kHash := getShardIndexCached(req.Key)
	// Try to acquire X (no-wait).
	if ok := kv.acquireX(tx, kHash.shardIdx, kHash.keyHash); !ok {
		resp.Status = kvs.StatusWouldBlock
		return nil
	}
	// Record X lock and stage the value.
	st.HeldXLocks[kHash.keyHash] = struct{}{}
	st.WriteSet[kHash.keyHash] = req.Value
	resp.Status = kvs.StatusOK
	return nil
}

// Commit: apply staged writes, release locks, mark committed, count if Lead.
func (kv *KVService) Commit(req *kvs.CommitRequest, resp *kvs.CommitResponse) error {
	tx := string(req.Tx)

	kv.muTx.Lock()
	st := kv.txTable[tx]
	kv.muTx.Unlock()
	if st == nil {
		return nil
	}

	// Apply all staged writes to the real map.
	for keyHash, val := range st.WriteSet {
		shIdx := keyHash % numShards
		sh := kv.shardmp.shards[shIdx]
		sh.muShard.Lock()
		sh.mp[keyHash] = val
		sh.muShard.Unlock()
	}

	// Release locks.
	kv.releaseAll(tx)

	// Mark and remove tx state.
	kv.muTx.Lock()
	st.Status = "Committed"
	delete(kv.txTable, tx)
	kv.muTx.Unlock()

	// Count commit on the lead participant.
	if req.Lead {
		kv.muCommits.Lock()
		kv.commits++
		kv.muCommits.Unlock()
	}
	return nil
}

// Abort: drop staged writes, release locks, mark aborted, count abort.
func (kv *KVService) Abort(req *kvs.AbortRequest, resp *kvs.AbortResponse) error {
	tx := string(req.Tx)

	kv.muTx.Lock()
	st := kv.txTable[tx]
	kv.muTx.Unlock()
	if st == nil {
		return nil
	}

	kv.releaseAll(tx)

	kv.muTx.Lock()
	st.Status = "Aborted"
	delete(kv.txTable, tx)
	kv.muTx.Unlock()

	kv.muAborts.Lock()
	kv.aborts++
	kv.muAborts.Unlock()
	return nil
}
//END Transactional RPC skeletons

func (kv *KVService) printStats() {
	//gets/puts snapshot
	kv.muStatsGets.Lock()
	kv.muStatsPuts.Lock()
	stats := kv.stats
	prevStats := kv.prevStats
	kv.prevStats = stats
	now := time.Now()
	lastPrint := kv.lastPrint
	kv.lastPrint = now
	kv.muStatsPuts.Unlock()
	kv.muStatsGets.Unlock()

	//commits/aborts snapshot
	kv.muCommits.Lock()
	commits := kv.commits
	prevCommits := kv.prevCommits
	kv.prevCommits = commits
	kv.muCommits.Unlock()

	kv.muAborts.Lock()
	aborts := kv.aborts
	prevAborts := kv.prevAborts
	kv.prevAborts = aborts
	kv.muAborts.Unlock()

	diff := stats.Sub(&prevStats)
	deltaS := now.Sub(lastPrint).Seconds()

	fmt.Printf("get/s %0.2f\nput/s %0.2f\nops/s %0.2f\ncommit/s %0.2f\nabort/s %0.2f\n\n",
		float64(diff.gets)/deltaS,
		float64(diff.puts)/deltaS,
		float64(diff.gets+diff.puts)/deltaS,
		float64(commits-prevCommits)/deltaS,
		float64(aborts-prevAborts)/deltaS,
	)
}

func main() {
	port := flag.String("port", "8080", "Port to run the server on")
	numShards = *flag.Uint64("num-shards", 64, "Number of Shards in the KVStore")
	mapAllocCount := *flag.Uint64("alloc", 400_000, "Number expected for keys per shard")
	enableCache = *flag.Bool("cache", false, "Use cached values for string storage")
	flag.Parse()

	kvs := NewKVService(numShards, mapAllocCount)
	rpc.Register(kvs)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", fmt.Sprintf(":%v", *port))
	if e != nil {
		log.Fatal("listen error:", e)
	}

	fmt.Printf("Starting KVS server on :%s %t\n", *port, enableCache)

	go func() {
		for {
			kvs.printStats()
			time.Sleep(1 * time.Second)
		}
	}()

	http.Serve(l, nil)
}
