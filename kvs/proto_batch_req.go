
package kvs

type PutRequest struct {
	Key   string
	Value string
}

type PutResponse struct{}

type GetRequest struct {
	Key string
}

type GetResponse struct {
	Value string
	Found bool
}

/*************** NEW: batch API ****************/

// BatchOp mixes reads and writes in a single ordered request.
// For reads, Value is ignored; for writes, Value is stored.
type BatchOp struct {
	Key    string
	Value  string
	IsRead bool
}

type BatchRequest struct {
	Ops []BatchOp
}

// Results[i] corresponds to Ops[i]. For writes, Found=false/Value=""; for reads,
// Found indicates presence and Value carries the value (if found).
type BatchItem struct {
	Found bool
	Value string
}

type BatchResponse struct {
	Results []BatchItem
}
