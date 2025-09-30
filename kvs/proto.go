
package kvs

// ---- Transactional messages ----

type GetRequest struct {
	Txid string
	Key  string
}

type GetResponse struct {
	Value   string
	Granted bool // false => lock denied (no-wait); client must Abort() and retry AS IS
}

type PutRequest struct {
	Txid  string
	Key   string
	Value string
}

type PutResponse struct {
	Granted bool // false => lock denied (no-wait)
}

type CommitRequest struct {
	Txid string
	Lead bool // true on exactly one participant so servers can count commit/s once
}

type CommitResponse struct{}

type AbortRequest struct {
	Txid string
	Lead bool // true on exactly one participant so servers can count abort/s once
}

type AbortResponse struct{}
