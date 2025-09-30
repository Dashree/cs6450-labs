package kvs

// ---------- Transactional messages (with ClientID) ----------

type GetRequest struct {
	ClientID uint64
	Txid     string
	Key      string
}

type GetResponse struct {
	ClientID uint64
	Value    string
	Granted  bool // false => lock denied (no-wait); client must Abort() & retry AS IS
}

type PutRequest struct {
	ClientID uint64
	Txid     string
	Key      string
	Value    string
}

type PutResponse struct {
	ClientID uint64
	Granted  bool // false => lock denied (no-wait)
}

type CommitRequest struct {
	ClientID uint64
	Txid     string
}

type CommitResponse struct {
	ClientID uint64
}

type AbortRequest struct {
	ClientID uint64
	Txid     string
}

type AbortResponse struct {
	ClientID uint64
}

