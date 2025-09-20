//proto
package kvs

type PutRequest struct {
	Key   string
	Value string
}

type PutResponse struct {
}

type GetRequest struct {
	Key string
}

type GetResponse struct {
	Value string
}

//NEW Transactional addition
type Status int

const (
	StatusOK Status = iota
	StatusWouldBlock // lock conflict (no-wait) → client must abort+retry
	StatusNotInTx    // called a tx op without Begin
)

type TxID string

type BeginRequest struct {
	ClientID uint64
}
type BeginResponse struct {
	Tx TxID
}

type CommitRequest struct {
	Tx   TxID
	Lead bool // count commit/s only on exactly one participant
}
type CommitResponse struct{}

type AbortRequest struct {
	Tx TxID
}
type AbortResponse struct{}

// Transactional Get/Put that carry the TxID

type TxGetRequest struct {
	Tx  TxID
	Key string
}
type TxGetResponse struct {
	Status Status
	Value  string
}

type TxPutRequest struct {
	Tx    TxID
	Key   string
	Value string
}
type TxPutResponse struct {
	Status Status
}
//END Transactional addition