package kvs

type PutRequest struct {
	Key          string
	Value        string
	TrasactionId uint32
	ClientId     int
}

type PutResponse struct {
	Ack bool
}

type GetRequest struct {
	Key          string
	TrasactionId int
	ClientId     int
}

type GetResponse struct {
	Value string
	Ack   bool
}

// TxnRequest is used for both Get and Put operations in a transaction.
type TxnRequest struct {
	ClientID string // NEW: ID of the client
	TxID     string // Transaction ID
	Key      string
	Value    string // Only used in Put
}

type TxnResponse struct {
	Value string
	Ok    bool
}

// CommitRequest is used to either commit or abort a transaction.
type CommitRequest struct {
	ClientID string // NEW: ID of the client
	TxID     string
	Lead     bool
}

type CommitResponse struct {
	Ok bool
}
