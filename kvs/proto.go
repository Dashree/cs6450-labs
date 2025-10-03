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

type TxnRequest struct {
	TxID  string
	Key   string
	Value string
}

type TxnResponse struct {
	Value string
	Ok    bool
}

type CommitRequest struct {
	TxID string
	Lead bool
}

type CommitResponse struct {
	Ok bool
}

