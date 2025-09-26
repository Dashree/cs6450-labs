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

type CommitRequest struct {
	TransactionId uint32
}

type CommitResponse struct {
	Ack bool
}

type AbortRequest struct {
	TransactionId uint32
}

type AbortResponse struct {
	Ack bool
}

type GetSumRequest struct {
	Key string
}

type GetSumResponse struct {
	Value string
}

type TransactionOperation struct {
	IsRead bool
	Key    string
	Value  string
}
