package kvs

type PutRequest struct {
	Key           string
	Value         string
	TransactionId uint32
	ClientId      int
}

type PutResponse struct {
	Ack bool
}

type GetRequest struct {
	Key           string
	TransactionId int
	ClientId      int
}

type GetResponse struct {
	Value string
	Ack   bool
}

type CommitRequest struct {
	ClientId      int
	TransactionId uint32
	Lead          bool
}

type CommitResponse struct {
	Ack bool
}

type AbortRequest struct {
	ClientId      int
	TransactionId uint32
	Lead          bool
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
