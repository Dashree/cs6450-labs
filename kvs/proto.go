package kvs

type PutRequest struct {
	Key           string
	Value         string
	TransactionId uint32
}

type PutResponse struct {
	Yes bool
}

type GetRequest struct {
	Key           string
	TransactionId uint32
}

type GetResponse struct {
	Value string
	Yes   bool
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

type InitializeAccountRequest struct {
	Key   string
	Value string
}

type InitializeAccountResponse struct {
	Ack bool
}

type TransactionOperation struct {
	IsRead bool
	Key    string
	Value  string
}
