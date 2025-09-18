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

type Commit struct {
	transactionID uint64
}

type CommitResponse struct {
}

type Abort struct {
	transactionID uint64
}

type AbortResponse struct {
}

type TransactionOperation struct {
	IsRead bool
	Key    string
	Value  string
}
