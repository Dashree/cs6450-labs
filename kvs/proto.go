package kvs

type PutRequest struct {
	Key   uint64
	Value string
}

type PutResponse struct {
}

type GetRequest struct {
	Key []uint64
}

type GetResponse struct {
	Value []string
}
