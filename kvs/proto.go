package kvs

type PutRequest struct {
	Key   string
	Value string
}

type PutResponse struct {
}

type GetRequest struct {
	Keys []string
}

type GetResponse struct {
	Values []string
}
