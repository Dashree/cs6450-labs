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

// Batch operation types
type OperationType int

const (
	OpGet OperationType = iota
	OpPut
)

// Individual operation within a batch
type Operation struct {
	Type  OperationType
	Key   string
	Value string // Only used for PUT operations
}

// Batch request containing multiple operations
type BatchRequest struct {
	Operations []Operation
}

// Batch response containing results for all operations
type BatchResponse struct {
	Values []string // Results for GET operations, empty for PUT operations
	Errors []error  // Any errors that occurred during processing
}
