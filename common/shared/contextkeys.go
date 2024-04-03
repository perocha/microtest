package shared

// OperationIDKey represents the key type for the operation ID in context
type OperationIDKey string

const (
	// OperationIDKeyContextKey is the key used to store the operation ID in context
	OperationIDKeyContextKey OperationIDKey = "operationID"
)
