package builder

// QueryParams represents query parameters for Datastore
type QueryParams struct {
	Filters     []FilterParam
	Orders      []OrderParam
	Limit       int
	Offset      int
	Cursor      string
	Select      []string
	Distinct    bool
	KeysOnly    bool
	Ancestor    *AncestorParam
	Transaction bool
}

// FilterParam represents a filter condition
type FilterParam struct {
	Field    string
	Operator FilterOperator
	Value    interface{}
}

// OrderParam represents ordering
type OrderParam struct {
	Field     string
	Direction OrderDirection
}

// AncestorParam for ancestor queries
type AncestorParam struct {
	Kind string
	ID   interface{} // string or int64
}

// FilterOperator types
type FilterOperator string

const (
	Equal              FilterOperator = "="
	LessThan           FilterOperator = "<"
	LessThanOrEqual    FilterOperator = "<="
	GreaterThan        FilterOperator = ">"
	GreaterThanOrEqual FilterOperator = ">="
	NotEqual           FilterOperator = "!="
)

// OrderDirection types
type OrderDirection string

const (
	Ascending  OrderDirection = "asc"
	Descending OrderDirection = "desc"
)

// PaginationResult contains pagination info
type PaginationResult struct {
	NextCursor string
	HasMore    bool
	Total      int
}

// Response wraps query results
type Response struct {
	Data       interface{}
	Pagination *PaginationResult
	Error      error
}
