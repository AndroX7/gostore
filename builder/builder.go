package builder

import (
	"context"
	"fmt"

	"cloud.google.com/go/datastore"
	"google.golang.org/api/iterator"
)

// Builder constructs Datastore queries
type Builder struct {
	kind   string
	params QueryParams
}

// New creates a new query builder
func New() *Builder {
	return &Builder{
		params: QueryParams{
			Filters: make([]FilterParam, 0),
			Orders:  make([]OrderParam, 0),
		},
	}
}

// Kind sets the kind name
func (b *Builder) Kind(kind string) *Builder {
	b.kind = kind
	return b
}

// Filter adds a filter condition
func (b *Builder) Filter(field string, operator FilterOperator, value interface{}) *Builder {
	b.params.Filters = append(b.params.Filters, FilterParam{
		Field:    field,
		Operator: operator,
		Value:    value,
	})
	return b
}

// Where is an alias for Filter with Equal operator
func (b *Builder) Where(field string, value interface{}) *Builder {
	return b.Filter(field, Equal, value)
}

// WhereIn adds IN filter (multiple OR conditions)
func (b *Builder) WhereIn(field string, values []interface{}) *Builder {
	// Note: Datastore doesn't support IN operator directly
	// This would need to be split into multiple queries
	for _, v := range values {
		b.Filter(field, Equal, v)
	}
	return b
}

// WhereLike adds LIKE filter (for string contains)
func (b *Builder) WhereLike(field string, value string) *Builder {
	// Note: Datastore doesn't support LIKE
	// You need to use full-text search or implement prefix matching
	return b.Filter(field, GreaterThanOrEqual, value)
}

// Order adds ordering
func (b *Builder) Order(field string, direction OrderDirection) *Builder {
	b.params.Orders = append(b.params.Orders, OrderParam{
		Field:     field,
		Direction: direction,
	})
	return b
}

// OrderAsc adds ascending order
func (b *Builder) OrderAsc(field string) *Builder {
	return b.Order(field, Ascending)
}

// OrderDesc adds descending order
func (b *Builder) OrderDesc(field string) *Builder {
	return b.Order(field, Descending)
}

// Limit sets query limit
func (b *Builder) Limit(limit int) *Builder {
	b.params.Limit = limit
	return b
}

// Offset sets query offset
func (b *Builder) Offset(offset int) *Builder {
	b.params.Offset = offset
	return b
}

// Cursor sets start cursor for pagination
func (b *Builder) Cursor(cursor string) *Builder {
	b.params.Cursor = cursor
	return b
}

// Select specifies fields to project
func (b *Builder) Select(fields ...string) *Builder {
	b.params.Select = fields
	return b
}

// Distinct enables distinct results
func (b *Builder) Distinct() *Builder {
	b.params.Distinct = true
	return b
}

// KeysOnly retrieves only keys
func (b *Builder) KeysOnly() *Builder {
	b.params.KeysOnly = true
	return b
}

// Ancestor sets ancestor filter
func (b *Builder) Ancestor(kind string, id interface{}) *Builder {
	b.params.Ancestor = &AncestorParam{
		Kind: kind,
		ID:   id,
	}
	return b
}

// Build constructs the Datastore query
func (b *Builder) Build() *datastore.Query {
	query := datastore.NewQuery(b.kind)

	// Apply filters
	for _, filter := range b.params.Filters {
		query = query.Filter(
			fmt.Sprintf("%s %s", filter.Field, filter.Operator),
			filter.Value,
		)
	}

	// Apply ordering
	for _, order := range b.params.Orders {
		if order.Direction == Descending {
			query = query.Order("-" + order.Field)
		} else {
			query = query.Order(order.Field)
		}
	}

	// Apply limit
	if b.params.Limit > 0 {
		query = query.Limit(b.params.Limit)
	}

	// Apply offset
	if b.params.Offset > 0 {
		query = query.Offset(b.params.Offset)
	}

	// Apply cursor
	if b.params.Cursor != "" {
		if cursor, err := decodeCursor(b.params.Cursor); err == nil {
			query = query.Start(cursor)
		}
	}

	// Apply projection
	if len(b.params.Select) > 0 {
		query = query.Project(b.params.Select...)
	}

	// Apply distinct
	if b.params.Distinct {
		query = query.Distinct()
	}

	// Apply keys only
	if b.params.KeysOnly {
		query = query.KeysOnly()
	}

	// Apply ancestor
	if b.params.Ancestor != nil {
		var key *datastore.Key
		switch id := b.params.Ancestor.ID.(type) {
		case string:
			key = datastore.NameKey(b.params.Ancestor.Kind, id, nil)
		case int64:
			key = datastore.IDKey(b.params.Ancestor.Kind, id, nil)
		}
		if key != nil {
			query = query.Ancestor(key)
		}
	}

	return query
}

// Execute runs the query and returns results
func (b *Builder) Execute(ctx context.Context, client *datastore.Client, dest interface{}) (*PaginationResult, error) {
	query := b.Build()

	keys, err := client.GetAll(ctx, query, dest)
	if err != nil {
		return nil, err
	}

	pagination := &PaginationResult{
		Total:   len(keys),
		HasMore: len(keys) == b.params.Limit && b.params.Limit > 0,
	}

	return pagination, nil
}

// ExecuteWithCursor runs query and returns cursor for next page
func (b *Builder) ExecuteWithCursor(ctx context.Context, client *datastore.Client, dest interface{}) (*PaginationResult, error) {
	query := b.Build()

	it := client.Run(ctx, query)

	count := 0
	var lastCursor datastore.Cursor
	var err error

	for {
		_, err = it.Next(dest)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		count++

		// Get cursor after each iteration
		lastCursor, err = it.Cursor()
		if err != nil {
			return nil, err
		}
	}

	pagination := &PaginationResult{
		Total:   count,
		HasMore: count == b.params.Limit && b.params.Limit > 0,
	}

	// Set cursor if we have results and might have more pages
	if count > 0 && pagination.HasMore {
		pagination.NextCursor = encodeCursor(lastCursor)
	}

	return pagination, nil
}

// Count counts matching entities
func (b *Builder) Count(ctx context.Context, client *datastore.Client) (int, error) {
	// Create a copy to avoid modifying the original builder
	countBuilder := &Builder{
		kind:   b.kind,
		params: b.params,
	}
	countBuilder.KeysOnly()

	query := countBuilder.Build()

	keys, err := client.GetAll(ctx, query, nil)
	if err != nil {
		return 0, err
	}

	return len(keys), nil
}

func encodeCursor(cursor datastore.Cursor) string {
	return cursor.String()
}

func decodeCursor(s string) (datastore.Cursor, error) {
	return datastore.DecodeCursor(s)
}
