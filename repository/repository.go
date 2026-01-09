package repository

import (
	"context"

	"cloud.google.com/go/datastore"
	"github.com/AndroX7/gostore/builder"
	"github.com/AndroX7/gostore/exec"
)

// Repository interface defines standard CRUD operations
type Repository interface {
	GetByID(ctx context.Context, id interface{}, dest interface{}) error
	Create(ctx context.Context, id interface{}, entity interface{}) error
	Update(ctx context.Context, id interface{}, entity interface{}) error
	Delete(ctx context.Context, id interface{}) error
	Query(ctx context.Context, params interface{}) ([]interface{}, *builder.PaginationResult, error)
	Count(ctx context.Context, filters interface{}) (int, error)
	Exists(ctx context.Context, id interface{}) (bool, error)
}

// BaseRepository implements common repository operations
type BaseRepository struct {
	client   *datastore.Client
	kind     string
	executor *exec.Exec
}

// NewBaseRepository creates a new base repository
func NewBaseRepository(client *datastore.Client, kind string) *BaseRepository {
	return &BaseRepository{
		client:   client,
		kind:     kind,
		executor: exec.NewExec(),
	}
}

// GetByID retrieves entity by ID
func (r *BaseRepository) GetByID(ctx context.Context, id interface{}, dest interface{}) error {
	return r.executor.GetByID(ctx, r.kind, id, dest)
}

// GetMulti retrieves multiple entities
func (r *BaseRepository) GetMulti(ctx context.Context, ids []interface{}, dest interface{}) error {
	return r.executor.GetMulti(ctx, r.kind, ids, dest)
}

// Create creates a new entity
func (r *BaseRepository) Create(ctx context.Context, id interface{}, entity interface{}) error {
	return r.executor.Create(ctx, r.kind, id, entity)
}

// CreateMulti creates multiple entities
func (r *BaseRepository) CreateMulti(ctx context.Context, ids []interface{}, entities interface{}) error {
	return r.executor.CreateMulti(ctx, r.kind, ids, entities)
}

// Update updates an entity
func (r *BaseRepository) Update(ctx context.Context, id interface{}, entity interface{}) error {
	return r.executor.Update(ctx, r.kind, id, entity)
}

// UpdateMulti updates multiple entities
func (r *BaseRepository) UpdateMulti(ctx context.Context, ids []interface{}, entities interface{}) error {
	return r.executor.UpdateMulti(ctx, r.kind, ids, entities)
}

// Delete deletes an entity
func (r *BaseRepository) Delete(ctx context.Context, id interface{}) error {
	return r.executor.Delete(ctx, r.kind, id)
}

// DeleteMulti deletes multiple entities
func (r *BaseRepository) DeleteMulti(ctx context.Context, ids []interface{}) error {
	return r.executor.DeleteMulti(ctx, r.kind, ids)
}

// Exists checks if entity exists
func (r *BaseRepository) Exists(ctx context.Context, id interface{}) (bool, error) {
	return r.executor.Exists(ctx, r.kind, id)
}

// Query executes a query with flexible parameters
func (r *BaseRepository) Query(ctx context.Context, params interface{}) ([]interface{}, *builder.PaginationResult, error) {
	b := builder.New().Kind(r.kind)

	// Parse params
	switch p := params.(type) {
	case *builder.QueryParams:
		return r.queryWithParams(ctx, b, p)
	case builder.QueryParams:
		return r.queryWithParams(ctx, b, &p)
	case map[string]interface{}:
		return r.queryWithMap(ctx, b, p)
	default:
		return r.queryWithStruct(ctx, b, params)
	}
}

// QueryTyped executes query and returns typed results
func (r *BaseRepository) QueryTyped(ctx context.Context, params interface{}, dest interface{}) (*builder.PaginationResult, error) {
	b := builder.New().Kind(r.kind)
	// Parse params
	switch p := params.(type) {
	case *builder.QueryParams:
		r.applyQueryParams(b, p)
	case builder.QueryParams:
		r.applyQueryParams(b, &p)
	case map[string]interface{}:
		r.applyMapParams(b, p)
	default:
		r.applyStructParams(b, params)
	}

	return b.Execute(ctx, r.client, dest)
}

// Count counts entities matching filters
func (r *BaseRepository) Count(ctx context.Context, filters interface{}) (int, error) {
	b := builder.New().Kind(r.kind)
	switch f := filters.(type) {
	case map[string]interface{}:
		fb := builder.NewFilter().FromMap(f)
		for _, filter := range fb.Build() {
			b.Filter(filter.Field, filter.Operator, filter.Value)
		}
	case []builder.FilterParam:
		for _, filter := range f {
			b.Filter(filter.Field, filter.Operator, filter.Value)
		}
	default:
		fb := builder.NewFilter().FromStruct(filters)
		for _, filter := range fb.Build() {
			b.Filter(filter.Field, filter.Operator, filter.Value)
		}
	}

	return b.Count(ctx, r.client)
}

// FindAll retrieves all entities
func (r *BaseRepository) FindAll(ctx context.Context, dest interface{}) error {
	return r.executor.FindAll(ctx, r.kind, dest)
}

// FindWhere retrieves entities matching filters
func (r *BaseRepository) FindWhere(ctx context.Context, filters map[string]interface{}, dest interface{}) error {
	return r.executor.FindWhere(ctx, r.kind, filters, dest)
}

// FindOne retrieves first matching entity
func (r *BaseRepository) FindOne(ctx context.Context, filters map[string]interface{}, dest interface{}) error {
	return r.executor.FindOne(ctx, r.kind, filters, dest)
}

// Paginate retrieves paginated results
func (r *BaseRepository) Paginate(ctx context.Context, filters map[string]interface{}, page, pageSize int, dest interface{}) (*builder.PaginationResult, error) {
	return r.executor.Paginate(ctx, r.kind, filters, page, pageSize, dest)
}

// BulkCreate creates entities in batches
func (r *BaseRepository) BulkCreate(ctx context.Context, entities interface{}, batchSize int) error {
	return r.executor.BulkCreate(ctx, r.kind, entities, batchSize)
}

// BulkDelete deletes entities matching query
func (r *BaseRepository) BulkDelete(ctx context.Context, filters map[string]interface{}) (int, error) {
	return r.executor.BulkDelete(ctx, r.kind, filters)
}

// Private helper methods
func (r *BaseRepository) queryWithParams(ctx context.Context, b *builder.Builder, params *builder.QueryParams) ([]interface{}, *builder.PaginationResult, error) {
	r.applyQueryParams(b, params)
	var results []map[string]interface{}
	pagination, err := b.Execute(ctx, r.client, &results)
	if err != nil {
		return nil, nil, err
	}

	// Convert to []interface{}
	interfaceResults := make([]interface{}, len(results))
	for i, v := range results {
		interfaceResults[i] = v
	}

	return interfaceResults, pagination, nil
}
func (r *BaseRepository) queryWithMap(ctx context.Context, b *builder.Builder, params map[string]interface{}) ([]interface{}, *builder.PaginationResult, error) {
	r.applyMapParams(b, params)
	var results []map[string]interface{}
	pagination, err := b.Execute(ctx, r.client, &results)
	if err != nil {
		return nil, nil, err
	}

	// Convert to []interface{}
	interfaceResults := make([]interface{}, len(results))
	for i, v := range results {
		interfaceResults[i] = v
	}

	return interfaceResults, pagination, nil
}
func (r *BaseRepository) queryWithStruct(ctx context.Context, b *builder.Builder, params interface{}) ([]interface{}, *builder.PaginationResult, error) {
	r.applyStructParams(b, params)
	var results []map[string]interface{}
	pagination, err := b.Execute(ctx, r.client, &results)
	if err != nil {
		return nil, nil, err
	}

	// Convert to []interface{}
	interfaceResults := make([]interface{}, len(results))
	for i, v := range results {
		interfaceResults[i] = v
	}

	return interfaceResults, pagination, nil
}
func (r *BaseRepository) applyQueryParams(b *builder.Builder, params *builder.QueryParams) {
	for _, filter := range params.Filters {
		b.Filter(filter.Field, filter.Operator, filter.Value)
	}

	for _, order := range params.Orders {
		b.Order(order.Field, order.Direction)
	}

	if params.Limit > 0 {
		b.Limit(params.Limit)
	}

	if params.Offset > 0 {
		b.Offset(params.Offset)
	}

	if params.Cursor != "" {
		b.Cursor(params.Cursor)
	}

	if len(params.Select) > 0 {
		b.Select(params.Select...)
	}

	if params.Distinct {
		b.Distinct()
	}

	if params.KeysOnly {
		b.KeysOnly()
	}

	if params.Ancestor != nil {
		b.Ancestor(params.Ancestor.Kind, params.Ancestor.ID)
	}
}
func (r *BaseRepository) applyMapParams(b *builder.Builder, params map[string]interface{}) {
	for key, value := range params {
		switch key {
		case "limit":
			if v, ok := value.(int); ok {
				b.Limit(v)
			}
		case "offset":
			if v, ok := value.(int); ok {
				b.Offset(v)
			}
		case "cursor":
			if v, ok := value.(string); ok {
				b.Cursor(v)
			}
		case "order_by":
			if v, ok := value.(string); ok {
				b.OrderAsc(v)
			}
		default:
			// Treat as filter
			b.Where(key, value)
		}
	}
}
func (r *BaseRepository) applyStructParams(b *builder.Builder, params interface{}) {
	fb := builder.NewFilter().FromStruct(params)
	for _, filter := range fb.Build() {
		b.Filter(filter.Field, filter.Operator, filter.Value)
	}
}

// GetKind returns the kind name
func (r *BaseRepository) GetKind() string {
	return r.kind
}

// GetClient returns the datastore client
func (r *BaseRepository) GetClient() *datastore.Client {
	return r.client
}
