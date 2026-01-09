package helper

import (
	"context"
	"fmt"
	"reflect"

	"cloud.google.com/go/datastore"
	"github.com/AndroX7/gostore/builder"
)

// Helper provides utility functions for Datastore operations
type Helper struct {
	client *datastore.Client
}

// NewHelper creates a new helper instance
func NewHelper(client *datastore.Client) *Helper {
	return &Helper{client: client}
}

// GetByID retrieves entity by ID
func (h *Helper) GetByID(ctx context.Context, kind string, id interface{}, dest interface{}) error {
	var key *datastore.Key

	switch v := id.(type) {
	case string:
		key = datastore.NameKey(kind, v, nil)
	case int64:
		key = datastore.IDKey(kind, v, nil)
	default:
		return fmt.Errorf("invalid ID type: %T", id)
	}

	return h.client.Get(ctx, key, dest)
}

// GetMulti retrieves multiple entities by IDs
func (h *Helper) GetMulti(ctx context.Context, kind string, ids []interface{}, dest interface{}) error {
	keys := make([]*datastore.Key, len(ids))

	for i, id := range ids {
		switch v := id.(type) {
		case string:
			keys[i] = datastore.NameKey(kind, v, nil)
		case int64:
			keys[i] = datastore.IDKey(kind, v, nil)
		default:
			return fmt.Errorf("invalid ID type at index %d: %T", i, id)
		}
	}

	return h.client.GetMulti(ctx, keys, dest)
}

// Create creates a new entity
func (h *Helper) Create(ctx context.Context, kind string, id interface{}, entity interface{}) error {
	var key *datastore.Key

	switch v := id.(type) {
	case string:
		key = datastore.NameKey(kind, v, nil)
	case int64:
		key = datastore.IDKey(kind, v, nil)
	case nil:
		// Auto-generate ID
		key = datastore.IncompleteKey(kind, nil)
	default:
		return fmt.Errorf("invalid ID type: %T", id)
	}

	_, err := h.client.Put(ctx, key, entity)
	return err
}

// CreateMulti creates multiple entities
func (h *Helper) CreateMulti(ctx context.Context, kind string, ids []interface{}, entities interface{}) error {
	v := reflect.ValueOf(entities)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("entities must be a slice")
	}

	keys := make([]*datastore.Key, len(ids))
	for i, id := range ids {
		switch v := id.(type) {
		case string:
			keys[i] = datastore.NameKey(kind, v, nil)
		case int64:
			keys[i] = datastore.IDKey(kind, v, nil)
		case nil:
			keys[i] = datastore.IncompleteKey(kind, nil)
		default:
			return fmt.Errorf("invalid ID type at index %d: %T", i, id)
		}
	}

	_, err := h.client.PutMulti(ctx, keys, entities)
	return err
}

// Update updates an existing entity
func (h *Helper) Update(ctx context.Context, kind string, id interface{}, entity interface{}) error {
	return h.Create(ctx, kind, id, entity) // Put works for both create and update
}

// UpdateMulti updates multiple entities
func (h *Helper) UpdateMulti(ctx context.Context, kind string, ids []interface{}, entities interface{}) error {
	return h.CreateMulti(ctx, kind, ids, entities)
}

// Delete deletes an entity
func (h *Helper) Delete(ctx context.Context, kind string, id interface{}) error {
	var key *datastore.Key

	switch v := id.(type) {
	case string:
		key = datastore.NameKey(kind, v, nil)
	case int64:
		key = datastore.IDKey(kind, v, nil)
	default:
		return fmt.Errorf("invalid ID type: %T", id)
	}

	return h.client.Delete(ctx, key)
}

// DeleteMulti deletes multiple entities
func (h *Helper) DeleteMulti(ctx context.Context, kind string, ids []interface{}) error {
	keys := make([]*datastore.Key, len(ids))

	for i, id := range ids {
		switch v := id.(type) {
		case string:
			keys[i] = datastore.NameKey(kind, v, nil)
		case int64:
			keys[i] = datastore.IDKey(kind, v, nil)
		default:
			return fmt.Errorf("invalid ID type at index %d: %T", i, id)
		}
	}

	return h.client.DeleteMulti(ctx, keys)
}

// Exists checks if entity exists
func (h *Helper) Exists(ctx context.Context, kind string, id interface{}) (bool, error) {
	var entity map[string]interface{}
	err := h.GetByID(ctx, kind, id, &entity)

	if err == datastore.ErrNoSuchEntity {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return true, nil
}

// Count counts entities matching query
func (h *Helper) Count(ctx context.Context, kind string, filters []builder.FilterParam) (int, error) {
	b := builder.New().Kind(kind)

	for _, filter := range filters {
		b.Filter(filter.Field, filter.Operator, filter.Value)
	}

	return b.Count(ctx, h.client)
}

// FindAll retrieves all entities of a kind
func (h *Helper) FindAll(ctx context.Context, kind string, dest interface{}) error {
	query := datastore.NewQuery(kind)
	_, err := h.client.GetAll(ctx, query, dest)
	return err
}

// FindWhere retrieves entities matching filters
func (h *Helper) FindWhere(ctx context.Context, kind string, filters map[string]interface{}, dest interface{}) error {
	b := builder.New().Kind(kind)

	fb := builder.NewFilter().FromMap(filters)
	for _, filter := range fb.Build() {
		b.Filter(filter.Field, filter.Operator, filter.Value)
	}

	_, err := b.Execute(ctx, h.client, dest)
	return err
}

// FindOne retrieves first entity matching filters
func (h *Helper) FindOne(ctx context.Context, kind string, filters map[string]interface{}, dest interface{}) error {
	b := builder.New().Kind(kind).Limit(1)

	fb := builder.NewFilter().FromMap(filters)
	for _, filter := range fb.Build() {
		b.Filter(filter.Field, filter.Operator, filter.Value)
	}

	query := b.Build()
	it := h.client.Run(ctx, query)

	_, err := it.Next(dest)
	return err
}

// Paginate retrieves paginated results
func (h *Helper) Paginate(ctx context.Context, kind string, filters map[string]interface{}, page, pageSize int, dest interface{}) (*builder.PaginationResult, error) {
	offset := (page - 1) * pageSize

	b := builder.New().Kind(kind).Limit(pageSize).Offset(offset)

	fb := builder.NewFilter().FromMap(filters)
	for _, filter := range fb.Build() {
		b.Filter(filter.Field, filter.Operator, filter.Value)
	}

	return b.Execute(ctx, h.client, dest)
}

// Transaction executes operations in a transaction
func (h *Helper) Transaction(ctx context.Context, fn func(tx *datastore.Transaction) error) error {
	_, err := h.client.RunInTransaction(ctx, fn)
	return err
}

// BulkCreate creates entities in batches
func (h *Helper) BulkCreate(ctx context.Context, kind string, entities interface{}, batchSize int) error {
	v := reflect.ValueOf(entities)
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("entities must be a slice")
	}

	total := v.Len()
	for i := 0; i < total; i += batchSize {
		end := i + batchSize
		if end > total {
			end = total
		}

		batch := v.Slice(i, end).Interface()
		ids := make([]interface{}, end-i)
		for j := range ids {
			ids[j] = nil // Auto-generate IDs
		}

		if err := h.CreateMulti(ctx, kind, ids, batch); err != nil {
			return err
		}
	}

	return nil
}

// BulkDelete deletes entities matching query
func (h *Helper) BulkDelete(ctx context.Context, kind string, filters map[string]interface{}) (int, error) {
	b := builder.New().Kind(kind).KeysOnly()

	fb := builder.NewFilter().FromMap(filters)
	for _, filter := range fb.Build() {
		b.Filter(filter.Field, filter.Operator, filter.Value)
	}

	query := b.Build()
	keys, err := h.client.GetAll(ctx, query, nil)
	if err != nil {
		return 0, err
	}

	if len(keys) == 0 {
		return 0, nil
	}

	if err := h.client.DeleteMulti(ctx, keys); err != nil {
		return 0, err
	}

	return len(keys), nil
}
