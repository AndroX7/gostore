package exec

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"cloud.google.com/go/datastore"
	"github.com/AndroX7/gostore/builder"
	contextKey "github.com/AndroX7/gostore/key"
)

// Exec provides utility functions for Datastore operations
type Exec struct {
}

// NewExec creates a new helper instance
func NewExec() *Exec {
	return &Exec{}
}

// GetByID retrieves entity by ID
func (h *Exec) GetByID(ctx context.Context, kind string, id any, dest any) error {
	var key *datastore.Key

	var client *datastore.Client
	ref := ctx.Value(contextKey.NOSQL_KEY)
	if tmp, ok := ref.(*datastore.Client); ok && tmp != nil {
		client = tmp
	} else {
		err := errors.New("database is not initialized")
		return err
	}

	switch v := id.(type) {
	case string:
		key = datastore.NameKey(kind, v, nil)
	case int64:
		key = datastore.IDKey(kind, v, nil)
	default:
		return fmt.Errorf("invalid ID type: %T", id)
	}

	return client.Get(ctx, key, dest)
}

// GetMulti retrieves multiple entities by IDs
func (h *Exec) GetMulti(ctx context.Context, kind string, ids []any, dest any) error {
	var client *datastore.Client
	ref := ctx.Value(contextKey.NOSQL_KEY)
	if tmp, ok := ref.(*datastore.Client); ok && tmp != nil {
		client = tmp
	} else {
		err := errors.New("database is not initialized")
		return err
	}

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

	return client.GetMulti(ctx, keys, dest)
}

// Create creates a new entity
func (h *Exec) Create(ctx context.Context, kind string, id any, entity any) error {

	var client *datastore.Client
	ref := ctx.Value(contextKey.NOSQL_KEY)
	if tmp, ok := ref.(*datastore.Client); ok && tmp != nil {
		client = tmp
	} else {
		err := errors.New("database is not initialized")
		return err
	}

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

	_, err := client.Put(ctx, key, entity)
	return err
}

// CreateMulti creates multiple entities
func (h *Exec) CreateMulti(ctx context.Context, kind string, ids []any, entities any) error {

	var client *datastore.Client
	ref := ctx.Value(contextKey.NOSQL_KEY)
	if tmp, ok := ref.(*datastore.Client); ok && tmp != nil {
		client = tmp
	} else {
		err := errors.New("database is not initialized")
		return err
	}

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

	_, err := client.PutMulti(ctx, keys, entities)
	return err
}

// Update updates an existing entity
func (h *Exec) Update(ctx context.Context, kind string, id any, entity any) error {
	return h.Create(ctx, kind, id, entity) // Put works for both create and update
}

// UpdateMulti updates multiple entities
func (h *Exec) UpdateMulti(ctx context.Context, kind string, ids []any, entities any) error {
	return h.CreateMulti(ctx, kind, ids, entities)
}

// Delete deletes an entity
func (h *Exec) Delete(ctx context.Context, kind string, id any) error {

	var client *datastore.Client
	ref := ctx.Value(contextKey.NOSQL_KEY)
	if tmp, ok := ref.(*datastore.Client); ok && tmp != nil {
		client = tmp
	} else {
		err := errors.New("database is not initialized")
		return err
	}

	var key *datastore.Key

	switch v := id.(type) {
	case string:
		key = datastore.NameKey(kind, v, nil)
	case int64:
		key = datastore.IDKey(kind, v, nil)
	default:
		return fmt.Errorf("invalid ID type: %T", id)
	}

	return client.Delete(ctx, key)
}

// DeleteMulti deletes multiple entities
func (h *Exec) DeleteMulti(ctx context.Context, kind string, ids []any) error {

	var client *datastore.Client
	ref := ctx.Value(contextKey.NOSQL_KEY)
	if tmp, ok := ref.(*datastore.Client); ok && tmp != nil {
		client = tmp
	} else {
		err := errors.New("database is not initialized")
		return err
	}

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

	return client.DeleteMulti(ctx, keys)
}

// Exists checks if entity exists
func (h *Exec) Exists(ctx context.Context, kind string, id any) (bool, error) {
	var entity map[string]any
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
func (h *Exec) Count(ctx context.Context, kind string, filters []builder.FilterParam) (int, error) {

	var client *datastore.Client
	ref := ctx.Value(contextKey.NOSQL_KEY)
	if tmp, ok := ref.(*datastore.Client); ok && tmp != nil {
		client = tmp
	} else {
		err := errors.New("database is not initialized")
		return 0, err
	}

	b := builder.New().Kind(kind)

	for _, filter := range filters {
		b.Filter(filter.Field, filter.Operator, filter.Value)
	}

	return b.Count(ctx, client)
}

// FindAll retrieves all entities of a kind
func (h *Exec) FindAll(ctx context.Context, kind string, dest any) error {

	var client *datastore.Client
	ref := ctx.Value(contextKey.NOSQL_KEY)
	if tmp, ok := ref.(*datastore.Client); ok && tmp != nil {
		client = tmp
	} else {
		err := errors.New("database is not initialized")
		return err
	}

	query := datastore.NewQuery(kind)
	_, err := client.GetAll(ctx, query, dest)
	return err
}

// FindWhere retrieves entities matching filters
func (h *Exec) FindWhere(ctx context.Context, kind string, filters map[string]any, dest any) error {

	var client *datastore.Client
	ref := ctx.Value(contextKey.NOSQL_KEY)
	if tmp, ok := ref.(*datastore.Client); ok && tmp != nil {
		client = tmp
	} else {
		err := errors.New("database is not initialized")
		return err
	}

	b := builder.New().Kind(kind)

	fb := builder.NewFilter().FromMap(filters)
	for _, filter := range fb.Build() {
		b.Filter(filter.Field, filter.Operator, filter.Value)
	}

	_, err := b.Execute(ctx, client, dest)
	return err
}

// FindOne retrieves first entity matching filters
func (h *Exec) FindOne(ctx context.Context, kind string, filters map[string]any, dest any) error {

	var client *datastore.Client
	ref := ctx.Value(contextKey.NOSQL_KEY)
	if tmp, ok := ref.(*datastore.Client); ok && tmp != nil {
		client = tmp
	} else {
		err := errors.New("database is not initialized")
		return err
	}

	b := builder.New().Kind(kind).Limit(1)

	fb := builder.NewFilter().FromMap(filters)
	for _, filter := range fb.Build() {
		b.Filter(filter.Field, filter.Operator, filter.Value)
	}

	query := b.Build()
	it := client.Run(ctx, query)

	_, err := it.Next(dest)
	return err
}

// Paginate retrieves paginated results
func (h *Exec) Paginate(ctx context.Context, kind string, filters map[string]any, page, pageSize int, dest any) (*builder.PaginationResult, error) {

	var client *datastore.Client
	ref := ctx.Value(contextKey.NOSQL_KEY)
	if tmp, ok := ref.(*datastore.Client); ok && tmp != nil {
		client = tmp
	} else {
		err := errors.New("database is not initialized")
		return nil, err
	}

	offset := (page - 1) * pageSize

	b := builder.New().Kind(kind).Limit(pageSize).Offset(offset)

	fb := builder.NewFilter().FromMap(filters)
	for _, filter := range fb.Build() {
		b.Filter(filter.Field, filter.Operator, filter.Value)
	}

	return b.Execute(ctx, client, dest)
}

// Transaction executes operations in a transaction
func (h *Exec) Transaction(ctx context.Context, fn func(tx *datastore.Transaction) error) error {

	var client *datastore.Client
	ref := ctx.Value(contextKey.NOSQL_KEY)
	if tmp, ok := ref.(*datastore.Client); ok && tmp != nil {
		client = tmp
	} else {
		err := errors.New("database is not initialized")
		return err
	}

	_, err := client.RunInTransaction(ctx, fn)
	return err
}

// BulkCreate creates entities in batches
func (h *Exec) BulkCreate(ctx context.Context, kind string, entities any, batchSize int) error {

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
		ids := make([]any, end-i)
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
func (h *Exec) BulkDelete(ctx context.Context, kind string, filters map[string]any) (int, error) {

	var client *datastore.Client
	ref := ctx.Value(contextKey.NOSQL_KEY)
	if tmp, ok := ref.(*datastore.Client); ok && tmp != nil {
		client = tmp
	} else {
		err := errors.New("database is not initialized")
		return 0, err
	}

	b := builder.New().Kind(kind).KeysOnly()

	fb := builder.NewFilter().FromMap(filters)
	for _, filter := range fb.Build() {
		b.Filter(filter.Field, filter.Operator, filter.Value)
	}

	query := b.Build()
	keys, err := client.GetAll(ctx, query, nil)
	if err != nil {
		return 0, err
	}

	if len(keys) == 0 {
		return 0, nil
	}

	if err := client.DeleteMulti(ctx, keys); err != nil {
		return 0, err
	}

	return len(keys), nil
}
