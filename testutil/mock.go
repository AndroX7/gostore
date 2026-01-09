package testutil

import (
	"context"
	"fmt"
	"sync"

	"cloud.google.com/go/datastore"
)

// MockDatastoreClient is a mock implementation for testing
type MockDatastoreClient struct {
	mu       sync.RWMutex
	entities map[string]map[string]interface{} // kind -> id -> entity
}

// NewMockClient creates a new mock datastore client
func NewMockClient() *MockDatastoreClient {
	return &MockDatastoreClient{
		entities: make(map[string]map[string]interface{}),
	}
}

// Put stores an entity
func (m *MockDatastoreClient) Put(ctx context.Context, key *datastore.Key, entity interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	kind := key.Kind
	id := key.Name
	if id == "" {
		id = fmt.Sprintf("%d", key.ID)
	}

	if m.entities[kind] == nil {
		m.entities[kind] = make(map[string]interface{})
	}

	m.entities[kind][id] = entity
	return nil
}

// Get retrieves an entity
func (m *MockDatastoreClient) Get(ctx context.Context, key *datastore.Key, entity interface{}) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	kind := key.Kind
	id := key.Name
	if id == "" {
		id = fmt.Sprintf("%d", key.ID)
	}

	kindEntities, ok := m.entities[kind]
	if !ok {
		return datastore.ErrNoSuchEntity
	}

	stored, ok := kindEntities[id]
	if !ok {
		return datastore.ErrNoSuchEntity
	}

	// Simple copy (in real implementation, would need proper reflection)
	*entity.(*map[string]interface{}) = stored.(map[string]interface{})
	return nil
}

// Delete removes an entity
func (m *MockDatastoreClient) Delete(ctx context.Context, key *datastore.Key) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	kind := key.Kind
	id := key.Name
	if id == "" {
		id = fmt.Sprintf("%d", key.ID)
	}

	if m.entities[kind] != nil {
		delete(m.entities[kind], id)
	}

	return nil
}

// Clear removes all entities
func (m *MockDatastoreClient) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entities = make(map[string]map[string]interface{})
}

// Count returns total entities in a kind
func (m *MockDatastoreClient) Count(kind string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.entities[kind] == nil {
		return 0
	}
	return len(m.entities[kind])
}
