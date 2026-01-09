package testutil

import (
	"time"
)

// TestUser represents a test user entity
type TestUser struct {
	ID        string    `datastore:"-"`
	Email     string    `datastore:"email"`
	Name      string    `datastore:"name"`
	Age       int       `datastore:"age"`
	Status    string    `datastore:"status"`
	CreatedAt time.Time `datastore:"created_at"`
}

// TestPost represents a test post entity
type TestPost struct {
	ID        string    `datastore:"-"`
	UserID    string    `datastore:"user_id"`
	Title     string    `datastore:"title"`
	Content   string    `datastore:"content"`
	Published bool      `datastore:"published"`
	CreatedAt time.Time `datastore:"created_at"`
}

// CreateTestUsers creates sample users for testing
func CreateTestUsers() []TestUser {
	now := time.Now()
	return []TestUser{
		{
			ID:        "user1",
			Email:     "john@example.com",
			Name:      "John Doe",
			Age:       30,
			Status:    "active",
			CreatedAt: now.Add(-24 * time.Hour),
		},
		{
			ID:        "user2",
			Email:     "jane@example.com",
			Name:      "Jane Smith",
			Age:       25,
			Status:    "active",
			CreatedAt: now.Add(-12 * time.Hour),
		},
		{
			ID:        "user3",
			Email:     "bob@example.com",
			Name:      "Bob Wilson",
			Age:       35,
			Status:    "inactive",
			CreatedAt: now.Add(-48 * time.Hour),
		},
		{
			ID:        "user4",
			Email:     "alice@example.com",
			Name:      "Alice Brown",
			Age:       28,
			Status:    "active",
			CreatedAt: now,
		},
	}
}

// CreateTestPosts creates sample posts for testing
func CreateTestPosts() []TestPost {
	now := time.Now()
	return []TestPost{
		{
			ID:        "post1",
			UserID:    "user1",
			Title:     "First Post",
			Content:   "Content of first post",
			Published: true,
			CreatedAt: now.Add(-24 * time.Hour),
		},
		{
			ID:        "post2",
			UserID:    "user1",
			Title:     "Second Post",
			Content:   "Content of second post",
			Published: true,
			CreatedAt: now.Add(-12 * time.Hour),
		},
		{
			ID:        "post3",
			UserID:    "user2",
			Title:     "Draft Post",
			Content:   "Content of draft post",
			Published: false,
			CreatedAt: now,
		},
	}
}
