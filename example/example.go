// 6. **Usage Examples**

// examples/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/AndroX7/gostore/builder"
	"github.com/AndroX7/gostore/repository"
)

type User struct {
	ID        string    `datastore:"-"`
	Email     string    `datastore:"email"`
	Name      string    `datastore:"name"`
	Age       int       `datastore:"age"`
	Status    string    `datastore:"status"`
	CreatedAt time.Time `datastore:"created_at"`
}

func main() {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, "my-project")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Create repository
	repo := repository.NewBaseRepository(client, "users")

	// Example 1: Create user
	user := &User{
		ID:        "user123",
		Email:     "john@example.com",
		Name:      "John Doe",
		Age:       30,
		Status:    "active",
		CreatedAt: time.Now(),
	}

	if err := repo.Create(ctx, user.ID, user); err != nil {
		log.Fatal(err)
	}
	fmt.Println("✓ User created")

	// Example 2: Get by ID
	var fetchedUser User
	if err := repo.GetByID(ctx, "user123", &fetchedUser); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ User fetched: %s\n", fetchedUser.Name)

	// Example 3: Query with builder
	b := builder.New().
		Kind("users").
		Where("status", "active").
		Filter("age", builder.GreaterThanOrEqual, 18).
		OrderDesc("created_at").
		Limit(10)

	var users []User
	pagination, err := b.Execute(ctx, client, &users)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Found %d users\n", pagination.Total)

	// Example 4: Query with map
	params := map[string]interface{}{
		"status":   "active",
		"age>=":    18,
		"limit":    10,
		"order_by": "created_at",
	}

	results, _, err := repo.Query(ctx, params)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Query with map: %d results\n", len(results))

	// Example 5: Query with struct
	type UserFilter struct {
		Status string `datastore:"status"`
		Age    int    `datastore:"age"`
	}

	filter := UserFilter{
		Status: "active",
		Age:    18,
	}

	var filteredUsers []User
	_, err = repo.QueryTyped(ctx, filter, &filteredUsers)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Query with struct: %d results\n", len(filteredUsers))

	// Example 6: Complex query with filter builder
	fb := builder.NewFilter().
		Equal("status", "active").
		GreaterThanOrEqual("age", 18).
		LessThan("age", 65).
		Today("created_at")

	qp := &builder.QueryParams{
		Filters: fb.Build(),
		Orders: []builder.OrderParam{
			{Field: "created_at", Direction: builder.Descending},
		},
		Limit: 20,
	}

	var queryUsers []User
	_, err = repo.QueryTyped(ctx, qp, &queryUsers)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Complex query: %d results\n", len(queryUsers))

	// Example 7: Pagination
	var page1Users []User
	pg, err := repo.Paginate(ctx, map[string]interface{}{"status": "active"}, 1, 10, &page1Users)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Page 1: %d users, HasMore: %v\n", pg.Total, pg.HasMore)

	// Example 8: Count
	count, err := repo.Count(ctx, map[string]interface{}{"status": "active"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Total active users: %d\n", count)

	// Example 9: Bulk create
	newUsers := []User{
		{ID: "user456", Email: "jane@example.com", Name: "Jane", Age: 25, Status: "active"},
		{ID: "user789", Email: "bob@example.com", Name: "Bob", Age: 35, Status: "active"},
	}

	if err := repo.BulkCreate(ctx, newUsers, 100); err != nil {
		log.Fatal(err)
	}
	fmt.Println("✓ Bulk create completed")

	// Example 10: Bulk delete
	deleted, err := repo.BulkDelete(ctx, map[string]interface{}{"status": "inactive"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Deleted %d inactive users\n", deleted)
}

/*
This implementation provides:

✅ **Fluent builder pattern** like GOSL
✅ **Flexible query parameters** (map, struct, QueryParams)
✅ **Helper utilities** for common operations
✅ **Repository pattern** with base implementation
✅ **Filter builder** for complex queries
✅ **Pagination support** (offset and cursor-based)
✅ **Bulk operations** (create, delete)
✅ **Transaction support**
✅ **Type-safe** queries

The API is inspired by GOSL but adapted for Datastore's unique features and limitations!
*/
