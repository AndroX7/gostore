# gostore

A Go library for simplified Google Cloud Datastore operations.

## Features

- Easy-to-use wrapper for Google Cloud Datastore
- Dynamic field extraction and inspection
- Support for querying and entity management
- Built-in helpers for common Datastore operations

## Installation

```bash
go get github.com/AndroX7/gostore
```

## Prerequisites

- Go 1.16 or higher
- Google Cloud Project with Datastore enabled
- Google Cloud credentials configured

## Setup

### 1. Install Google Cloud Datastore SDK

```bash
go get cloud.google.com/go/datastore
```

### 2. Set up Google Cloud Authentication

Set your Google Cloud credentials:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/credentials.json"
```

Or set your project ID:

```bash
export GOOGLE_CLOUD_PROJECT="your-project-id"
```

## Usage

### Basic Example

```go
package main

import (
    "context"
    "fmt"
    "log"

    "cloud.google.com/go/datastore"
)

func main() {
    ctx := context.Background()
    
    // Create a new Datastore client
    client, err := datastore.NewClient(ctx, "your-project-id")
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Your code here
}
```

### Fetching Entity Fields

```go
// Define your entity structure
type User struct {
    Name  string
    Email string
    Age   int
}

// Fetch an entity
key := datastore.NameKey("User", "user-123", nil)
var user User

if err := client.Get(ctx, key, &user); err != nil {
    log.Fatal(err)
}

fmt.Printf("User: %+v\n", user)
```

### Dynamic Field Inspection

Extract all fields from entities without defining a struct:

```go
query := datastore.NewQuery("User").Limit(1)

var entities []datastore.PropertyList
if _, err := client.GetAll(ctx, query, &entities); err != nil {
    log.Fatal(err)
}

if len(entities) > 0 {
    fmt.Println("Fields found:")
    for _, prop := range entities[0] {
        fmt.Printf("- %s (type: %T)\n", prop.Name, prop.Value)
    }
}
```

### Get All Unique Fields Across Entities

```go
func getAllFields(ctx context.Context, client *datastore.Client, kind string) (map[string]bool, error) {
    query := datastore.NewQuery(kind)
    
    var entities []datastore.PropertyList
    if _, err := client.GetAll(ctx, query, &entities); err != nil {
        return nil, err
    }

    fields := make(map[string]bool)
    for _, entity := range entities {
        for _, prop := range entity {
            fields[prop.Name] = true
        }
    }
    
    return fields, nil
}

// Usage
fields, err := getAllFields(ctx, client, "User")
if err != nil {
    log.Fatal(err)
}

fmt.Println("All fields:")
for field := range fields {
    fmt.Println("-", field)
}
```

### Projection Queries (Fetch Specific Fields)

```go
query := datastore.NewQuery("User").
    Project("Name", "Email").
    Filter("Age >", 18)

var results []struct {
    Name  string
    Email string
}

if _, err := client.GetAll(ctx, query, &results); err != nil {
    log.Fatal(err)
}

for _, result := range results {
    fmt.Printf("%s - %s\n", result.Name, result.Email)
}
```

### Create and Save Entities

```go
// Create a new user
newUser := User{
    Name:  "John Doe",
    Email: "john@example.com",
    Age:   30,
}

// Create a key (auto-generated ID)
key := datastore.IncompleteKey("User", nil)

// Save to Datastore
savedKey, err := client.Put(ctx, key, &newUser)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Saved with ID: %d\n", savedKey.ID)
```

### Update Entities

```go
// Fetch existing entity
key := datastore.NameKey("User", "user-123", nil)
var user User

if err := client.Get(ctx, key, &user); err != nil {
    log.Fatal(err)
}

// Update fields
user.Age = 31

// Save back to Datastore
if _, err := client.Put(ctx, key, &user); err != nil {
    log.Fatal(err)
}
```

### Delete Entities

```go
key := datastore.NameKey("User", "user-123", nil)

if err := client.Delete(ctx, key); err != nil {
    log.Fatal(err)
}
```

## Advanced Features

### Transaction Support

```go
_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
    var user User
    key := datastore.NameKey("User", "user-123", nil)
    
    if err := tx.Get(key, &user); err != nil {
        return err
    }
    
    user.Age++
    
    if _, err := tx.Put(key, &user); err != nil {
        return err
    }
    
    return nil
})

if err != nil {
    log.Fatal(err)
}
```

### Batch Operations

```go
// Batch Get
keys := []*datastore.Key{
    datastore.NameKey("User", "user-1", nil),
    datastore.NameKey("User", "user-2", nil),
}

users := make([]User, len(keys))
if err := client.GetMulti(ctx, keys, users); err != nil {
    log.Fatal(err)
}

// Batch Put
newUsers := []User{
    {Name: "Alice", Email: "alice@example.com", Age: 25},
    {Name: "Bob", Email: "bob@example.com", Age: 28},
}

keys = make([]*datastore.Key, len(newUsers))
for i := range newUsers {
    keys[i] = datastore.IncompleteKey("User", nil)
}

if _, err := client.PutMulti(ctx, keys, newUsers); err != nil {
    log.Fatal(err)
}
```

## Best Practices

1. **Always close the client** when done:
   ```go
   defer client.Close()
   ```

2. **Use contexts** for timeout and cancellation:
   ```go
   ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
   defer cancel()
   ```

3. **Index fields** used in queries and projections

4. **Batch operations** when working with multiple entities for better performance

5. **Use transactions** for operations that require consistency

## Common Issues

### Error: "could not find default credentials"

Make sure you have set up Google Cloud authentication:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/credentials.json"
```

### Error: "no such entity"

The entity with the specified key doesn't exist. Check your key and kind names.

### Projection Query Limitations

- Projected fields must be indexed
- Cannot project same field multiple times
- Limited to 1000 results without cursor pagination

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Resources

- [Google Cloud Datastore Documentation](https://cloud.google.com/datastore/docs)
- [Go Client Library Reference](https://pkg.go.dev/cloud.google.com/go/datastore)
- [Datastore Best Practices](https://cloud.google.com/datastore/docs/best-practices)

## Author

**AndroX7**

## Acknowledgments

- Thanks to the Google Cloud team for the Datastore SDK
- Inspired by the Go community's best practices

---

For questions or support, please open an issue on GitHub.
