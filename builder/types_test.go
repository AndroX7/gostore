package builder

import (
	"testing"
)

func TestFilterOperators(t *testing.T) {
	tests := []struct {
		name     string
		operator FilterOperator
		expected string
	}{
		{"Equal", Equal, "="},
		{"LessThan", LessThan, "<"},
		{"LessThanOrEqual", LessThanOrEqual, "<="},
		{"GreaterThan", GreaterThan, ">"},
		{"GreaterThanOrEqual", GreaterThanOrEqual, ">="},
		{"NotEqual", NotEqual, "!="},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.operator) != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, tt.operator)
			}
		})
	}
}

func TestOrderDirections(t *testing.T) {
	tests := []struct {
		name      string
		direction OrderDirection
		expected  string
	}{
		{"Ascending", Ascending, "asc"},
		{"Descending", Descending, "desc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.direction) != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, tt.direction)
			}
		})
	}
}

func TestQueryParamsCreation(t *testing.T) {
	t.Run("Create empty QueryParams", func(t *testing.T) {
		qp := QueryParams{
			Filters: make([]FilterParam, 0),
			Orders:  make([]OrderParam, 0),
		}

		if qp.Filters == nil {
			t.Error("Filters should not be nil")
		}

		if qp.Orders == nil {
			t.Error("Orders should not be nil")
		}

		if len(qp.Filters) != 0 {
			t.Errorf("expected 0 filters, got %d", len(qp.Filters))
		}
	})

	t.Run("Create QueryParams with values", func(t *testing.T) {
		qp := QueryParams{
			Filters: []FilterParam{
				{Field: "status", Operator: Equal, Value: "active"},
				{Field: "age", Operator: GreaterThan, Value: 18},
			},
			Orders: []OrderParam{
				{Field: "created_at", Direction: Descending},
			},
			Limit:  10,
			Offset: 0,
		}

		if len(qp.Filters) != 2 {
			t.Errorf("expected 2 filters, got %d", len(qp.Filters))
		}

		if len(qp.Orders) != 1 {
			t.Errorf("expected 1 order, got %d", len(qp.Orders))
		}

		if qp.Limit != 10 {
			t.Errorf("expected limit 10, got %d", qp.Limit)
		}
	})
}

func TestFilterParamCreation(t *testing.T) {
	t.Run("Create basic filter", func(t *testing.T) {
		filter := FilterParam{
			Field:    "email",
			Operator: Equal,
			Value:    "test@example.com",
		}

		if filter.Field != "email" {
			t.Errorf("expected field 'email', got '%s'", filter.Field)
		}

		if filter.Operator != Equal {
			t.Errorf("expected operator '=', got '%s'", filter.Operator)
		}

		if filter.Value != "test@example.com" {
			t.Errorf("expected value 'test@example.com', got '%v'", filter.Value)
		}
	})

	t.Run("Create filter with different operators", func(t *testing.T) {
		operators := []FilterOperator{
			Equal, LessThan, LessThanOrEqual,
			GreaterThan, GreaterThanOrEqual, NotEqual,
		}

		for _, op := range operators {
			filter := FilterParam{
				Field:    "age",
				Operator: op,
				Value:    30,
			}

			if filter.Operator != op {
				t.Errorf("expected operator '%s', got '%s'", op, filter.Operator)
			}
		}
	})
}

func TestOrderParamCreation(t *testing.T) {
	t.Run("Create ascending order", func(t *testing.T) {
		order := OrderParam{
			Field:     "name",
			Direction: Ascending,
		}

		if order.Field != "name" {
			t.Errorf("expected field 'name', got '%s'", order.Field)
		}

		if order.Direction != Ascending {
			t.Errorf("expected direction 'asc', got '%s'", order.Direction)
		}
	})

	t.Run("Create descending order", func(t *testing.T) {
		order := OrderParam{
			Field:     "created_at",
			Direction: Descending,
		}

		if order.Field != "created_at" {
			t.Errorf("expected field 'created_at', got '%s'", order.Field)
		}

		if order.Direction != Descending {
			t.Errorf("expected direction 'desc', got '%s'", order.Direction)
		}
	})
}

func TestAncestorParamCreation(t *testing.T) {
	t.Run("Create with string ID", func(t *testing.T) {
		ancestor := AncestorParam{
			Kind: "users",
			ID:   "user123",
		}

		if ancestor.Kind != "users" {
			t.Errorf("expected kind 'users', got '%s'", ancestor.Kind)
		}

		if ancestor.ID != "user123" {
			t.Errorf("expected ID 'user123', got '%v'", ancestor.ID)
		}
	})

	t.Run("Create with int64 ID", func(t *testing.T) {
		ancestor := AncestorParam{
			Kind: "posts",
			ID:   int64(12345),
		}

		if ancestor.Kind != "posts" {
			t.Errorf("expected kind 'posts', got '%s'", ancestor.Kind)
		}

		id, ok := ancestor.ID.(int64)
		if !ok || id != 12345 {
			t.Errorf("expected ID 12345, got '%v'", ancestor.ID)
		}
	})
}

func TestPaginationResultCreation(t *testing.T) {
	t.Run("Create with cursor and hasMore", func(t *testing.T) {
		pagination := PaginationResult{
			NextCursor: "cursor123",
			HasMore:    true,
			Total:      10,
		}

		if pagination.NextCursor != "cursor123" {
			t.Errorf("expected cursor 'cursor123', got '%s'", pagination.NextCursor)
		}

		if !pagination.HasMore {
			t.Error("expected HasMore to be true")
		}

		if pagination.Total != 10 {
			t.Errorf("expected total 10, got %d", pagination.Total)
		}
	})

	t.Run("Create without more results", func(t *testing.T) {
		pagination := PaginationResult{
			NextCursor: "",
			HasMore:    false,
			Total:      5,
		}

		if pagination.NextCursor != "" {
			t.Errorf("expected empty cursor, got '%s'", pagination.NextCursor)
		}

		if pagination.HasMore {
			t.Error("expected HasMore to be false")
		}
	})
}
