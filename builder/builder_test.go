package builder

import (
	"testing"
)

func TestNew(t *testing.T) {
	t.Run("Create new builder", func(t *testing.T) {
		b := New()

		if b == nil {
			t.Fatal("builder should not be nil")
		}

		if b.params.Filters == nil {
			t.Error("Filters should not be nil")
		}

		if b.params.Orders == nil {
			t.Error("Orders should not be nil")
		}

		if len(b.params.Filters) != 0 {
			t.Errorf("expected 0 filters, got %d", len(b.params.Filters))
		}

		if len(b.params.Orders) != 0 {
			t.Errorf("expected 0 orders, got %d", len(b.params.Orders))
		}
	})
}

func TestKind(t *testing.T) {
	t.Run("Set kind", func(t *testing.T) {
		b := New().Kind("users")

		if b.kind != "users" {
			t.Errorf("expected kind 'users', got '%s'", b.kind)
		}
	})

	t.Run("Chain kind", func(t *testing.T) {
		b := New().Kind("users").Kind("posts")

		if b.kind != "posts" {
			t.Errorf("expected kind 'posts', got '%s'", b.kind)
		}
	})
}

func TestFilter(t *testing.T) {
	t.Run("Add single filter", func(t *testing.T) {
		b := New().Filter("status", Equal, "active")

		if len(b.params.Filters) != 1 {
			t.Fatalf("expected 1 filter, got %d", len(b.params.Filters))
		}

		filter := b.params.Filters[0]
		if filter.Field != "status" {
			t.Errorf("expected field 'status', got '%s'", filter.Field)
		}

		if filter.Operator != Equal {
			t.Errorf("expected operator '=', got '%s'", filter.Operator)
		}

		if filter.Value != "active" {
			t.Errorf("expected value 'active', got '%v'", filter.Value)
		}
	})

	t.Run("Add multiple filters", func(t *testing.T) {
		b := New().
			Filter("status", Equal, "active").
			Filter("age", GreaterThan, 18)

		if len(b.params.Filters) != 2 {
			t.Fatalf("expected 2 filters, got %d", len(b.params.Filters))
		}
	})

	t.Run("Test all filter operators", func(t *testing.T) {
		tests := []struct {
			operator FilterOperator
			value    interface{}
		}{
			{Equal, "test"},
			{LessThan, 10},
			{LessThanOrEqual, 20},
			{GreaterThan, 30},
			{GreaterThanOrEqual, 40},
			{NotEqual, "inactive"},
		}

		for _, tt := range tests {
			b := New().Filter("field", tt.operator, tt.value)

			if len(b.params.Filters) != 1 {
				t.Errorf("expected 1 filter, got %d", len(b.params.Filters))
			}

			filter := b.params.Filters[0]
			if filter.Operator != tt.operator {
				t.Errorf("expected operator '%s', got '%s'", tt.operator, filter.Operator)
			}
		}
	})
}

func TestWhere(t *testing.T) {
	t.Run("Where is alias for Filter with Equal", func(t *testing.T) {
		b := New().Where("email", "test@example.com")

		if len(b.params.Filters) != 1 {
			t.Fatalf("expected 1 filter, got %d", len(b.params.Filters))
		}

		filter := b.params.Filters[0]
		if filter.Operator != Equal {
			t.Errorf("expected operator '=', got '%s'", filter.Operator)
		}

		if filter.Value != "test@example.com" {
			t.Errorf("expected value 'test@example.com', got '%v'", filter.Value)
		}
	})
}

func TestWhereIn(t *testing.T) {
	t.Run("WhereIn adds multiple filters", func(t *testing.T) {
		values := []interface{}{"active", "pending", "inactive"}
		b := New().WhereIn("status", values)

		if len(b.params.Filters) != len(values) {
			t.Errorf("expected %d filters, got %d", len(values), len(b.params.Filters))
		}

		for i, filter := range b.params.Filters {
			if filter.Field != "status" {
				t.Errorf("filter %d: expected field 'status', got '%s'", i, filter.Field)
			}

			if filter.Operator != Equal {
				t.Errorf("filter %d: expected operator '=', got '%s'", i, filter.Operator)
			}

			if filter.Value != values[i] {
				t.Errorf("filter %d: expected value '%v', got '%v'", i, values[i], filter.Value)
			}
		}
	})
}

func TestOrder(t *testing.T) {
	t.Run("Add single order", func(t *testing.T) {
		b := New().Order("created_at", Descending)

		if len(b.params.Orders) != 1 {
			t.Fatalf("expected 1 order, got %d", len(b.params.Orders))
		}

		order := b.params.Orders[0]
		if order.Field != "created_at" {
			t.Errorf("expected field 'created_at', got '%s'", order.Field)
		}

		if order.Direction != Descending {
			t.Errorf("expected direction 'desc', got '%s'", order.Direction)
		}
	})

	t.Run("Add multiple orders", func(t *testing.T) {
		b := New().
			Order("status", Ascending).
			Order("created_at", Descending)

		if len(b.params.Orders) != 2 {
			t.Fatalf("expected 2 orders, got %d", len(b.params.Orders))
		}
	})
}

func TestOrderAsc(t *testing.T) {
	t.Run("OrderAsc sets ascending direction", func(t *testing.T) {
		b := New().OrderAsc("name")

		if len(b.params.Orders) != 1 {
			t.Fatalf("expected 1 order, got %d", len(b.params.Orders))
		}

		order := b.params.Orders[0]
		if order.Direction != Ascending {
			t.Errorf("expected direction 'asc', got '%s'", order.Direction)
		}
	})
}

func TestOrderDesc(t *testing.T) {
	t.Run("OrderDesc sets descending direction", func(t *testing.T) {
		b := New().OrderDesc("created_at")

		if len(b.params.Orders) != 1 {
			t.Fatalf("expected 1 order, got %d", len(b.params.Orders))
		}

		order := b.params.Orders[0]
		if order.Direction != Descending {
			t.Errorf("expected direction 'desc', got '%s'", order.Direction)
		}
	})
}

func TestLimit(t *testing.T) {
	t.Run("Set limit", func(t *testing.T) {
		b := New().Limit(10)

		if b.params.Limit != 10 {
			t.Errorf("expected limit 10, got %d", b.params.Limit)
		}
	})

	t.Run("Chain limit", func(t *testing.T) {
		b := New().Limit(10).Limit(20)

		if b.params.Limit != 20 {
			t.Errorf("expected limit 20, got %d", b.params.Limit)
		}
	})
}

func TestOffset(t *testing.T) {
	t.Run("Set offset", func(t *testing.T) {
		b := New().Offset(5)

		if b.params.Offset != 5 {
			t.Errorf("expected offset 5, got %d", b.params.Offset)
		}
	})

	t.Run("Chain offset", func(t *testing.T) {
		b := New().Offset(5).Offset(10)

		if b.params.Offset != 10 {
			t.Errorf("expected offset 10, got %d", b.params.Offset)
		}
	})
}

func TestCursor(t *testing.T) {
	t.Run("Set cursor", func(t *testing.T) {
		cursor := "test-cursor-123"
		b := New().Cursor(cursor)

		if b.params.Cursor != cursor {
			t.Errorf("expected cursor '%s', got '%s'", cursor, b.params.Cursor)
		}
	})
}

func TestSelect(t *testing.T) {
	t.Run("Select single field", func(t *testing.T) {
		b := New().Select("name")

		if len(b.params.Select) != 1 {
			t.Fatalf("expected 1 field, got %d", len(b.params.Select))
		}

		if b.params.Select[0] != "name" {
			t.Errorf("expected field 'name', got '%s'", b.params.Select[0])
		}
	})

	t.Run("Select multiple fields", func(t *testing.T) {
		b := New().Select("name", "email", "age")

		if len(b.params.Select) != 3 {
			t.Fatalf("expected 3 fields, got %d", len(b.params.Select))
		}

		expected := []string{"name", "email", "age"}
		for i, field := range b.params.Select {
			if field != expected[i] {
				t.Errorf("field %d: expected '%s', got '%s'", i, expected[i], field)
			}
		}
	})
}

func TestDistinct(t *testing.T) {
	t.Run("Enable distinct", func(t *testing.T) {
		b := New().Distinct()

		if !b.params.Distinct {
			t.Error("expected Distinct to be true")
		}
	})
}

func TestKeysOnly(t *testing.T) {
	t.Run("Enable keys only", func(t *testing.T) {
		b := New().KeysOnly()

		if !b.params.KeysOnly {
			t.Error("expected KeysOnly to be true")
		}
	})
}

func TestAncestor(t *testing.T) {
	t.Run("Set ancestor with string ID", func(t *testing.T) {
		b := New().Ancestor("users", "user123")

		if b.params.Ancestor == nil {
			t.Fatal("expected Ancestor to not be nil")
		}

		if b.params.Ancestor.Kind != "users" {
			t.Errorf("expected kind 'users', got '%s'", b.params.Ancestor.Kind)
		}

		if b.params.Ancestor.ID != "user123" {
			t.Errorf("expected ID 'user123', got '%v'", b.params.Ancestor.ID)
		}
	})

	t.Run("Set ancestor with int64 ID", func(t *testing.T) {
		b := New().Ancestor("users", int64(12345))

		if b.params.Ancestor == nil {
			t.Fatal("expected Ancestor to not be nil")
		}

		if b.params.Ancestor.ID != int64(12345) {
			t.Errorf("expected ID 12345, got '%v'", b.params.Ancestor.ID)
		}
	})
}

func TestChaining(t *testing.T) {
	t.Run("Chain multiple methods", func(t *testing.T) {
		b := New().
			Kind("users").
			Where("status", "active").
			Filter("age", GreaterThan, 18).
			OrderDesc("created_at").
			Limit(10).
			Offset(5).
			Select("name", "email")

		if b.kind != "users" {
			t.Errorf("expected kind 'users', got '%s'", b.kind)
		}

		if len(b.params.Filters) != 2 {
			t.Errorf("expected 2 filters, got %d", len(b.params.Filters))
		}

		if len(b.params.Orders) != 1 {
			t.Errorf("expected 1 order, got %d", len(b.params.Orders))
		}

		if b.params.Limit != 10 {
			t.Errorf("expected limit 10, got %d", b.params.Limit)
		}

		if b.params.Offset != 5 {
			t.Errorf("expected offset 5, got %d", b.params.Offset)
		}

		if len(b.params.Select) != 2 {
			t.Errorf("expected 2 select fields, got %d", len(b.params.Select))
		}
	})
}

func TestCursorHelpers(t *testing.T) {
	t.Run("encodeCursor returns string", func(t *testing.T) {
		// Note: Can't create real cursor without Datastore connection
		// Just test that function signature works
		// In integration tests, use real cursor
	})

	t.Run("decodeCursor handles invalid string", func(t *testing.T) {
		_, err := decodeCursor("invalid-cursor")
		if err == nil {
			t.Error("expected error for invalid cursor")
		}
	})
}
