package builder

import (
	"testing"
	"time"
)

func TestNewFilter(t *testing.T) {
	t.Run("Create new filter builder", func(t *testing.T) {
		fb := NewFilter()

		if fb == nil {
			t.Fatal("filter builder should not be nil")
		}

		if fb.filters == nil {
			t.Error("filters should not be nil")
		}

		if len(fb.filters) != 0 {
			t.Errorf("expected 0 filters, got %d", len(fb.filters))
		}
	})
}

func TestFilterBuilderEqual(t *testing.T) {
	t.Run("Add equal filter", func(t *testing.T) {
		fb := NewFilter().Equal("status", "active")

		filters := fb.Build()
		if len(filters) != 1 {
			t.Fatalf("expected 1 filter, got %d", len(filters))
		}

		filter := filters[0]
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
}
func TestFilterBuilderNotEqual(t *testing.T) {
	t.Run("Add not equal filter", func(t *testing.T) {
		fb := NewFilter().NotEqual("status", "inactive")
		filters := fb.Build()
		if len(filters) != 1 {
			t.Fatalf("expected 1 filter, got %d", len(filters))
		}

		if filters[0].Operator != NotEqual {
			t.Errorf("expected operator '!=', got '%s'", filters[0].Operator)
		}
	})
}
func TestFilterBuilderGreaterThan(t *testing.T) {
	t.Run("Add greater than filter", func(t *testing.T) {
		fb := NewFilter().GreaterThan("age", 18)
		filters := fb.Build()
		if len(filters) != 1 {
			t.Fatalf("expected 1 filter, got %d", len(filters))
		}

		if filters[0].Operator != GreaterThan {
			t.Errorf("expected operator '>', got '%s'", filters[0].Operator)
		}

		if filters[0].Value != 18 {
			t.Errorf("expected value 18, got '%v'", filters[0].Value)
		}
	})
}
func TestFilterBuilderGreaterThanOrEqual(t *testing.T) {
	t.Run("Add greater than or equal filter", func(t *testing.T) {
		fb := NewFilter().GreaterThanOrEqual("age", 21)
		filters := fb.Build()
		if len(filters) != 1 {
			t.Fatalf("expected 1 filter, got %d", len(filters))
		}

		if filters[0].Operator != GreaterThanOrEqual {
			t.Errorf("expected operator '>=', got '%s'", filters[0].Operator)
		}
	})
}
func TestFilterBuilderLessThan(t *testing.T) {
	t.Run("Add less than filter", func(t *testing.T) {
		fb := NewFilter().LessThan("age", 65)
		filters := fb.Build()
		if len(filters) != 1 {
			t.Fatalf("expected 1 filter, got %d", len(filters))
		}

		if filters[0].Operator != LessThan {
			t.Errorf("expected operator '<', got '%s'", filters[0].Operator)
		}
	})
}
func TestFilterBuilderLessThanOrEqual(t *testing.T) {
	t.Run("Add less than or equal filter", func(t *testing.T) {
		fb := NewFilter().LessThanOrEqual("price", 100.00)
		filters := fb.Build()
		if len(filters) != 1 {
			t.Fatalf("expected 1 filter, got %d", len(filters))
		}

		if filters[0].Operator != LessThanOrEqual {
			t.Errorf("expected operator '<=', got '%s'", filters[0].Operator)
		}
	})
}
func TestFilterBuilderBetween(t *testing.T) {
	t.Run("Add between filter", func(t *testing.T) {
		fb := NewFilter().Between("age", 18, 65)
		filters := fb.Build()
		if len(filters) != 2 {
			t.Fatalf("expected 2 filters, got %d", len(filters))
		}

		// First filter should be >=
		if filters[0].Operator != GreaterThanOrEqual {
			t.Errorf("expected first operator '>=', got '%s'", filters[0].Operator)
		}

		if filters[0].Value != 18 {
			t.Errorf("expected first value 18, got '%v'", filters[0].Value)
		}

		// Second filter should be <=
		if filters[1].Operator != LessThanOrEqual {
			t.Errorf("expected second operator '<=', got '%s'", filters[1].Operator)
		}

		if filters[1].Value != 65 {
			t.Errorf("expected second value 65, got '%v'", filters[1].Value)
		}
	})
}
func TestFilterBuilderDateRange(t *testing.T) {
	t.Run("Add date range filter", func(t *testing.T) {
		start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		end := time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)
		fb := NewFilter().DateRange("created_at", start, end)

		filters := fb.Build()
		if len(filters) != 2 {
			t.Fatalf("expected 2 filters, got %d", len(filters))
		}
	})
}
func TestFilterBuilderToday(t *testing.T) {
	t.Run("Add today filter", func(t *testing.T) {
		fb := NewFilter().Today("created_at")
		filters := fb.Build()
		if len(filters) != 2 {
			t.Fatalf("expected 2 filters (>= and <), got %d", len(filters))
		}

		// Should have >= start of day and < start of next day
		if filters[0].Operator != GreaterThanOrEqual {
			t.Errorf("expected first operator '>=', got '%s'", filters[0].Operator)
		}

		if filters[1].Operator != LessThanOrEqual {
			t.Errorf("expected second operator '<=', got '%s'", filters[1].Operator)
		}
	})
}
func TestFilterBuilderFromStruct(t *testing.T) {

	type TestFilter struct {
		Status string `datastore:"status"`
		Age    int    `datastore:"age"`
		Email  string `datastore:"email"`
	}
	t.Run("Create filters from struct", func(t *testing.T) {
		filter := TestFilter{
			Status: "active",
			Age:    25,
			Email:  "test@example.com",
		}

		fb := NewFilter().FromStruct(filter)
		filters := fb.Build()

		if len(filters) != 3 {
			t.Fatalf("expected 3 filters, got %d", len(filters))
		}
	})

	t.Run("Skip zero values in struct", func(t *testing.T) {
		filter := TestFilter{
			Status: "active",
			// Age and Email are zero values
		}

		fb := NewFilter().FromStruct(filter)
		filters := fb.Build()

		if len(filters) != 1 {
			t.Fatalf("expected 1 filter (only Status), got %d", len(filters))
		}

		if filters[0].Field != "status" {
			t.Errorf("expected field 'status', got '%s'", filters[0].Field)
		}
	})

	t.Run("Handle pointer to struct", func(t *testing.T) {
		filter := &TestFilter{
			Status: "active",
			Age:    30,
		}

		fb := NewFilter().FromStruct(filter)
		filters := fb.Build()

		if len(filters) != 2 {
			t.Fatalf("expected 2 filters, got %d", len(filters))
		}
	})
}
func TestFilterBuilderFromMap(t *testing.T) {
	t.Run("Create filters from map with equal operator", func(t *testing.T) {
		m := map[string]interface{}{
			"status": "active",
			"age":    25,
		}
		fb := NewFilter().FromMap(m)
		filters := fb.Build()

		if len(filters) != 2 {
			t.Fatalf("expected 2 filters, got %d", len(filters))
		}
	})

	t.Run("Parse operators from map keys", func(t *testing.T) {
		m := map[string]interface{}{
			"age>=":    18,
			"age<=":    65,
			"price>":   100,
			"price<":   1000,
			"status!=": "deleted",
		}

		fb := NewFilter().FromMap(m)
		filters := fb.Build()

		if len(filters) != 5 {
			t.Fatalf("expected 5 filters, got %d", len(filters))
		}

		// Check that operators were parsed correctly
		operatorCount := make(map[FilterOperator]int)
		for _, filter := range filters {
			operatorCount[filter.Operator]++
		}

		if operatorCount[GreaterThanOrEqual] != 1 {
			t.Errorf("expected 1 '>=' operator, got %d", operatorCount[GreaterThanOrEqual])
		}

		if operatorCount[LessThanOrEqual] != 1 {
			t.Errorf("expected 1 '<=' operator, got %d", operatorCount[LessThanOrEqual])
		}

		if operatorCount[GreaterThan] != 1 {
			t.Errorf("expected 1 '>' operator, got %d", operatorCount[GreaterThan])
		}

		if operatorCount[LessThan] != 1 {
			t.Errorf("expected 1 '<' operator, got %d", operatorCount[LessThan])
		}

		if operatorCount[NotEqual] != 1 {
			t.Errorf("expected 1 '!=' operator, got %d", operatorCount[NotEqual])
		}
	})
}
func TestFilterBuilderChaining(t *testing.T) {
	t.Run("Chain multiple filter methods", func(t *testing.T) {
		now := time.Now()
		yesterday := now.Add(-24 * time.Hour)
		fb := NewFilter().
			Equal("status", "active").
			GreaterThan("age", 18).
			LessThan("age", 65).
			DateRange("created_at", yesterday, now)

		filters := fb.Build()

		// 1 Equal + 1 GreaterThan + 1 LessThan + 2 from DateRange
		if len(filters) != 5 {
			t.Fatalf("expected 5 filters, got %d", len(filters))
		}
	})
}
func TestIsZeroValue(t *testing.T) {
	t.Run("Test zero values for different types", func(t *testing.T) {
		// This tests the internal isZeroValue function behavior
		type TestStruct struct {
			StringField  string
			IntField     int
			BoolField    bool
			TimeField    time.Time
			SliceField   []string
			PointerField *string
		}
		// Empty struct - all zero values
		empty := TestStruct{}
		fb := NewFilter().FromStruct(empty)
		filters := fb.Build()

		if len(filters) != 0 {
			t.Errorf("expected 0 filters for zero struct, got %d", len(filters))
		}

		// Non-zero values
		nonZero := TestStruct{
			StringField: "test",
			IntField:    42,
			BoolField:   true,
			TimeField:   time.Now(),
			SliceField:  []string{"item"},
		}

		fb2 := NewFilter().FromStruct(nonZero)
		filters2 := fb2.Build()

		if len(filters2) != 5 {
			t.Errorf("expected 5 filters for non-zero struct, got %d", len(filters2))
		}
	})
}
