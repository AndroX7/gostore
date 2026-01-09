package builder

import (
	"testing"
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
