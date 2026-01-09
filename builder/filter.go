package builder

import (
	"reflect"
	"strings"
	"time"
)

// FilterBuilder helps build complex filters
type FilterBuilder struct {
	filters []FilterParam
}

// NewFilter creates a new filter builder
func NewFilter() *FilterBuilder {
	return &FilterBuilder{
		filters: make([]FilterParam, 0),
	}
}

// Equal adds equality filter
func (f *FilterBuilder) Equal(field string, value interface{}) *FilterBuilder {
	f.filters = append(f.filters, FilterParam{
		Field:    field,
		Operator: Equal,
		Value:    value,
	})
	return f
}

// NotEqual adds not equal filter
func (f *FilterBuilder) NotEqual(field string, value interface{}) *FilterBuilder {
	f.filters = append(f.filters, FilterParam{
		Field:    field,
		Operator: NotEqual,
		Value:    value,
	})
	return f
}

// GreaterThan adds > filter
func (f *FilterBuilder) GreaterThan(field string, value interface{}) *FilterBuilder {
	f.filters = append(f.filters, FilterParam{
		Field:    field,
		Operator: GreaterThan,
		Value:    value,
	})
	return f
}

// GreaterThanOrEqual adds >= filter
func (f *FilterBuilder) GreaterThanOrEqual(field string, value interface{}) *FilterBuilder {
	f.filters = append(f.filters, FilterParam{
		Field:    field,
		Operator: GreaterThanOrEqual,
		Value:    value,
	})
	return f
}

// LessThan adds < filter
func (f *FilterBuilder) LessThan(field string, value interface{}) *FilterBuilder {
	f.filters = append(f.filters, FilterParam{
		Field:    field,
		Operator: LessThan,
		Value:    value,
	})
	return f
}

// LessThanOrEqual adds <= filter
func (f *FilterBuilder) LessThanOrEqual(field string, value interface{}) *FilterBuilder {
	f.filters = append(f.filters, FilterParam{
		Field:    field,
		Operator: LessThanOrEqual,
		Value:    value,
	})
	return f
}

// Between adds range filter (field >= start AND field <= end)
func (f *FilterBuilder) Between(field string, start, end interface{}) *FilterBuilder {
	f.GreaterThanOrEqual(field, start)
	f.LessThanOrEqual(field, end)
	return f
}

// IsNull checks if field is nil (Datastore doesn't store null, so check for zero value)
func (f *FilterBuilder) IsNull(field string) *FilterBuilder {
	f.filters = append(f.filters, FilterParam{
		Field:    field,
		Operator: Equal,
		Value:    nil,
	})
	return f
}

// DateRange adds date range filter
func (f *FilterBuilder) DateRange(field string, start, end time.Time) *FilterBuilder {
	return f.Between(field, start, end)
}

// Today filters for today's records
func (f *FilterBuilder) Today(field string) *FilterBuilder {
	now := time.Now()
	start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	end := start.Add(24 * time.Hour)
	return f.Between(field, start, end)
}

// FromStruct creates filters from struct fields
func (f *FilterBuilder) FromStruct(s interface{}) *FilterBuilder {
	v := reflect.ValueOf(s)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return f
	}

	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		value := v.Field(i)

		// Skip zero values
		if isZeroValue(value) {
			continue
		}

		// Get field name from tag
		tag := field.Tag.Get("datastore")
		if tag == "" || tag == "-" {
			tag = field.Tag.Get("json")
		}
		if tag == "" || tag == "-" {
			tag = strings.ToLower(field.Name)
		}

		// Parse tag
		tagParts := strings.Split(tag, ",")
		fieldName := tagParts[0]

		f.Equal(fieldName, value.Interface())
	}

	return f
}

// FromMap creates filters from map
func (f *FilterBuilder) FromMap(m map[string]interface{}) *FilterBuilder {
	for key, value := range m {
		// Parse operator from key
		operator := Equal
		field := key

		if strings.Contains(key, ">=") {
			parts := strings.Split(key, ">=")
			field = strings.TrimSpace(parts[0])
			operator = GreaterThanOrEqual
		} else if strings.Contains(key, "<=") {
			parts := strings.Split(key, "<=")
			field = strings.TrimSpace(parts[0])
			operator = LessThanOrEqual
		} else if strings.Contains(key, ">") {
			parts := strings.Split(key, ">")
			field = strings.TrimSpace(parts[0])
			operator = GreaterThan
		} else if strings.Contains(key, "<") {
			parts := strings.Split(key, "<")
			field = strings.TrimSpace(parts[0])
			operator = LessThan
		} else if strings.Contains(key, "!=") {
			parts := strings.Split(key, "!=")
			field = strings.TrimSpace(parts[0])
			operator = NotEqual
		}

		f.filters = append(f.filters, FilterParam{
			Field:    field,
			Operator: operator,
			Value:    value,
		})
	}

	return f
}

// Build returns the filter params
func (f *FilterBuilder) Build() []FilterParam {
	return f.filters
}

// Helper function
func isZeroValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	case reflect.Struct:
		// Special case for time.Time
		if v.Type() == reflect.TypeOf(time.Time{}) {
			return v.Interface().(time.Time).IsZero()
		}
		return false
	}
	return false
}
