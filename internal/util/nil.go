package util

import "reflect"

// IsNilInterface reports whether value is nil or contains a typed nil in one
// of Go's nil-able kinds. Constructors use it at capability boundaries so an
// interface that appears non-nil cannot panic when first invoked.
func IsNilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
