package provisioner

import "reflect"

// isNilCapability recognizes both a nil interface and an interface containing
// a typed nil. Consumer constructors use it to avoid retaining a capability
// that looks present but panics on first use.
func isNilCapability(capability any) bool {
	if capability == nil {
		return true
	}
	value := reflect.ValueOf(capability)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
