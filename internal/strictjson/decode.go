// Package strictjson provides an exact JSON decoding boundary for durable
// authority. It supplements encoding/json with recursive duplicate rejection
// and case-sensitive field matching; DisallowUnknownFields alone is
// case-insensitive and therefore cannot prevent alias overwrites.
package strictjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"unicode/utf8"
)

var jsonUnmarshalerType = reflect.TypeFor[json.Unmarshaler]()

// DecodeObject decodes exactly one JSON object into destination. Object field
// names must exactly match their Go JSON tags at every recursively typed
// struct boundary; duplicate, unknown, case-aliased, and trailing data fail.
func DecodeObject(value []byte, maxBytes int, destination any) error {
	if err := ValidateUniqueObject(value, maxBytes); err != nil {
		return err
	}
	if err := validateExactShape(value, destination); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	return requireEOF(decoder)
}

// DecodeArray is DecodeObject's top-level-array counterpart. It exists for
// narrowly gated legacy formats whose historical root was an array.
func DecodeArray(value []byte, maxBytes int, destination any) error {
	if err := ValidateUniqueArray(value, maxBytes); err != nil {
		return err
	}
	if err := validateExactShape(value, destination); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	return requireEOF(decoder)
}

// DecodeRawObject returns an exact object whose keys are constrained by
// allowed. Values remain raw but are still recursively checked for duplicate
// object fields and the document must contain exactly one value.
func DecodeRawObject(
	value []byte,
	maxBytes int,
	allowed map[string]struct{},
) (map[string]json.RawMessage, error) {
	if err := ValidateUniqueObject(value, maxBytes); err != nil {
		return nil, err
	}
	var object map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(value))
	if err := decoder.Decode(&object); err != nil {
		return nil, err
	}
	if err := requireEOF(decoder); err != nil {
		return nil, err
	}
	for name := range object {
		if _, ok := allowed[name]; !ok {
			return nil, fmt.Errorf("unknown field %q", name)
		}
	}
	return object, nil
}

func validateExactShape(value []byte, destination any) error {
	var decoded any
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	typ := reflect.TypeOf(destination)
	if typ == nil || typ.Kind() != reflect.Pointer || typ.Elem().Kind() == reflect.Invalid {
		return errors.New("strict JSON destination must be a non-nil pointer")
	}
	return validateExactValue(decoded, typ.Elem())
}

func validateExactValue(value any, typ reflect.Type) error {
	for typ.Kind() == reflect.Pointer {
		if value == nil {
			return nil
		}
		if typ.Implements(jsonUnmarshalerType) {
			return nil
		}
		typ = typ.Elem()
	}
	if reflect.PointerTo(typ).Implements(jsonUnmarshalerType) {
		return nil
	}
	switch typ.Kind() {
	case reflect.Struct:
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("expected JSON object for %s", typ)
		}
		fields := exactStructFields(typ)
		for name, child := range object {
			fieldType, exists := fields[name]
			if !exists {
				return fmt.Errorf("unknown field %q", name)
			}
			if err := validateExactValue(child, fieldType); err != nil {
				return fmt.Errorf("decode field %q: %w", name, err)
			}
		}
	case reflect.Slice:
		if typ.Elem().Kind() == reflect.Uint8 {
			return nil
		}
		array, ok := value.([]any)
		if !ok {
			if value == nil {
				return nil
			}
			return fmt.Errorf("expected JSON array for %s", typ)
		}
		for index, child := range array {
			if err := validateExactValue(child, typ.Elem()); err != nil {
				return fmt.Errorf("decode array element %d: %w", index, err)
			}
		}
	case reflect.Array:
		array, ok := value.([]any)
		if !ok {
			return fmt.Errorf("expected JSON array for %s", typ)
		}
		for index, child := range array {
			if err := validateExactValue(child, typ.Elem()); err != nil {
				return fmt.Errorf("decode array element %d: %w", index, err)
			}
		}
	case reflect.Map:
		object, ok := value.(map[string]any)
		if !ok {
			if value == nil {
				return nil
			}
			return fmt.Errorf("expected JSON object for %s", typ)
		}
		for name, child := range object {
			if err := validateExactValue(child, typ.Elem()); err != nil {
				return fmt.Errorf("decode map value %q: %w", name, err)
			}
		}
	}
	return nil
}

func exactStructFields(typ reflect.Type) map[string]reflect.Type {
	fields := make(map[string]reflect.Type)
	for index := range typ.NumField() {
		field := typ.Field(index)
		if field.PkgPath != "" && !field.Anonymous {
			continue
		}
		tag := field.Tag.Get("json")
		name, _, _ := strings.Cut(tag, ",")
		if name == "-" {
			continue
		}
		if field.Anonymous && name == "" {
			embedded := field.Type
			for embedded.Kind() == reflect.Pointer {
				embedded = embedded.Elem()
			}
			if embedded.Kind() == reflect.Struct {
				for embeddedName, embeddedType := range exactStructFields(embedded) {
					fields[embeddedName] = embeddedType
				}
				continue
			}
		}
		if name == "" {
			name = field.Name
		}
		fields[name] = field.Type
	}
	return fields
}

// ValidateUniqueObject verifies one bounded object and rejects duplicate keys
// recursively, including objects nested inside arrays.
func ValidateUniqueObject(value []byte, maxBytes int) error {
	return validateUniqueComposite(value, maxBytes, '{')
}

// ValidateUniqueArray verifies one bounded array and rejects duplicate object
// keys recursively.
func ValidateUniqueArray(value []byte, maxBytes int) error {
	return validateUniqueComposite(value, maxBytes, '[')
}

func validateUniqueComposite(value []byte, maxBytes int, want json.Delim) error {
	if len(value) > maxBytes {
		return fmt.Errorf("JSON entry exceeds %d bytes", maxBytes)
	}
	if !utf8.Valid(value) {
		return errors.New("JSON is not valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	opening, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := opening.(json.Delim)
	if !ok || delimiter != want {
		if want == '{' {
			return errors.New("expected JSON object")
		}
		return errors.New("expected JSON array")
	}
	if want == '{' {
		err = validateUniqueObjectBody(decoder)
	} else {
		for index := 0; decoder.More(); index++ {
			if err = validateUniqueValue(decoder); err != nil {
				err = fmt.Errorf("decode array element %d: %w", index, err)
				break
			}
		}
		if err == nil {
			err = consumeClosing(decoder, ']')
		}
	}
	if err != nil {
		return err
	}
	if err := requireTokenEOF(decoder); err != nil {
		return err
	}
	return nil
}

func validateUniqueObjectBody(decoder *json.Decoder) error {
	seen := make(map[string]struct{})
	for decoder.More() {
		nameToken, err := decoder.Token()
		if err != nil {
			return err
		}
		name, ok := nameToken.(string)
		if !ok {
			return errors.New("object field name is not a string")
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("duplicate field %q", name)
		}
		seen[name] = struct{}{}
		if err := validateUniqueValue(decoder); err != nil {
			return fmt.Errorf("decode field %q: %w", name, err)
		}
	}
	return consumeClosing(decoder, '}')
}

func validateUniqueValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		return nil
	}
	switch delimiter {
	case '{':
		return validateUniqueObjectBody(decoder)
	case '[':
		for index := 0; decoder.More(); index++ {
			if err := validateUniqueValue(decoder); err != nil {
				return fmt.Errorf("decode array element %d: %w", index, err)
			}
		}
		return consumeClosing(decoder, ']')
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
}

func consumeClosing(decoder *json.Decoder, want json.Delim) error {
	closing, err := decoder.Token()
	if err != nil {
		return err
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != want {
		return fmt.Errorf("unterminated JSON %s", map[json.Delim]string{
			'}': "object",
			']': "array",
		}[want])
	}
	return nil
}

func requireTokenEOF(decoder *json.Decoder) error {
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err != nil {
			return err
		}
		return errors.New("unexpected data after JSON value")
	}
	return nil
}

func requireEOF(decoder *json.Decoder) error {
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err != nil {
			return err
		}
		return errors.New("trailing JSON value")
	}
	return nil
}
