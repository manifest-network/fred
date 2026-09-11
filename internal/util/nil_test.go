package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type nilTestValue struct{}

func TestIsNilInterface(t *testing.T) {
	var (
		nilPointer  *nilTestValue
		nilFunction func()
		nilMap      map[string]string
		nilSlice    []string
		nilChannel  chan struct{}
	)
	nonNilPointer := &nilTestValue{}

	tests := []struct {
		name  string
		value any
		want  bool
	}{
		{name: "nil interface", value: nil, want: true},
		{name: "typed nil pointer", value: nilPointer, want: true},
		{name: "typed nil function", value: nilFunction, want: true},
		{name: "typed nil map", value: nilMap, want: true},
		{name: "typed nil slice", value: nilSlice, want: true},
		{name: "typed nil channel", value: nilChannel, want: true},
		{name: "non-nil pointer", value: nonNilPointer, want: false},
		{name: "non-nil function", value: func() {}, want: false},
		{name: "non-nil map", value: map[string]string{}, want: false},
		{name: "non-nil slice", value: []string{}, want: false},
		{name: "non-nil channel", value: make(chan struct{}), want: false},
		{name: "zero scalar", value: 0, want: false},
		{name: "empty string", value: "", want: false},
		{name: "struct", value: nilTestValue{}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsNilInterface(tt.value))
		})
	}
}
