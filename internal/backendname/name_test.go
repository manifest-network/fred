package backendname

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	for _, name := range []string{
		"backend-a",
		"s049-u002",
		"backend east",
		"backend-é",
	} {
		t.Run("valid "+name, func(t *testing.T) {
			require.NoError(t, Validate(name))
		})
	}

	for _, test := range []struct {
		name string
		want string
	}{
		{name: "", want: "required"},
		{name: " backend-a", want: "leading or trailing whitespace"},
		{name: "backend-a ", want: "leading or trailing whitespace"},
		{name: "backend-a\nPASS: forged", want: "non-printable character U+000A"},
		{name: "backend-a\t", want: "leading or trailing whitespace"},
		{name: "backend-\u200B", want: "non-printable character U+200B"},
		{name: string([]byte{'b', 0xff}), want: "valid UTF-8"},
	} {
		t.Run("invalid", func(t *testing.T) {
			assert.ErrorContains(t, Validate(test.name), test.want)
		})
	}
}
