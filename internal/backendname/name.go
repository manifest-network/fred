// Package backendname defines the shared syntax for configured and durable
// backend identifiers. Keeping the check in a leaf package prevents config,
// placement metadata, and operator tools from accepting different identities.
package backendname

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Validate rejects names that cannot be rendered as one unambiguous printable
// identifier. Internal spaces remain valid for compatibility, but leading or
// trailing whitespace and control/format characters are not identities.
func Validate(name string) error {
	if name == "" {
		return errors.New("backend name is required")
	}
	if !utf8.ValidString(name) {
		return errors.New("backend name must be valid UTF-8")
	}
	if strings.TrimSpace(name) != name {
		return errors.New("backend name must not have leading or trailing whitespace")
	}
	for _, character := range name {
		if !unicode.IsPrint(character) {
			return fmt.Errorf("backend name contains non-printable character %U", character)
		}
	}
	return nil
}
