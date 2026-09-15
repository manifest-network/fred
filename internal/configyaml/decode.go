// Package configyaml decodes one operator configuration document, rejecting
// unknown struct fields and additional YAML documents instead of ignoring them.
package configyaml

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"gopkg.in/yaml.v3"
)

// Decode fills destination from exactly one YAML document. Map keys remain
// unrestricted; callers decoding into maps must validate the resulting keys.
func Decode(data []byte, destination any) error {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return fmt.Errorf("invalid trailing YAML: %w", err)
		}
		return errors.New("configuration must contain exactly one YAML document")
	}
	return nil
}
