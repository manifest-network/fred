package manifest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/require"
)

// Exercise the published schema and the real wire parser with the same corpus.
// Cross-service dependency and lease-shape rules remain runtime-only, as the
// schema documents; these field limits and user values must agree in both forms.
func TestPublishedSchemaMatchesRuntimeFieldBoundaries(t *testing.T) {
	compiler := jsonschema.NewCompiler()
	schema, err := compiler.Compile(filepath.Join("..", "..", "..", "..", "docs", "manifest-schema.json"))
	require.NoError(t, err)
	type example struct {
		name  string
		field string
		value any
		valid bool
	}
	cases := []example{
		{"image user default", "user", "", true},
		{"numeric user", "user", "1000:1000", true},
		{"named user", "user", "postgres", true},
		{"empty group", "user", "1000:", false},
		{"whitespace user", "user", "a b", false},
		{"extra user separator", "user", "1000:1000:1000", false},
		{"form feed user", "user", "a\fb", false},
		{"vertical tab user", "user", "a\vb", false},
		{"non-ASCII user", "user", "a\u00a0b", true},
	}
	for _, field := range []struct {
		name string
		max  int
	}{
		{"ports", MaxPorts}, {"env", MaxEnvVars},
		{"labels", MaxLabels}, {"expose", MaxExposePorts},
	} {
		for _, count := range []int{field.max, field.max + 1} {
			values := make(map[string]any, count)
			ports := make([]string, count)
			for i := range count {
				key := fmt.Sprintf("item%d", i)
				values[key] = "value"
				ports[i] = fmt.Sprint(10000 + i)
				if field.name == "ports" {
					delete(values, key)
					values[ports[i]+"/tcp"] = map[string]any{}
				}
			}
			var value any = values
			if field.name == "expose" {
				value = ports
			}
			cases = append(cases, example{
				fmt.Sprintf("%s/%d", field.name, count), field.name, value, count == field.max,
			})
		}
	}
	for _, tc := range cases {
		for _, stack := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stack=%t", tc.name, stack), func(t *testing.T) {
				service := map[string]any{"image": "nginx:latest", tc.field: tc.value}
				var value any = service
				if stack {
					value = map[string]any{"services": map[string]any{"app": service}}
				}
				wire, err := json.Marshal(value)
				require.NoError(t, err)
				document, err := jsonschema.UnmarshalJSON(bytes.NewReader(wire))
				require.NoError(t, err)
				schemaErr := schema.Validate(document)
				_, runtimeErr := ParsePayload(wire)
				if tc.valid {
					require.NoError(t, schemaErr, "published schema rejected valid wire input")
					require.NoError(t, runtimeErr, "runtime rejected valid wire input")
				} else {
					require.Error(t, schemaErr, "published schema accepted invalid wire input")
					require.Error(t, runtimeErr, "runtime accepted invalid wire input")
				}
			})
		}
	}
}
