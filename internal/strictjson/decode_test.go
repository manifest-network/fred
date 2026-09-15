package strictjson

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testEmbedded struct {
	Tenant string `json:"tenant"`
}

type testChild struct {
	Name string `json:"name"`
}

type testDocument struct {
	testEmbedded
	Version  int                  `json:"version"`
	Child    testChild            `json:"child"`
	Children []testChild          `json:"children"`
	ByName   map[string]testChild `json:"by_name"`
	Custom   testCustom           `json:"custom"`
}

type testCustom struct {
	Value string
}

func (custom *testCustom) UnmarshalJSON(value []byte) error {
	type wire struct {
		Value string `json:"value"`
	}
	var decoded wire
	if err := DecodeObject(value, 1<<10, &decoded); err != nil {
		return err
	}
	custom.Value = decoded.Value
	return nil
}

func TestDecodeObjectAcceptsExactRecursiveShape(t *testing.T) {
	var document testDocument
	err := DecodeObject([]byte(`{
		"tenant":"tenant-a",
		"version":1,
		"child":{"name":"one"},
		"children":[{"name":"two"}],
		"by_name":{"three":{"name":"three"}},
		"custom":{"value":"four"}
	}`), 4<<10, &document)
	require.NoError(t, err)
	assert.Equal(t, "tenant-a", document.Tenant)
	assert.Equal(t, "one", document.Child.Name)
	assert.Equal(t, "two", document.Children[0].Name)
	assert.Equal(t, "three", document.ByName["three"].Name)
	assert.Equal(t, "four", document.Custom.Value)
}

func TestDecodeObjectRejectsCaseAliasesRecursively(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		field string
	}{
		{name: "embedded alias only", value: `{"Tenant":"a"}`, field: "Tenant"},
		{name: "canonical and alias", value: `{"tenant":"a","Tenant":"b"}`, field: "Tenant"},
		{name: "nested struct", value: `{"child":{"Name":"a"}}`, field: "Name"},
		{name: "slice element", value: `{"children":[{"Name":"a"}]}`, field: "Name"},
		{name: "map value", value: `{"by_name":{"a":{"Name":"a"}}}`, field: "Name"},
		{name: "custom decoder", value: `{"custom":{"Value":"a"}}`, field: "Value"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var document testDocument
			err := DecodeObject([]byte(test.value), 4<<10, &document)
			require.ErrorContains(t, err, `unknown field "`+test.field+`"`)
		})
	}
}

func TestDecodeObjectRejectsDuplicateEscapedName(t *testing.T) {
	var document testDocument
	err := DecodeObject(
		[]byte(`{"tenant":"a","\u0074enant":"b"}`),
		1<<10,
		&document,
	)
	require.ErrorContains(t, err, `duplicate field "tenant"`)
}

func TestDecodeObjectRejectsFramingAndSizeViolations(t *testing.T) {
	for _, test := range []struct {
		name  string
		value []byte
		max   int
		want  string
	}{
		{name: "root array", value: []byte(`[]`), max: 10, want: "expected JSON object"},
		{name: "trailing", value: []byte(`{} true`), max: 10, want: "unexpected data"},
		{name: "size", value: []byte(`{"version":1}`), max: 2, want: "exceeds 2 bytes"},
		{name: "invalid UTF-8", value: []byte{'{', '"', 0xff, '"', ':', '1', '}'}, max: 10, want: "not valid UTF-8"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var document testDocument
			require.ErrorContains(
				t, DecodeObject(test.value, test.max, &document), test.want,
			)
		})
	}
}

func TestDecodeArrayUsesTheSameExactBoundary(t *testing.T) {
	var children []testChild
	require.NoError(t, DecodeArray([]byte(`[{"name":"one"}]`), 1<<10, &children))
	require.Equal(t, []testChild{{Name: "one"}}, children)
	require.ErrorContains(
		t,
		DecodeArray([]byte(`[{"name":"one","Name":"two"}]`), 1<<10, &children),
		`unknown field "Name"`,
	)
}

func TestDecodeRawObjectRejectsUnknownDuplicateAndTrailingData(t *testing.T) {
	allowed := map[string]struct{}{"version": {}}
	object, err := DecodeRawObject([]byte(`{"version":1}`), 1<<10, allowed)
	require.NoError(t, err)
	assert.Equal(t, json.RawMessage("1"), object["version"])

	for _, value := range []string{
		`{"Version":1}`,
		`{"version":1,"version":2}`,
		`{"version":1} null`,
	} {
		_, err := DecodeRawObject([]byte(value), 1<<10, allowed)
		require.Error(t, err)
	}
}
