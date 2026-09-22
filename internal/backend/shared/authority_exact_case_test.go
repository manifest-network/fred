package shared

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCurrentAuthorityDecodersRejectCaseAliases(t *testing.T) {
	tests := []struct {
		name   string
		decode func([]byte) error
		alias  string
		both   string
	}{
		{
			name: "v2 callback",
			decode: func(value []byte) error {
				_, err := decodeV2CallbackEntry(value)
				return err
			},
			alias: `{"Version":2}`,
			both:  `{"version":99,"Version":2}`,
		},
		{
			name: "operation intent",
			decode: func(value []byte) error {
				_, err := decodeOperationIntent([]byte(schemaTestLeaseUUID), value)
				return err
			},
			alias: `{"LeaseUUID":"` + schemaTestLeaseUUID + `"}`,
			both: `{"lease_uuid":"` + schemaTestLeaseUUID +
				`","LeaseUUID":"22222222-2222-4222-8222-222222222222"}`,
		},
		{
			name: "operation completion",
			decode: func(value []byte) error {
				_, err := decodeOperationCompletionRecord(
					[]byte(schemaTestLeaseUUID), nil, value,
				)
				return err
			},
			alias: `{"Version":1}`,
			both:  `{"version":99,"Version":1}`,
		},
		{
			name: "maintenance intent",
			decode: func(value []byte) error {
				_, err := decodeMaintenanceIntent([]byte(schemaTestLeaseUUID), value)
				return err
			},
			alias: `{"LeaseUUID":"` + schemaTestLeaseUUID + `"}`,
			both: `{"lease_uuid":"` + schemaTestLeaseUUID +
				`","LeaseUUID":"22222222-2222-4222-8222-222222222222"}`,
		},
		{
			name: "maintenance completion",
			decode: func(value []byte) error {
				_, err := decodeMaintenanceCompletionRecord(
					[]byte(schemaTestLeaseUUID), nil, value,
				)
				return err
			},
			alias: `{"Version":1}`,
			both:  `{"version":99,"Version":1}`,
		},
		{
			name: "close intent",
			decode: func(value []byte) error {
				_, err := decodeCloseIntent([]byte(schemaTestLeaseUUID), value)
				return err
			},
			alias: `{"LeaseUUID":"` + schemaTestLeaseUUID + `"}`,
			both: `{"lease_uuid":"` + schemaTestLeaseUUID +
				`","LeaseUUID":"22222222-2222-4222-8222-222222222222"}`,
		},
		{
			name: "closed lease",
			decode: func(value []byte) error {
				_, err := decodeClosedLeaseTombstone([]byte(schemaTestLeaseUUID), value)
				return err
			},
			alias: `{"LeaseUUID":"` + schemaTestLeaseUUID + `"}`,
			both: `{"lease_uuid":"` + schemaTestLeaseUUID +
				`","LeaseUUID":"22222222-2222-4222-8222-222222222222"}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name+" alias only", func(t *testing.T) {
			require.ErrorContains(t, test.decode([]byte(test.alias)), "unknown field")
		})
		t.Run(test.name+" canonical and alias", func(t *testing.T) {
			require.ErrorContains(t, test.decode([]byte(test.both)), "unknown field")
		})
	}
}

func TestMutationHeadRejectsAliasesInEveryNestedAuthorityVariant(t *testing.T) {
	for _, kind := range []string{"operation", "maintenance", "close", "closed"} {
		t.Run(kind, func(t *testing.T) {
			value := []byte(`{"version":1,"kind":"` + kind + `","` + kind +
				`":{"lease_uuid":"` + schemaTestLeaseUUID + `","LeaseUUID":"` +
				`22222222-2222-4222-8222-222222222222"}}`)
			_, err := decodeLeaseMutationHead([]byte(schemaTestLeaseUUID), value)
			require.ErrorContains(t, err, `unknown field "LeaseUUID"`)
		})
	}

	_, err := decodeLeaseMutationHead(
		[]byte(schemaTestLeaseUUID),
		[]byte(`{"version":99,"Version":1,"kind":"closed","closed":{}}`),
	)
	require.ErrorContains(t, err, `unknown field "Version"`)
}
