package main

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/hmacauth"
)

func TestPrintRequestKeyIDsReportsBothKeysWithoutTheKeys(t *testing.T) {
	cfg := docker.Config{CallbackSecret: config.Secret(rotationOldKey), CallbackSecretNext: config.RotationSecret(rotationNewKey)}
	var out bytes.Buffer
	require.NoError(t, printRequestKeyIDs(&out, cfg))
	assert.NotContains(t, out.String(), rotationOldKey)
	assert.NotContains(t, out.String(), rotationNewKey)
	var printed requestKeyIDs
	decoder := json.NewDecoder(&out)
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&printed))
	assert.Equal(t, requestKeyIDs{
		CurrentKeyID: hmacauth.KeyID(rotationOldKey),
		NextKeyID:    hmacauth.KeyID(rotationNewKey),
	}, printed)
}

func TestParseStartupFlagsKeepsKeyIDPrintingExclusive(t *testing.T) {
	flags, err := parseStartupFlags([]string{"-print-hmac-key-ids"}, io.Discard)
	require.NoError(t, err)
	assert.True(t, flags.printHMACKeyIDs)
	_, err = parseStartupFlags([]string{"-print-hmac-key-ids", "-validate-config"}, io.Discard)
	assert.ErrorContains(t, err, "mutually exclusive")
}
