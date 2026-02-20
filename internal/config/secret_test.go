package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestSecret_Redaction(t *testing.T) {
	s := Secret("super-secret-value")

	assert.Equal(t, "[REDACTED]", s.String(), "String()")
	assert.Equal(t, "[REDACTED]", fmt.Sprint(s), "fmt %%s")
	assert.Equal(t, "[REDACTED]", fmt.Sprintf("%v", s), "fmt %%v")
	assert.Equal(t, "[REDACTED]", s.GoString(), "GoString()")
	assert.Equal(t, slog.StringValue("[REDACTED]"), s.LogValue(), "LogValue()")

	b, err := json.Marshal(s)
	require.NoError(t, err)
	assert.Equal(t, `"[REDACTED]"`, string(b), "MarshalJSON")

	b, err = s.MarshalText()
	require.NoError(t, err)
	assert.Equal(t, "[REDACTED]", string(b), "MarshalText")
}

func TestSecret_RawValue(t *testing.T) {
	s := Secret("my-secret")
	assert.Equal(t, "my-secret", string(s), "string() conversion preserves raw value")
}

func TestSecret_Comparison(t *testing.T) {
	assert.True(t, Secret("") == "", "empty comparison")
	assert.Equal(t, 0, len(Secret("")), "len of empty")
	assert.Equal(t, 9, len(Secret("my-secret")), "len of non-empty")
}

func TestSecret_StructRedaction(t *testing.T) {
	type cfg struct {
		Name   string `json:"name"`
		Secret Secret `json:"secret"`
	}
	c := cfg{Name: "test", Secret: "do-not-leak-me"}

	t.Run("json.Marshal redacts secret", func(t *testing.T) {
		b, err := json.Marshal(c)
		require.NoError(t, err)
		assert.Contains(t, string(b), `"name":"test"`)
		assert.Contains(t, string(b), `"secret":"[REDACTED]"`)
		assert.NotContains(t, string(b), "do-not-leak-me")
	})

	t.Run("fmt %+v redacts secret", func(t *testing.T) {
		out := fmt.Sprintf("%+v", c)
		assert.Contains(t, out, "test")
		assert.Contains(t, out, "[REDACTED]")
		assert.NotContains(t, out, "do-not-leak-me")
	})

	t.Run("slog redacts secret", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))
		logger.Info("config", "secret", c.Secret)
		assert.Contains(t, buf.String(), "[REDACTED]")
		assert.NotContains(t, buf.String(), "do-not-leak-me")
	})
}

func TestSecret_JSONUnmarshal(t *testing.T) {
	type cfg struct {
		Secret Secret `json:"secret"`
	}

	input := []byte(`{"secret":"my-json-secret"}`)
	var c cfg
	require.NoError(t, json.Unmarshal(input, &c))
	assert.Equal(t, "my-json-secret", string(c.Secret))
}

func TestSecret_YAMLUnmarshal(t *testing.T) {
	type cfg struct {
		Secret Secret `yaml:"secret"`
	}

	input := []byte("secret: my-yaml-secret\n")
	var c cfg
	require.NoError(t, yaml.Unmarshal(input, &c))
	assert.Equal(t, "my-yaml-secret", string(c.Secret))
}

func TestSecret_YAMLMarshal(t *testing.T) {
	type cfg struct {
		Secret Secret `yaml:"secret"`
	}
	c := cfg{Secret: "do-not-leak"}

	b, err := yaml.Marshal(c)
	require.NoError(t, err)
	assert.NotContains(t, string(b), "do-not-leak")
	assert.Contains(t, string(b), "[REDACTED]")
}

func TestSecret_ViperUnmarshal(t *testing.T) {
	type cfg struct {
		CallbackSecret Secret `mapstructure:"callback_secret"`
	}

	v := viper.New()
	v.Set("callback_secret", "viper-secret-value")

	var c cfg
	require.NoError(t, v.Unmarshal(&c))
	assert.Equal(t, "viper-secret-value", string(c.CallbackSecret))
}
