package k3s

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/config"
)

// TestDefaultMaxRequestBodySize_HeadroomOverAPICap is the F42 regression: the
// backend body cap must be strictly larger than providerd's request cap, so a
// tenant manifest that cleared providerd (then got re-serialized/wrapped for the
// backend hop) is not rejected with an opaque 400 at the backend.
func TestDefaultMaxRequestBodySize_HeadroomOverAPICap(t *testing.T) {
	require.Greater(t, DefaultMaxRequestBodySize, config.DefaultMaxRequestBodySize,
		"backend body cap must exceed providerd's cap to leave room for forward-wrapping overhead")
}

// TestConfigValidate_FloorsNonPositiveBodyCap: an unset/zero cap normalizes to
// the default rather than disabling the body limit entirely.
func TestConfigValidate_FloorsNonPositiveBodyCap(t *testing.T) {
	c := Config{Name: "k3s", MaxRequestBodySize: 0}
	_ = c.Validate() // later required-field checks may error; the floor runs first
	require.Equal(t, DefaultMaxRequestBodySize, c.MaxRequestBodySize)
}
