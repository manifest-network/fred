//go:build integration

package chain

import (
	"testing"

	"github.com/manifest-network/fred/test/e2e"
)

func TestMain(m *testing.M) { e2e.Main(m) }
