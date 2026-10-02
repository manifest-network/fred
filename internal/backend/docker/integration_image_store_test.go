//go:build integration

package docker

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIntegration_Docker_ImageStoreMatchesRunnerDeclaration proves the suite ran
// against the image store the CI leg declares, using the same predicates
// production uses. Without it a runner that silently fell back to the other
// store would pass both legs while testing only one store.
//
// It skips when FRED_TEST_IMAGE_STORE is unset (a local run); CI sets it on
// every leg, and the workflow's SKIP guard fails the job if it ever does not.
func TestIntegration_Docker_ImageStoreMatchesRunnerDeclaration(t *testing.T) {
	want := os.Getenv("FRED_TEST_IMAGE_STORE")
	if want == "" {
		t.Skip("FRED_TEST_IMAGE_STORE unset: no runner image-store declaration to check")
	}
	require.Contains(t, []string{"overlay2", "containerd"}, want, "unknown FRED_TEST_IMAGE_STORE")

	sdk := newImageSecurityFixtureClient(t)
	if _, err := sdk.Ping(t.Context()); err != nil {
		t.Skipf("Docker daemon unavailable: %v", err)
	}
	info, err := sdk.Info(t.Context())
	require.NoError(t, err)

	require.NoError(t, requireBoundedImageStore(info), "the runner daemon must use a store bounded image import supports")
	require.Equal(t, want == "containerd", daemonUsesContainerd(info),
		"runner declared %q but the daemon reports driver %q with status %v", want, info.Driver, info.DriverStatus)
	if want == "overlay2" {
		require.Equal(t, "overlay2", info.Driver)
	} else {
		require.Equal(t, "overlayfs", info.Driver)
	}
	// Mirror the deployed configuration: the classic store needs no
	// image_data_path, while the containerd store requires an explicit one.
	if want == "containerd" {
		require.NotEmpty(t, os.Getenv("FRED_TEST_IMAGE_DATA_PATH"), "the containerd leg must declare its image data path")
	} else {
		require.Empty(t, os.Getenv("FRED_TEST_IMAGE_DATA_PATH"), "the overlay2 leg must use Docker's data root, as deployed")
	}
}
