package imageexec

import (
	"maps"
	"slices"
	"strings"
	"testing"

	composeapi "github.com/docker/compose/v5/pkg/api"
	"github.com/stretchr/testify/require"
)

// owns and apply must agree key for key: an admitted key that apply never
// overwrote would let the image's value reach a container by inheritance.
func TestOwnedComposeBuildLabelsTableDrivesOwnsAndApply(t *testing.T) {
	bound := (composeBuildLabels{}).forProject("owned-project", "web-1")
	neutral := map[string]string{}
	(composeBuildLabels{}).apply(neutral)
	projected := map[string]string{}
	bound.apply(projected)

	tableKeys := make([]string, 0, len(ownedComposeBuildLabels))
	for _, owned := range ownedComposeBuildLabels {
		tableKeys = append(tableKeys, owned.key)
		require.True(t, bound.owns(owned.key), "table key %q must be admitted", owned.key)
		for _, applied := range []struct {
			from   composeBuildLabels
			labels map[string]string
		}{{composeBuildLabels{}, neutral}, {bound, projected}} {
			value, present := applied.labels[owned.key]
			require.True(t, present, "apply must write %q explicitly", owned.key)
			require.Equal(t, owned.replace(applied.from), value)
		}
		require.Empty(t, neutral[owned.key], "the direct projection neutralizes %q", owned.key)
		for _, variant := range []string{strings.ToUpper(owned.key), owned.key + " ", "K" + owned.key} {
			require.False(t, bound.owns(variant), "owns is exact: %q", variant)
		}
	}
	require.Len(t, tableKeys, len(slices.Compact(slices.Sorted(slices.Values(tableKeys)))), "table keys are unique")
	// apply writes nothing outside the table, and every key it writes is owned.
	require.ElementsMatch(t, tableKeys, slices.Collect(maps.Keys(neutral)))
	require.ElementsMatch(t, tableKeys, slices.Collect(maps.Keys(projected)))
	require.Equal(t, neutral, DirectCreationLabels())

	// The replacement policy itself, pinned per key.
	require.Equal(t, map[string]string{
		composeapi.ProjectLabel:      "owned-project",
		composeapi.ServiceLabel:      "web-1",
		composeapi.VersionLabel:      composeapi.ComposeVersion,
		composeapi.ImageBuilderLabel: "",
	}, projected)
	require.False(t, bound.owns(composeapi.ConfigHashLabel))
	require.False(t, bound.owns(composeapi.ImageDigestLabel))
}
