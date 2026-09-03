package docker

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestValidateComposeServiceNames(t *testing.T) {
	t.Run("rejects scaled name colliding with an unscaled service", func(t *testing.T) {
		err := validateComposeServiceNames([]backend.LeaseItem{
			{SKU: "docker-small", ServiceName: "web", Quantity: 2},
			{SKU: "docker-small", ServiceName: "web-0", Quantity: 1},
		})
		require.ErrorContains(t, err, `expanded Compose service name "web-0" collides`)
	})

	t.Run("accepts distinct expanded names", func(t *testing.T) {
		require.NoError(t, validateComposeServiceNames([]backend.LeaseItem{
			{SKU: "docker-small", ServiceName: "web", Quantity: 2},
			{SKU: "docker-small", ServiceName: "worker", Quantity: 3},
			{SKU: "docker-small", ServiceName: "web-0", Quantity: 2},
		}))
	})

	t.Run("construction uses the proven naming rule", func(t *testing.T) {
		item := backend.LeaseItem{ServiceName: "api", Quantity: 2}
		require.Equal(t, "api-0", composeServiceName(item, 0))
		require.Equal(t, "api-1", composeServiceName(item, 1))
		item.Quantity = 1
		require.Equal(t, "api", composeServiceName(item, 0))
	})
}

func TestMapComposeContainersUsesExactExpandedServiceKeys(t *testing.T) {
	items := []backend.LeaseItem{
		{SKU: "docker-small", ServiceName: "web", Quantity: 2},
		{SKU: "docker-small", ServiceName: "web-01", Quantity: 1},
		{SKU: "docker-small", ServiceName: "web-999", Quantity: 1},
	}
	containers := []composeContainerSummary{
		{ID: "leading-zero", Service: "web-01"},
		{ID: "web-one", Service: "web-1"},
		{ID: "out-of-range-name", Service: "web-999"},
		{ID: "web-zero", Service: "web-0"},
	}

	containerIDs, serviceContainers, err := mapComposeContainers(containers, items)
	require.NoError(t, err)
	require.Equal(t, []string{"leading-zero", "web-one", "out-of-range-name", "web-zero"}, containerIDs)
	require.Equal(t, map[string][]string{
		"web":     {"web-one", "web-zero"},
		"web-01":  {"leading-zero"},
		"web-999": {"out-of-range-name"},
	}, serviceContainers)
}

func TestMapComposeContainersRejectsUnknownServiceKey(t *testing.T) {
	containerIDs, serviceContainers, err := mapComposeContainers(
		[]composeContainerSummary{
			{ID: "known", Service: "web-0"},
			{ID: "unknown", Service: "web-2"},
		},
		[]backend.LeaseItem{{SKU: "docker-small", ServiceName: "web", Quantity: 2}},
	)

	require.ErrorContains(t, err, `compose ps returned unknown service key "web-2"`)
	require.Equal(t, []string{"known", "unknown"}, containerIDs,
		"return discovered IDs so failure cleanup can remove the full observed cohort")
	require.Equal(t, map[string][]string{"web": {"known"}}, serviceContainers)
}
