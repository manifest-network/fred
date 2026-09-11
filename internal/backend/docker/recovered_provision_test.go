package docker

import (
	"maps"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// fullRecoveredProvision builds a recoveredProvision with every field set to a
// distinct non-zero value, so materialize round-tripping can be asserted
// field-by-field.
func fullRecoveredProvision() recoveredProvision {
	return recoveredProvision{
		ProvisionState: leasesm.ProvisionState{
			LeaseUUID:         "lease-1",
			Tenant:            "tenant-a",
			ProviderUUID:      "prov-1",
			SKU:               "docker-small",
			Status:            backend.ProvisionStatusReady,
			Quantity:          2,
			CreatedAt:         time.Unix(1700000000, 0),
			FailCount:         3,
			LastError:         "boom",
			CallbackURL:       "http://cb/callbacks/provision",
			ActiveOperationID: mustDockerOperationID("11111111-1111-4111-8111-111111111111"),
			Items:             []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "app"}},
			ResourceProfiles: []shared.SKUResourceSnapshot{{
				SKU: "docker-small", CPUCores: 0.5, MemoryMB: 512, ScratchDiskMB: 64,
			}},
			ContainerIDs:      []string{"c1", "c2"},
			StackManifest:     nil,
			ServiceContainers: map[string][]string{"app": {"c1", "c2"}},
		},
	}
}

func TestRecoveredProvision_Materialize_RoundTripsEveryField(t *testing.T) {
	rec := fullRecoveredProvision()
	p := rec.materialize()
	require.NotNil(t, p)
	assert.Equal(t, rec.ProvisionState, p.ProvisionState, "ProvisionState must round-trip wholesale")
	assert.Equal(t, rec.ResourceProfiles, p.ResourceProfiles, "resource profiles must round-trip")
	p.ResourceProfiles[0].ScratchDiskMB = 2048
	assert.Equal(t, int64(64), rec.ResourceProfiles[0].ScratchDiskMB,
		"materialize must not alias the recovered snapshot")
}

func TestRecoveredFromProvision_ClonesReferenceFields(t *testing.T) {
	src := &provision{
		ProvisionState: leasesm.ProvisionState{
			LeaseUUID:         "lease-1",
			Status:            backend.ProvisionStatusFailing,
			Items:             []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
			ResourceProfiles:  []shared.SKUResourceSnapshot{{SKU: "docker-small", CPUCores: 0.5, MemoryMB: 512, ScratchDiskMB: 64}},
			ContainerIDs:      []string{"c1"},
			ServiceContainers: map[string][]string{"app": {"c1"}},
		},
	}
	rec := recoveredFromProvision(src)
	// Mutating the clone must not touch the source's backing arrays/maps.
	rec.Items[0].ServiceName = "mutated"
	rec.ContainerIDs[0] = "mutated"
	rec.ServiceContainers["app"][0] = "mutated"
	rec.ProvisionState.ResourceProfiles[0].ScratchDiskMB = 1024
	assert.Equal(t, "app", src.Items[0].ServiceName, "Items must be cloned")
	assert.Equal(t, "c1", src.ContainerIDs[0], "ContainerIDs must be cloned")
	assert.Equal(t, "c1", src.ServiceContainers["app"][0], "ServiceContainers must be deep-cloned")
	assert.Equal(t, int64(64), src.ProvisionState.ResourceProfiles[0].ScratchDiskMB,
		"embedded provision-state resource profiles must be cloned")
	assert.Equal(t, int64(64), src.ResourceProfiles[0].ScratchDiskMB, "resource profiles must be cloned")
}

func TestRecoveredFromProvision_PreservesNilVsEmpty(t *testing.T) {
	// slices.Clone preserves nil-vs-empty, so a kept entry's reference fields
	// keep the same nil-ness they had before normalization (byte-equivalent to
	// the prior preserve-by-pointer path; the old append([]T(nil), ...) idiom
	// collapsed a non-nil empty slice to nil).
	t.Run("nil stays nil", func(t *testing.T) {
		rec := recoveredFromProvision(&provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "L1"}})
		assert.Nil(t, rec.Items)
		assert.Nil(t, rec.ContainerIDs)
		assert.Nil(t, rec.ServiceContainers)
		assert.Nil(t, rec.ResourceProfiles)
	})
	t.Run("non-nil empty stays non-nil empty", func(t *testing.T) {
		rec := recoveredFromProvision(&provision{ProvisionState: leasesm.ProvisionState{
			LeaseUUID:        "L1",
			Items:            []backend.LeaseItem{},
			ContainerIDs:     []string{},
			ResourceProfiles: []shared.SKUResourceSnapshot{},
		}})
		assert.NotNil(t, rec.Items, "non-nil empty Items must stay non-nil")
		assert.Empty(t, rec.Items)
		assert.NotNil(t, rec.ContainerIDs, "non-nil empty ContainerIDs must stay non-nil")
		assert.Empty(t, rec.ContainerIDs)
		assert.NotNil(t, rec.ResourceProfiles, "non-nil empty resource profiles must stay non-nil")
		assert.Empty(t, rec.ResourceProfiles)
	})
}

func TestProvisionMatchesRecovered_DetectsMutationWithoutAllocating(t *testing.T) {
	p := fullRecoveredProvision().materialize()
	snapshot := recoveredFromProvision(p)
	require.True(t, provisionMatchesRecovered(p, snapshot))

	allocations := testing.AllocsPerRun(100, func() {
		if !provisionMatchesRecovered(p, snapshot) {
			panic("unchanged provision stopped matching its recovery baseline")
		}
	})
	assert.Zero(t, allocations,
		"the fleet-sized comparison runs under provisionsMu and must not clone live state")

	p.ContainerIDs[0] = "replacement"
	assert.False(t, provisionMatchesRecovered(p, snapshot),
		"the allocation-free comparison must still detect in-place actor mutation")
}

func TestProvisionMatchesRecovered_DetectsNilToEmptyMutation(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(*provision)
		mutate func(*provision)
	}{
		{
			name: "items",
			mutate: func(p *provision) {
				p.Items = []backend.LeaseItem{}
			},
		},
		{
			name: "state resource profiles",
			mutate: func(p *provision) {
				p.ResourceProfiles = []shared.SKUResourceSnapshot{}
			},
		},
		{
			name: "container IDs",
			mutate: func(p *provision) {
				p.ContainerIDs = []string{}
			},
		},
		{
			name: "service containers map",
			mutate: func(p *provision) {
				p.ServiceContainers = map[string][]string{}
			},
		},
		{
			name: "service containers value",
			setup: func(p *provision) {
				p.ServiceContainers = map[string][]string{"app": nil}
			},
			mutate: func(p *provision) {
				p.ServiceContainers["app"] = []string{}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1"}}
			if test.setup != nil {
				test.setup(p)
			}
			snapshot := recoveredFromProvision(p)
			require.True(t, provisionMatchesRecovered(p, snapshot))

			test.mutate(p)
			assert.False(t, provisionMatchesRecovered(p, snapshot),
				"nil-to-empty mutation must invalidate the recovery snapshot")
		})
	}
}

func TestProvisionStateMatches_CoversEveryField(t *testing.T) {
	mutations := map[string]func(*provision){
		"LeaseUUID":    func(p *provision) { p.LeaseUUID = "lease-2" },
		"Tenant":       func(p *provision) { p.Tenant = "tenant-b" },
		"ProviderUUID": func(p *provision) { p.ProviderUUID = "provider-2" },
		"SKU":          func(p *provision) { p.SKU = "docker-large" },
		"Status":       func(p *provision) { p.Status = backend.ProvisionStatusFailed },
		"Quantity":     func(p *provision) { p.Quantity++ },
		"CreatedAt":    func(p *provision) { p.CreatedAt = p.CreatedAt.Add(time.Second) },
		"FailCount":    func(p *provision) { p.FailCount++ },
		"LastError":    func(p *provision) { p.LastError += " changed" },
		"Reason":       func(p *provision) { p.Reason = backend.ReasonInternal },
		"Message":      func(p *provision) { p.Message = "changed" },
		"CallbackURL":  func(p *provision) { p.CallbackURL += "?changed=1" },
		"LifecycleCallbackURL": func(p *provision) {
			p.LifecycleCallbackURL = "https://example.test/callbacks/lifecycle"
		},
		"ActiveReleaseVersion": func(p *provision) { p.ActiveReleaseVersion++ },
		"ActiveOperationID": func(p *provision) {
			p.ActiveOperationID = mustDockerOperationID("22222222-2222-4222-8222-222222222222")
		},
		"Items": func(p *provision) {
			p.Items[0].Quantity++
		},
		"ResourceProfiles": func(p *provision) {
			p.ResourceProfiles = []shared.SKUResourceSnapshot{{SKU: "changed"}}
		},
		"ContainerIDs": func(p *provision) {
			p.ContainerIDs[0] = "changed"
		},
		"StackManifest": func(p *provision) {
			p.StackManifest = &manifest.StackManifest{}
		},
		"ServiceContainers": func(p *provision) {
			p.ServiceContainers["app"][0] = "changed"
		},
	}

	typeOfState := reflect.TypeFor[leasesm.ProvisionState]()
	fields := make([]string, 0, typeOfState.NumField())
	for index := range typeOfState.NumField() {
		fields = append(fields, typeOfState.Field(index).Name)
	}
	slices.Sort(fields)
	want := slices.Collect(maps.Keys(mutations))
	slices.Sort(want)
	require.Equal(t, want, fields,
		"add a field mutation whenever ProvisionState gains or loses a field")

	for field, mutate := range mutations {
		t.Run(field, func(t *testing.T) {
			p := fullRecoveredProvision().materialize()
			snapshot := recoveredFromProvision(p)
			require.True(t, provisionMatchesRecovered(p, snapshot))
			mutate(p)
			assert.False(t, provisionMatchesRecovered(p, snapshot),
				"provisionStateMatches must compare %s", field)
		})
	}
}
