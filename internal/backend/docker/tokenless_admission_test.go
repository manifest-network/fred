package docker

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

// These requests are otherwise valid, including their retained source. Neither
// an explicit tokenless pair nor lifecycle-URL normalization may turn an
// observational callback into authority for new physical work.
func TestNewOperationRejectsTokenlessCallbackBeforeAdmission(t *testing.T) {
	const callbackURL = "https://fred.example/callbacks/provision"
	const destination = "88888888-8888-4888-8888-888888888888"
	const source = "99999999-9999-4999-8999-999999999999"
	for _, operation := range []string{"provision", "restore"} {
		for _, lifecycle := range []string{"omitted", "explicit tokenless"} {
			t.Run(operation+"/"+lifecycle, func(t *testing.T) {
				var mutations atomic.Int32
				refuseMutation := func() error {
					mutations.Add(1)
					return assert.AnError
				}
				mock := &mockDockerClient{
					PullImageFn: func(context.Context, string, time.Duration) error { return refuseMutation() },
					CreateContainerFn: func(context.Context, CreateContainerParams, time.Duration) (string, error) {
						return "", refuseMutation()
					},
					StartContainerFn:  func(context.Context, string, time.Duration) error { return refuseMutation() },
					StopContainerFn:   func(context.Context, string, time.Duration) error { return refuseMutation() },
					RenameContainerFn: func(context.Context, string, string) error { return refuseMutation() },
					RemoveContainerFn: func(context.Context, string) error { return refuseMutation() },
				}
				b := newBackendForProvisionTest(t, mock, nil)
				t.Cleanup(func() { b.stopCancel(); b.wg.Wait() })
				b.compose = &mockComposeExecutor{
					UpFn:   func(context.Context, *composetypes.Project, composeUpOpts) error { return refuseMutation() },
					DownFn: func(context.Context, string, time.Duration) error { return refuseMutation() },
				}
				b.volumes = &mockVolumeManager{
					CreateFn: func(context.Context, string, int64) (string, bool, error) {
						return "", false, refuseMutation()
					},
					EnsureQuotaFn:  func(context.Context, string, int64) error { return refuseMutation() },
					DestroyFn:      func(context.Context, string) error { return refuseMutation() },
					RenameVolumeFn: func(string, string) error { return refuseMutation() },
				}
				retained := seedActiveRetained(t, b.retentionStore, source)
				payload, err := json.Marshal(restoreStackManifest())
				require.NoError(t, err)
				lifecycleURL := ""
				if lifecycle == "explicit tokenless" {
					lifecycleURL = callbackURL
				}
				beforeJournals := restoreAuthorityJournalBytes(t, b)
				beforeResources := b.pool.Stats()
				if operation == "provision" {
					request := newProvisionRequest(destination, "tenant-a", "docker-small", 1, payload)
					request.CallbackURL = callbackURL // Deliberately bypass the typed test-request convenience.
					request.LifecycleCallbackURL = lifecycleURL
					err = b.Provision(t.Context(), request)
				} else {
					request := restoreRequest(destination, source, callbackURL)
					request.CallbackURL = callbackURL
					request.LifecycleCallbackURL = lifecycleURL
					err = b.Restore(t.Context(), request)
				}
				assert.ErrorIs(t, err, backend.ErrValidation,
					"the HTTP adapter must return definitive validation refusal, not an ambiguous 500")
				assert.ErrorContains(t, err, "runtime authority requires a canonical UUIDv4 operation ID")
				assert.Equal(t, beforeJournals, restoreAuthorityJournalBytes(t, b), "rejection must precede the write-ahead barrier")
				assert.Equal(t, beforeResources, b.pool.Stats())
				assert.Empty(t, b.pool.ListAllocations())
				assert.Empty(t, b.provisions)
				assert.Empty(t, b.DebugActors())
				assert.Zero(t, mutations.Load(), "rejection must precede container, Compose, and volume work")
				pending, err := b.operationSettlement.ListOperationIntents()
				require.NoError(t, err)
				assert.Empty(t, pending)
				afterRetained, err := b.retentionStore.Get(source)
				require.NoError(t, err)
				assert.Equal(t, &retained, afterRetained, "a rejected destination must not claim or advance its source")
			})
		}
	}
}
