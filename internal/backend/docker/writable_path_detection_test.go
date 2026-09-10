package docker

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInspectImageForSetup_WritablePathCacheUsesCompleteDetectionSubject(t *testing.T) {
	imageID := fixtureImageID("multiple-runtime-users")
	expected := map[int][]string{
		0:    {"/var/lib/first", "/var/lib/second"},
		1000: {"/var/lib/first"},
		2000: nil,
	}
	detections := make(map[int]int)
	mock := &mockDockerClient{
		InspectImageFn: func(context.Context, string) (*ImageInfo, error) {
			return &ImageInfo{ID: imageID}, nil
		},
		ResolveImageUserFn: func(_ context.Context, _ string, user string) (int, int, error) {
			uid, err := strconv.Atoi(user)
			return uid, uid, err
		},
		DetectWritablePathsFn: func(_ context.Context, gotImageID string, uid int, _ []string) ([]string, error) {
			require.Equal(t, imageID, gotImageID)
			detections[uid]++
			paths, found := expected[uid]
			require.True(t, found, "unexpected detection UID %d", uid)
			return paths, nil
		},
	}
	b := newBackendForTest(mock, nil)
	t.Cleanup(b.stopCancel)

	// These references resolve to the same immutable image. The first pass
	// detects each user's paths independently; the alias must then reuse each
	// complete query, including the successful empty result for UID 2000.
	for _, reference := range []string{"tenant/app:latest", "tenant/app:alias"} {
		for _, uid := range []int{1000, 2000, 0} {
			setup, err := inspectImageForSetupForTest(t, b, t.Context(), reference, strconv.Itoa(uid))
			require.NoError(t, err)
			assert.Equal(t, expected[uid], setup.WritablePaths, "image %s, UID %d", reference, uid)
		}
	}
	assert.Equal(t, map[int]int{0: 1, 1000: 1, 2000: 1}, detections)
}

func TestInspectImageForSetup_WritablePathCacheSeparatesImageContent(t *testing.T) {
	detections := 0
	mock := &mockDockerClient{
		InspectImageFn: func(_ context.Context, reference string) (*ImageInfo, error) {
			return &ImageInfo{ID: fixtureImageID(reference), User: "1000"}, nil
		},
		ResolveImageUserFn: func(context.Context, string, string) (int, int, error) {
			return 1000, 1000, nil
		},
		DetectWritablePathsFn: func(_ context.Context, _ string, _ int, _ []string) ([]string, error) {
			detections++
			return []string{fmt.Sprintf("/var/lib/release-%d", detections)}, nil
		},
	}
	b := newBackendForTest(mock, nil)
	t.Cleanup(b.stopCancel)
	for index, reference := range []string{"tenant/app:v1", "tenant/app:v2", "tenant/app:v1"} {
		setup, err := inspectImageForSetupForTest(t, b, t.Context(), reference, "")
		require.NoError(t, err)
		want := "/var/lib/release-1"
		if index == 1 {
			want = "/var/lib/release-2"
		}
		assert.Equal(t, []string{want}, setup.WritablePaths)
		setup.WritablePaths[0] = "/caller-mutated"
	}
	assert.Equal(t, 2, detections)
}
