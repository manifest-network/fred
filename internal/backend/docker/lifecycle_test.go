package docker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseLabelMeta(t *testing.T) {
	t.Run("all labels present and valid", func(t *testing.T) {
		labels := map[string]string{
			LabelInstanceIndex: "3",
			LabelFailCount:     "2",
			LabelCreatedAt:     "2025-01-15T10:30:00Z",
		}
		meta, err := parseLabelMeta(labels)
		require.NoError(t, err)
		assert.Equal(t, 3, meta.InstanceIndex)
		assert.Equal(t, 2, meta.FailCount)
		assert.Equal(t, time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC), meta.CreatedAt)
	})

	t.Run("empty labels returns zero values", func(t *testing.T) {
		meta, err := parseLabelMeta(map[string]string{})
		require.NoError(t, err)
		assert.Equal(t, 0, meta.InstanceIndex)
		assert.Equal(t, 0, meta.FailCount)
		assert.True(t, meta.CreatedAt.IsZero())
	})

	t.Run("nil labels returns zero values", func(t *testing.T) {
		meta, err := parseLabelMeta(nil)
		require.NoError(t, err)
		assert.Equal(t, 0, meta.InstanceIndex)
		assert.Equal(t, 0, meta.FailCount)
		assert.True(t, meta.CreatedAt.IsZero())
	})

	t.Run("malformed instance_index returns error", func(t *testing.T) {
		labels := map[string]string{
			LabelInstanceIndex: "not-a-number",
		}
		_, err := parseLabelMeta(labels)
		require.Error(t, err)
		assert.Contains(t, err.Error(), LabelInstanceIndex)
		assert.Contains(t, err.Error(), "not-a-number")
	})

	t.Run("malformed fail_count returns error", func(t *testing.T) {
		labels := map[string]string{
			LabelFailCount: "abc",
		}
		_, err := parseLabelMeta(labels)
		require.Error(t, err)
		assert.Contains(t, err.Error(), LabelFailCount)
		assert.Contains(t, err.Error(), "abc")
	})

	t.Run("malformed created_at returns error", func(t *testing.T) {
		labels := map[string]string{
			LabelCreatedAt: "not-a-timestamp",
		}
		_, err := parseLabelMeta(labels)
		require.Error(t, err)
		assert.Contains(t, err.Error(), LabelCreatedAt)
		assert.Contains(t, err.Error(), "not-a-timestamp")
	})

	t.Run("partial labels are valid", func(t *testing.T) {
		labels := map[string]string{
			LabelInstanceIndex: "5",
			// No FailCount or CreatedAt
		}
		meta, err := parseLabelMeta(labels)
		require.NoError(t, err)
		assert.Equal(t, 5, meta.InstanceIndex)
		assert.Equal(t, 0, meta.FailCount)
		assert.True(t, meta.CreatedAt.IsZero())
	})

	t.Run("zero values are valid", func(t *testing.T) {
		labels := map[string]string{
			LabelInstanceIndex: "0",
			LabelFailCount:     "0",
		}
		meta, err := parseLabelMeta(labels)
		require.NoError(t, err)
		assert.Equal(t, 0, meta.InstanceIndex)
		assert.Equal(t, 0, meta.FailCount)
	})
}

func TestSplitUserGroup(t *testing.T) {
	tests := []struct {
		input     string
		wantUser  string
		wantGroup string
	}{
		{"", "", ""},
		{"999", "999", ""},
		{"999:999", "999", "999"},
		{"999:100", "999", "100"},
		{"postgres", "postgres", ""},
		{"postgres:postgres", "postgres", "postgres"},
		{"user:group", "user", "group"},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			user, group := splitUserGroup(tc.input)
			assert.Equal(t, tc.wantUser, user)
			assert.Equal(t, tc.wantGroup, group)
		})
	}
}

func TestParsePasswdForUser(t *testing.T) {
	passwd := `root:x:0:0:root:/root:/bin/bash
daemon:x:1:1:daemon:/usr/sbin:/usr/sbin/nologin
postgres:x:999:999:PostgreSQL administrator:/var/lib/postgresql:/bin/bash
nobody:x:65534:65534:nobody:/nonexistent:/usr/sbin/nologin
`

	t.Run("finds postgres user", func(t *testing.T) {
		uid, gid, err := parsePasswdForUser(strings.NewReader(passwd), "postgres")
		require.NoError(t, err)
		assert.Equal(t, 999, uid)
		assert.Equal(t, 999, gid)
	})

	t.Run("finds root user", func(t *testing.T) {
		uid, gid, err := parsePasswdForUser(strings.NewReader(passwd), "root")
		require.NoError(t, err)
		assert.Equal(t, 0, uid)
		assert.Equal(t, 0, gid)
	})

	t.Run("finds nobody user", func(t *testing.T) {
		uid, gid, err := parsePasswdForUser(strings.NewReader(passwd), "nobody")
		require.NoError(t, err)
		assert.Equal(t, 65534, uid)
		assert.Equal(t, 65534, gid)
	})

	t.Run("user not found returns error", func(t *testing.T) {
		_, _, err := parsePasswdForUser(strings.NewReader(passwd), "nonexistent")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("skips comments and empty lines", func(t *testing.T) {
		data := `# comment
root:x:0:0:root:/root:/bin/bash

myuser:x:1000:1000::/home/myuser:/bin/sh
`
		uid, gid, err := parsePasswdForUser(strings.NewReader(data), "myuser")
		require.NoError(t, err)
		assert.Equal(t, 1000, uid)
		assert.Equal(t, 1000, gid)
	})
}

func TestParseGroupForName(t *testing.T) {
	group := `root:x:0:
daemon:x:1:
postgres:x:999:
nogroup:x:65534:
`

	t.Run("finds postgres group", func(t *testing.T) {
		gid, err := parseGroupForName(strings.NewReader(group), "postgres")
		require.NoError(t, err)
		assert.Equal(t, 999, gid)
	})

	t.Run("finds root group", func(t *testing.T) {
		gid, err := parseGroupForName(strings.NewReader(group), "root")
		require.NoError(t, err)
		assert.Equal(t, 0, gid)
	})

	t.Run("group not found returns error", func(t *testing.T) {
		_, err := parseGroupForName(strings.NewReader(group), "nonexistent")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})
}

func TestResolveImageUserNumeric(t *testing.T) {
	mock := &mockDockerClient{
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			// Simulate the logic: numeric UID, no group → UID=GID
			return 999, 999, nil
		},
	}

	uid, gid, err := mock.ResolveImageUser(context.TODO(), "postgres:16", "")
	require.NoError(t, err)
	assert.Equal(t, 999, uid)
	assert.Equal(t, 999, gid)
}

func TestResolveImageUserNumericWithGroup(t *testing.T) {
	mock := &mockDockerClient{
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 999, 100, nil
		},
	}

	uid, gid, err := mock.ResolveImageUser(context.TODO(), "test-image", "")
	require.NoError(t, err)
	assert.Equal(t, 999, uid)
	assert.Equal(t, 100, gid)
}

func TestResolveImageUserEmpty(t *testing.T) {
	mock := &mockDockerClient{
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 0, 0, nil
		},
	}

	uid, gid, err := mock.ResolveImageUser(context.TODO(), "nginx:latest", "")
	require.NoError(t, err)
	assert.Equal(t, 0, uid)
	assert.Equal(t, 0, gid)
}

func TestResolveImageUserManifestOverride(t *testing.T) {
	// When manifest specifies a user, it should be used instead of image Config.User.
	mock := &mockDockerClient{
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			assert.Equal(t, "postgres", userOverride, "manifest user should be passed as override")
			return 999, 999, nil
		},
	}

	uid, gid, err := mock.ResolveImageUser(context.TODO(), "postgres:16", "postgres")
	require.NoError(t, err)
	assert.Equal(t, 999, uid)
	assert.Equal(t, 999, gid)
}
