package docker

import (
	"archive/tar"
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	networktypes "github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"

	"github.com/manifest-network/fred/internal/backend"
)

// Labels used for tracking managed containers.
const (
	LabelManaged       = "fred.managed"
	LabelLeaseUUID     = "fred.lease_uuid"
	LabelTenant        = "fred.tenant"
	LabelProviderUUID  = "fred.provider_uuid"
	LabelSKU           = "fred.sku"
	LabelCreatedAt     = "fred.created_at"
	LabelInstanceIndex = "fred.instance_index"
	LabelFailCount     = "fred.fail_count"
	LabelCallbackURL   = "fred.callback_url"
	LabelBackendName   = "fred.backend_name"
	LabelServiceName   = "fred.service_name"
	LabelFQDN          = "fred.fqdn"
)

// DaemonSecurityInfo contains Docker daemon capabilities relevant to
// container hardening validation.
type DaemonSecurityInfo struct {
	StorageDriver     string
	BackingFilesystem string
	SecurityOptions   []string
	IPv4Forwarding    bool
	Warnings          []string
}

// ContainerInfo holds information about a managed container.
type ContainerInfo struct {
	ContainerID   string
	LeaseUUID     string
	Tenant        string
	ProviderUUID  string
	SKU           string
	ServiceName   string // Stack service name (empty for legacy single-container leases)
	InstanceIndex int
	FailCount     int
	CallbackURL   string
	Image         string
	Status        string
	Health        HealthStatus // Health check status (HealthStatusHealthy, HealthStatusUnhealthy, HealthStatusStarting, or HealthStatusNone)
	ExitCode      int          // Process exit code (meaningful when Status is "exited"/"dead")
	OOMKilled     bool         // True if container was killed by the OOM killer
	CreatedAt     time.Time
	Ports         map[string]PortBinding
	FQDN          string
}

// PortBinding represents a port mapping.
type PortBinding struct {
	HostIP   string `json:"host_ip"`
	HostPort string `json:"host_port"`
}

// DockerClient wraps the Docker client for container lifecycle operations.
type DockerClient struct {
	client      *client.Client
	backendName string
}

// NewDockerClient creates a new Docker client. The backendName parameter scopes
// all list/filter/event operations so that multiple backend instances sharing
// the same Docker daemon do not interfere with each other.
func NewDockerClient(host string, backendName string) (*DockerClient, error) {
	opts := []client.Opt{
		client.WithAPIVersionNegotiation(),
	}

	if host != "" {
		opts = append(opts, client.WithHost(host))
	}

	cli, err := client.NewClientWithOpts(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}

	return &DockerClient{client: cli, backendName: backendName}, nil
}

// Close closes the Docker client.
func (d *DockerClient) Close() error {
	return d.client.Close()
}

// Ping checks connectivity to the Docker daemon.
func (d *DockerClient) Ping(ctx context.Context) error {
	_, err := d.client.Ping(ctx)
	return err
}

// DaemonInfo returns Docker daemon capabilities for hardening validation.
func (d *DockerClient) DaemonInfo(ctx context.Context) (DaemonSecurityInfo, error) {
	info, err := d.client.Info(ctx)
	if err != nil {
		return DaemonSecurityInfo{}, fmt.Errorf("failed to get daemon info: %w", err)
	}

	var backingFS string
	for _, pair := range info.DriverStatus {
		if len(pair) == 2 && pair[0] == "Backing Filesystem" {
			backingFS = pair[1]
			break
		}
	}

	return DaemonSecurityInfo{
		StorageDriver:     info.Driver,
		BackingFilesystem: backingFS,
		SecurityOptions:   info.SecurityOptions,
		IPv4Forwarding:    info.IPv4Forwarding,
		Warnings:          info.Warnings,
	}, nil
}

// ImageInfo holds metadata from a container image inspection.
type ImageInfo struct {
	// ID is the content-addressable image ID (e.g., "sha256:...").
	// Immutable for a given image build, suitable as a cache key.
	ID string
	// Volumes are the VOLUME declarations from the Dockerfile.
	Volumes map[string]struct{}
	// User is the USER directive from the Dockerfile (may be name, uid, uid:gid, or name:group).
	User string
}

// InspectImage inspects a pulled image and returns its metadata.
func (d *DockerClient) InspectImage(ctx context.Context, imageName string) (*ImageInfo, error) {
	inspect, _, err := d.client.ImageInspectWithRaw(ctx, imageName)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect image %s: %w", imageName, err)
	}
	volumes := make(map[string]struct{})
	if inspect.Config != nil {
		for v := range inspect.Config.Volumes {
			volumes[v] = struct{}{}
		}
	}
	info := &ImageInfo{ID: inspect.ID, Volumes: volumes}
	if inspect.Config != nil {
		info.User = inspect.Config.User
	}
	return info, nil
}

// ResolveImageUser resolves a container user specification to numeric UID/GID.
// If userOverride is non-empty, it is used instead of the image's Config.User.
// This is needed for images like postgres that start as root and expect to
// chown data directories — since we drop CAP_CHOWN, the manifest must
// specify the target user explicitly.
// If both userOverride and Config.User are empty, returns (0, 0, nil) (root).
// Numeric UID/GID values are parsed directly. Non-numeric usernames are
// resolved by reading /etc/passwd (and optionally /etc/group) from a
// temporary container created from the image.
func (d *DockerClient) ResolveImageUser(ctx context.Context, imageName string, userOverride string) (uid, gid int, err error) {
	userStr := userOverride
	if userStr == "" {
		inspect, _, inspectErr := d.client.ImageInspectWithRaw(ctx, imageName)
		if inspectErr != nil {
			return 0, 0, fmt.Errorf("failed to inspect image %s: %w", imageName, inspectErr)
		}
		if inspect.Config != nil {
			userStr = inspect.Config.User
		}
	}
	if userStr == "" {
		return 0, 0, nil
	}

	user, group := splitUserGroup(userStr)

	// Try numeric UID.
	numUID, uidErr := strconv.Atoi(user)
	if uidErr == nil {
		// UID is numeric. Resolve GID.
		if group == "" {
			return numUID, numUID, nil
		}
		numGID, gidErr := strconv.Atoi(group)
		if gidErr == nil {
			return numUID, numGID, nil
		}
		// Group is a name — need to read /etc/group from the image.
		gidResolved, err := d.resolveGroupFromImage(ctx, imageName, group)
		if err != nil {
			return 0, 0, err
		}
		return numUID, gidResolved, nil
	}

	// Username is non-numeric — read /etc/passwd from the image.
	passwdUID, passwdGID, err := d.resolveUserFromImage(ctx, imageName, user)
	if err != nil {
		return 0, 0, err
	}

	if group == "" {
		return passwdUID, passwdGID, nil
	}

	// Explicit group override.
	numGID, gidErr := strconv.Atoi(group)
	if gidErr == nil {
		return passwdUID, numGID, nil
	}
	gidResolved, err := d.resolveGroupFromImage(ctx, imageName, group)
	if err != nil {
		return 0, 0, err
	}
	return passwdUID, gidResolved, nil
}

// resolveUserFromImage reads /etc/passwd from a temporary container to resolve
// a username to UID/GID.
func (d *DockerClient) resolveUserFromImage(ctx context.Context, imageName, username string) (uid, gid int, err error) {
	data, err := d.readFileFromImage(ctx, imageName, "/etc/passwd")
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read /etc/passwd from image %s: %w", imageName, err)
	}
	return parsePasswdForUser(strings.NewReader(string(data)), username)
}

// resolveGroupFromImage reads /etc/group from a temporary container to resolve
// a group name to GID.
func (d *DockerClient) resolveGroupFromImage(ctx context.Context, imageName, groupName string) (int, error) {
	data, err := d.readFileFromImage(ctx, imageName, "/etc/group")
	if err != nil {
		return 0, fmt.Errorf("failed to read /etc/group from image %s: %w", imageName, err)
	}
	return parseGroupForName(strings.NewReader(string(data)), groupName)
}

// readFileFromImage creates a temporary container (never started), extracts a
// file via CopyFromContainer, and removes the container.
func (d *DockerClient) readFileFromImage(ctx context.Context, imageName, path string) ([]byte, error) {
	resp, err := d.client.ContainerCreate(ctx, &container.Config{
		Image: imageName,
	}, nil, nil, nil, "")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp container: %w", err)
	}
	defer func() {
		_ = d.client.ContainerRemove(ctx, resp.ID, container.RemoveOptions{})
	}()

	return readFileFromContainer(ctx, d.client, resp.ID, path)
}

// DetectVolumeOwner inspects the ownership of VOLUME directories inside an
// image by creating a temporary container (never started) and reading the tar
// headers from CopyFromContainer. If all volume paths share the same non-root
// UID:GID, those values are returned. If the paths have mixed ownership, are
// owned by root, or a path doesn't exist, (0, 0, nil) is returned.
func (d *DockerClient) DetectVolumeOwner(ctx context.Context, imageName string, volumePaths []string) (uid, gid int, err error) {
	if len(volumePaths) == 0 {
		return 0, 0, nil
	}

	resp, err := d.client.ContainerCreate(ctx, &container.Config{
		Image: imageName,
	}, nil, nil, nil, "")
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create temp container for volume owner detection: %w", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = d.client.ContainerRemove(cleanupCtx, resp.ID, container.RemoveOptions{})
	}()

	firstUID, firstGID := -1, -1
	for _, volPath := range volumePaths {
		rc, _, copyErr := d.client.CopyFromContainer(ctx, resp.ID, volPath)
		if copyErr != nil {
			if errdefs.IsNotFound(copyErr) {
				// Path genuinely doesn't exist in image.
				return 0, 0, nil
			}
			return 0, 0, fmt.Errorf("CopyFromContainer %s: %w", volPath, copyErr)
		}

		tr := tar.NewReader(rc)
		hdr, tarErr := tr.Next()
		_ = rc.Close()
		if tarErr != nil {
			if errors.Is(tarErr, io.EOF) {
				// Empty tar stream — directory exists but has no entries.
				return 0, 0, nil
			}
			return 0, 0, fmt.Errorf("reading tar header for %s: %w", volPath, tarErr)
		}

		if firstUID == -1 {
			firstUID = hdr.Uid
			firstGID = hdr.Gid
		} else if hdr.Uid != firstUID || hdr.Gid != firstGID {
			// Mixed ownership across volume paths.
			return 0, 0, nil
		}
	}

	if firstUID == 0 || firstGID == 0 {
		return 0, 0, nil
	}

	return firstUID, firstGID, nil
}

// candidateWritableParents are common parent directories scanned for
// subdirectories owned by the container user (e.g., grafana/grafana
// chowns /var/lib/grafana, mysql:9 chowns /var/run/mysqld).
var candidateWritableParents = []string{"/var/lib", "/var/log", "/var/run", "/var/cache", "/data", "/home", "/opt", "/srv"}

// maxDetectedWritablePaths caps the number of auto-detected writable paths
// to limit tmpfs mounts on images with many user-owned directories.
const maxDetectedWritablePaths = 4

// maxTarEntriesPerParent caps the number of tar entries scanned per candidate
// parent to bound I/O on large directories.
const maxTarEntriesPerParent = 500

// DetectWritablePaths scans candidate parent directories inside an image for
// depth-1 subdirectories owned by uid. When uid is 0 (root image), it matches
// directories owned by any non-root user — this handles images like neo4j that
// run as root but chown directories to a service user during build.
func (d *DockerClient) DetectWritablePaths(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error) {
	if len(candidateParents) == 0 {
		return nil, nil
	}

	resp, err := d.client.ContainerCreate(ctx, &container.Config{
		Image: imageName,
	}, nil, nil, nil, "")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp container for writable path detection: %w", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = d.client.ContainerRemove(cleanupCtx, resp.ID, container.RemoveOptions{})
	}()

	var paths []string
	for _, parent := range candidateParents {
		if len(paths) >= maxDetectedWritablePaths {
			break
		}

		rc, _, copyErr := d.client.CopyFromContainer(ctx, resp.ID, parent)
		if copyErr != nil {
			if !errdefs.IsNotFound(copyErr) {
				// Non-"not found" errors (daemon unreachable, context canceled, etc.)
				// mean detection is incomplete — return what we have so far.
				return paths, fmt.Errorf("CopyFromContainer %s: %w", parent, copyErr)
			}
			// Parent doesn't exist in image — skip.
			continue
		}

		tr := tar.NewReader(rc)
		scanned := 0
		for scanned < maxTarEntriesPerParent {
			hdr, tarErr := tr.Next()
			if tarErr != nil {
				break // EOF or read error — either way, done with this parent
			}
			scanned++

			if hdr.Typeflag != tar.TypeDir {
				continue
			}

			// CopyFromContainer("/var/lib") returns entries like:
			//   "lib/"            (the parent itself)
			//   "lib/grafana/"    (depth-1 child — what we want)
			//   "lib/grafana/plugins/" (depth-2 — too deep)
			// We match entries with exactly 2 path components.
			name := strings.TrimSuffix(hdr.Name, "/")
			parts := strings.Split(name, "/")
			if len(parts) != 2 {
				continue
			}

			// Match specific uid, or any non-root uid when uid == 0.
			match := (uid != 0 && hdr.Uid == uid) || (uid == 0 && hdr.Uid != 0)
			if match {
				fullPath := parent + "/" + parts[1]
				paths = append(paths, fullPath)
				if len(paths) >= maxDetectedWritablePaths {
					break
				}
			}
		}
		_ = rc.Close()
	}

	return paths, nil
}

// ExtractImageContent extracts pre-existing image content for the given paths
// into destDir on the host filesystem. For each path, it creates a temporary
// container from the image (never started), streams the tar archive of that
// path via CopyFromContainer, sanitizes the tar stream, and extracts it to
// the appropriate subdirectory under destDir.
//
// Returns nil on full success, or a map of path → error for failures.
// Callers should log failures but not fail the provision (graceful degradation).
func (d *DockerClient) ExtractImageContent(ctx context.Context, imageName string, paths []string, destDir string, maxBytes int64) map[string]error {
	if len(paths) == 0 {
		return nil
	}

	// Create a temp container from the image to read original content.
	resp, err := d.client.ContainerCreate(ctx, &container.Config{
		Image: imageName,
	}, nil, nil, nil, "")
	if err != nil {
		// All paths fail with the same error.
		failures := make(map[string]error, len(paths))
		for _, p := range paths {
			failures[p] = fmt.Errorf("failed to create temp container for extraction: %w", err)
		}
		return failures
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = d.client.ContainerRemove(cleanupCtx, resp.ID, container.RemoveOptions{})
	}()

	var failures map[string]error
	remainingBytes := maxBytes
	for _, path := range paths {
		rc, _, copyErr := d.client.CopyFromContainer(ctx, resp.ID, path)
		if copyErr != nil {
			if errdefs.IsNotFound(copyErr) {
				continue // Path doesn't exist in image — skip
			}
			if failures == nil {
				failures = make(map[string]error)
			}
			failures[path] = fmt.Errorf("CopyFromContainer %s: %w", path, copyErr)
			continue
		}

		// CopyFromContainer("/var/lib/neo4j") returns tar entries like
		// "neo4j/conf/neo4j.conf". Extract to destDir/var/lib/ so content
		// ends up at destDir/var/lib/neo4j/conf/neo4j.conf.
		sanitized := sanitizeVolumePath(path)
		extractDir := filepath.Join(destDir, filepath.Dir(sanitized))
		if mkErr := os.MkdirAll(extractDir, 0o700); mkErr != nil {
			_ = rc.Close()
			if failures == nil {
				failures = make(map[string]error)
			}
			failures[path] = fmt.Errorf("mkdir %s: %w", extractDir, mkErr)
			continue
		}

		written, skippedSymlinks, extractErr := sanitizeAndExtractTar(rc, extractDir, remainingBytes)
		_ = rc.Close()
		if extractErr != nil {
			if failures == nil {
				failures = make(map[string]error)
			}
			failures[path] = fmt.Errorf("extract %s: %w", path, extractErr)
			continue
		}
		for _, sl := range skippedSymlinks {
			slog.Debug("extracted symlink with out-of-scope target", "path", path, "symlink", sl)
		}
		remainingBytes -= written
	}

	return failures
}

// sanitizeAndExtractTar reads a tar stream and extracts entries into destDir
// with security checks. It rejects absolute paths, path traversal via "..",
// and device nodes. Setuid/setgid bits are stripped. Total bytes written are
// tracked and an error is returned if maxBytes is exceeded. File ownership
// from tar headers is preserved when running as root; EPERM errors from chown
// are silently ignored.
//
// Symlinks whose targets point outside destDir (absolute paths, ".." traversal)
// are still created — they resolve correctly inside the container once
// bind-mounted. The returned slice contains descriptions of such out-of-scope
// symlinks (e.g., "name -> target") for caller-side debug logging.
func sanitizeAndExtractTar(src io.Reader, destDir string, maxBytes int64) (int64, []string, error) {
	tr := tar.NewReader(src)
	var totalBytes int64
	var outOfScope []string

	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return totalBytes, outOfScope, fmt.Errorf("reading tar header: %w", err)
		}

		// Reject absolute paths and path traversal.
		clean := filepath.Clean(hdr.Name)
		if filepath.IsAbs(clean) || strings.HasPrefix(clean, "..") {
			return totalBytes, outOfScope, fmt.Errorf("tar entry has unsafe path: %q", hdr.Name)
		}

		target := filepath.Join(destDir, clean)
		// Verify resolved path stays within destDir.
		if !strings.HasPrefix(target, filepath.Clean(destDir)+string(filepath.Separator)) && target != filepath.Clean(destDir) {
			return totalBytes, outOfScope, fmt.Errorf("tar entry escapes destination: %q", hdr.Name)
		}

		// Strip setuid/setgid bits.
		hdr.Mode &^= 0o6000

		switch hdr.Typeflag {
		case tar.TypeDir:
			if mkErr := os.MkdirAll(target, os.FileMode(hdr.Mode)&os.ModePerm|0o700); mkErr != nil {
				return totalBytes, outOfScope, fmt.Errorf("mkdir %s: %w", clean, mkErr)
			}
			if chErr := os.Chown(target, hdr.Uid, hdr.Gid); chErr != nil && !errors.Is(chErr, syscall.EPERM) {
				return totalBytes, outOfScope, fmt.Errorf("chown dir %s: %w", clean, chErr)
			}

		case tar.TypeReg:
			// Note: OpenFile could follow a symlink created earlier in the archive,
			// writing outside destDir despite the prefix check. This is not exploitable
			// here because the tar source is always Docker's CopyFromContainer API,
			// which walks the filesystem without following symlinks — so a symlink entry
			// is never followed by regular file entries "inside" it.
			if totalBytes+hdr.Size > maxBytes {
				return totalBytes, outOfScope, fmt.Errorf("tar extraction exceeds %d byte limit", maxBytes)
			}
			if mkErr := os.MkdirAll(filepath.Dir(target), 0o700); mkErr != nil {
				return totalBytes, outOfScope, fmt.Errorf("mkdir for %s: %w", clean, mkErr)
			}
			f, fErr := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode)&os.ModePerm)
			if fErr != nil {
				return totalBytes, outOfScope, fmt.Errorf("create %s: %w", clean, fErr)
			}
			n, copyErr := io.Copy(f, tr)
			closeErr := f.Close()
			if copyErr != nil {
				return totalBytes, outOfScope, fmt.Errorf("write %s: %w", clean, copyErr)
			}
			if closeErr != nil {
				return totalBytes, outOfScope, fmt.Errorf("close %s: %w", clean, closeErr)
			}
			totalBytes += n
			if chErr := os.Chown(target, hdr.Uid, hdr.Gid); chErr != nil && !errors.Is(chErr, syscall.EPERM) {
				return totalBytes, outOfScope, fmt.Errorf("chown %s: %w", clean, chErr)
			}

		case tar.TypeSymlink:
			// Symlinks with out-of-scope targets (absolute or ..-prefixed) are
			// created as-is. They'll be dangling on the host but resolve correctly
			// inside the container once bind-mounted (e.g., neo4j: data -> /data).
			// Docker's CopyFromContainer doesn't follow symlinks when building the
			// tar, so subsequent entries won't traverse through them on the host.
			escaped := filepath.IsAbs(hdr.Linkname) || strings.HasPrefix(filepath.Clean(hdr.Linkname), "..")
			if !escaped {
				// For relative symlinks, verify the resolved target stays within destDir.
				resolved := filepath.Join(filepath.Dir(target), hdr.Linkname)
				if !strings.HasPrefix(resolved, filepath.Clean(destDir)+string(filepath.Separator)) && resolved != filepath.Clean(destDir) {
					escaped = true
				}
			}
			if escaped {
				outOfScope = append(outOfScope, fmt.Sprintf("%s -> %s", hdr.Name, hdr.Linkname))
			}
			if mkErr := os.MkdirAll(filepath.Dir(target), 0o700); mkErr != nil {
				return totalBytes, outOfScope, fmt.Errorf("mkdir for symlink %s: %w", clean, mkErr)
			}
			if symlinkErr := os.Symlink(hdr.Linkname, target); symlinkErr != nil {
				return totalBytes, outOfScope, fmt.Errorf("symlink %s -> %s: %w", clean, hdr.Linkname, symlinkErr)
			}
			if chErr := os.Lchown(target, hdr.Uid, hdr.Gid); chErr != nil && !errors.Is(chErr, syscall.EPERM) {
				return totalBytes, outOfScope, fmt.Errorf("lchown symlink %s: %w", clean, chErr)
			}

		default:
			// Reject device nodes, char devices, sockets, etc.
			return totalBytes, outOfScope, fmt.Errorf("tar entry has disallowed type %d: %q", hdr.Typeflag, hdr.Name)
		}
	}
	return totalBytes, outOfScope, nil
}

// readFileFromContainer extracts a single file from a container using
// CopyFromContainer and returns its contents.
func readFileFromContainer(ctx context.Context, cli *client.Client, containerID, path string) ([]byte, error) {
	rc, _, err := cli.CopyFromContainer(ctx, containerID, path)
	if err != nil {
		return nil, fmt.Errorf("CopyFromContainer %s: %w", path, err)
	}
	defer func() { _ = rc.Close() }()

	tr := tar.NewReader(rc)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("file %s not found in tar stream", path)
		}
		if err != nil {
			return nil, fmt.Errorf("reading tar: %w", err)
		}
		if hdr.Typeflag == tar.TypeReg {
			// Limit reads to 1 MiB to prevent a malicious image from
			// causing OOM via an oversized /etc/passwd or /etc/group.
			const maxFileSize = 1 << 20
			data, err := io.ReadAll(io.LimitReader(tr, maxFileSize+1))
			if err != nil {
				return nil, fmt.Errorf("reading %s: %w", path, err)
			}
			if len(data) > maxFileSize {
				return nil, fmt.Errorf("file %s exceeds %d byte limit", path, maxFileSize)
			}
			return data, nil
		}
	}
}

// splitUserGroup splits a Docker USER string into user and group parts.
// "user:group" → ("user", "group"), "user" → ("user", "").
func splitUserGroup(s string) (user, group string) {
	if i := strings.IndexByte(s, ':'); i >= 0 {
		return s[:i], s[i+1:]
	}
	return s, ""
}

// parsePasswdForUser parses /etc/passwd content and returns the UID and primary
// GID for the given username. Returns an error if the user is not found.
func parsePasswdForUser(r io.Reader, username string) (uid, gid int, err error) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		fields := strings.Split(line, ":")
		if len(fields) < 4 {
			continue
		}
		if fields[0] != username {
			continue
		}
		uid, err = strconv.Atoi(fields[2])
		if err != nil {
			return 0, 0, fmt.Errorf("invalid UID %q for user %s: %w", fields[2], username, err)
		}
		gid, err = strconv.Atoi(fields[3])
		if err != nil {
			return 0, 0, fmt.Errorf("invalid GID %q for user %s: %w", fields[3], username, err)
		}
		return uid, gid, nil
	}
	if err = scanner.Err(); err != nil {
		return 0, 0, fmt.Errorf("reading passwd: %w", err)
	}
	return 0, 0, fmt.Errorf("user %q not found in /etc/passwd", username)
}

// parseGroupForName parses /etc/group content and returns the GID for the
// given group name. Returns an error if the group is not found.
func parseGroupForName(r io.Reader, groupName string) (gid int, err error) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		fields := strings.Split(line, ":")
		if len(fields) < 3 {
			continue
		}
		if fields[0] != groupName {
			continue
		}
		gid, err = strconv.Atoi(fields[2])
		if err != nil {
			return 0, fmt.Errorf("invalid GID %q for group %s: %w", fields[2], groupName, err)
		}
		return gid, nil
	}
	if err = scanner.Err(); err != nil {
		return 0, fmt.Errorf("reading group: %w", err)
	}
	return 0, fmt.Errorf("group %q not found in /etc/group", groupName)
}

// PullImage pulls a container image with timeout.
func (d *DockerClient) PullImage(ctx context.Context, imageName string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	reader, err := d.client.ImagePull(ctx, imageName, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("failed to pull image: %w", err)
	}
	defer func() { _ = reader.Close() }()

	// The Docker daemon streams JSON progress messages. Errors such as
	// "manifest unknown" are reported inside the stream (as an errorDetail
	// field) rather than as an HTTP-level error. We must decode each
	// message and check for embedded errors; discarding with io.Copy
	// would silently swallow them.
	decoder := json.NewDecoder(reader)
	for {
		var msg jsonPullMessage
		if err := decoder.Decode(&msg); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read image pull output: %w", err)
		}
		if msg.Error != "" {
			return fmt.Errorf("%s", msg.Error)
		}
		if msg.ErrorDetail != nil && msg.ErrorDetail.Message != "" {
			return fmt.Errorf("%s", msg.ErrorDetail.Message)
		}
	}

	return nil
}

// jsonPullMessage is the minimal structure needed to detect errors in the
// Docker daemon's image-pull JSON stream. The daemon sends errors in two
// fields: the deprecated top-level "error" string and the structured
// "errorDetail" object. We check both so that errors are not missed if
// either field is omitted in a future Docker release.
type jsonPullMessage struct {
	Error       string         `json:"error,omitempty"`
	ErrorDetail *jsonPullError `json:"errorDetail,omitempty"`
}

// jsonPullError mirrors the structured error object the Docker daemon embeds
// in image-pull progress messages under the "errorDetail" key.
type jsonPullError struct {
	Message string `json:"message,omitempty"`
}

// CreateContainerParams holds parameters for creating a container.
type CreateContainerParams struct {
	LeaseUUID     string
	Tenant        string
	ProviderUUID  string
	SKU           string
	ServiceName   string // Stack service name (empty for legacy)
	Manifest      *DockerManifest
	Profile       SKUProfile
	InstanceIndex int // For multi-unit leases (0-based index)

	// Retry tracking
	FailCount int

	// CallbackURL is persisted as a label so failure callbacks can be
	// sent after a docker-backend restart (when in-memory state is lost).
	CallbackURL string

	// Hardening parameters
	HostBindIP     string
	ReadonlyRootfs bool
	PidsLimit      *int64
	TmpfsSizeMB    int
	NetworkConfig  *networktypes.NetworkingConfig

	// VolumeBinds are bind mounts from host to container for managed volumes.
	// Each entry maps a host path to a container path.
	// Used for stateful containers (disk_mb > 0).
	VolumeBinds map[string]string

	// ImageVolumes are VOLUME paths from the image that need tmpfs overrides
	// (for ephemeral containers only, when VolumeBinds is nil).
	ImageVolumes []string

	// WritablePathBinds are bind mounts from managed volume subdirectories
	// to auto-detected writable directories in the container. Each entry
	// maps a host path (with pre-extracted image content) to a container path.
	WritablePathBinds map[string]string

	// User overrides the container's runtime user (e.g., "999:999").
	// When set, container.Config.User is set to this value so the container
	// runs directly as the target user instead of relying on the entrypoint
	// to switch users (which requires CAP_CHOWN that we drop).
	User string

	// BackendName identifies the backend instance that created this container,
	// stored as a label to scope list/filter operations per backend.
	BackendName string

	// Ingress holds the reverse proxy configuration.
	// When Enabled, proxy labels are injected into the container.
	// NetworkName must be non-empty when Ingress is enabled (enforced by
	// Config.Validate requiring network_isolation).
	Ingress     IngressConfig
	NetworkName string // Per-tenant network name for traefik.docker.network label
	Quantity    int    // Total quantity for this service (used in subdomain computation)
}

// portBindRetries is the number of times to retry container creation when
// an ephemeral port conflict occurs.
const portBindRetries = 3

// portBindRetryDelay is the delay between retries for ephemeral port conflicts.
const portBindRetryDelay = 500 * time.Millisecond

// isPortBindingError checks if an error is a port binding conflict.
func isPortBindingError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "port is already allocated") ||
		strings.Contains(msg, "address already in use")
}

// hasEphemeralPorts returns true if any port binding uses an ephemeral host
// port (HostPort == 0 or empty, meaning Docker assigns a random port).
// Returns false only when all ports have an explicit HostPort, in which
// case retrying a port conflict won't help.
func hasEphemeralPorts(ports map[string]PortConfig) bool {
	if len(ports) == 0 {
		return false // No ports, no conflicts to retry
	}
	for _, cfg := range ports {
		if cfg.HostPort == 0 {
			return true
		}
	}
	return false
}

// CreateContainer creates a new container with the specified configuration.
// For ephemeral port bindings, it retries up to portBindRetries times on
// port conflict errors since these can be transient.
func (d *DockerClient) CreateContainer(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	// Build labels
	labels := map[string]string{
		LabelManaged:       "true",
		LabelLeaseUUID:     params.LeaseUUID,
		LabelTenant:        params.Tenant,
		LabelProviderUUID:  params.ProviderUUID,
		LabelSKU:           params.SKU,
		LabelCreatedAt:     time.Now().Format(time.RFC3339),
		LabelInstanceIndex: strconv.Itoa(params.InstanceIndex),
		LabelFailCount:     strconv.Itoa(params.FailCount),
		LabelCallbackURL:   params.CallbackURL,
		LabelBackendName:   params.BackendName,
	}
	if params.ServiceName != "" {
		labels[LabelServiceName] = params.ServiceName
	}

	// Add user labels (already validated to not conflict with fred.*)
	for k, v := range params.Manifest.Labels {
		labels[k] = v
	}

	// Inject ingress labels for auto-discovery routing.
	if params.Ingress.Enabled {
		if port, ok := SelectIngressPort(params.Manifest.Ports); ok {
			subdomain := ComputeSubdomain(params.LeaseUUID, params.ServiceName, params.InstanceIndex, params.Quantity)
			fqdn := ComputeFQDN(subdomain, params.Ingress.WildcardDomain)
			routerName := RouterName(params.LeaseUUID, params.ServiceName, params.InstanceIndex, params.Quantity)
			for k, v := range TraefikLabels(params.Ingress, params.NetworkName, routerName, fqdn, port) {
				labels[k] = v
			}
			labels[LabelFQDN] = fqdn
		}
	}

	// Build environment variables
	var env []string
	for k, v := range params.Manifest.Env {
		env = append(env, k+"="+v)
	}

	// Build port bindings
	exposedPorts := nat.PortSet{}
	portBindings := nat.PortMap{}

	for portSpec, portCfg := range params.Manifest.Ports {
		port := nat.Port(portSpec)
		exposedPorts[port] = struct{}{}

		hostIP := params.HostBindIP
		if hostIP == "" {
			hostIP = "0.0.0.0"
		}
		binding := nat.PortBinding{
			HostIP: hostIP,
		}
		if portCfg.HostPort > 0 {
			binding.HostPort = strconv.Itoa(portCfg.HostPort)
		}
		portBindings[port] = []nat.PortBinding{binding}
	}

	// Build container config
	config := &container.Config{
		Image:        params.Manifest.Image,
		Env:          env,
		Labels:       labels,
		ExposedPorts: exposedPorts,
	}

	// Set container user if provided (bypasses entrypoint user switching)
	if params.User != "" {
		config.User = params.User
	}

	// Set command and args if provided
	if len(params.Manifest.Command) > 0 {
		config.Entrypoint = params.Manifest.Command
	}
	if len(params.Manifest.Args) > 0 {
		config.Cmd = params.Manifest.Args
	}

	// Set health check if provided
	if params.Manifest.HealthCheck != nil {
		config.Healthcheck = &container.HealthConfig{
			Test:        params.Manifest.HealthCheck.Test,
			Interval:    params.Manifest.HealthCheck.Interval.Duration(),
			Timeout:     params.Manifest.HealthCheck.Timeout.Duration(),
			Retries:     params.Manifest.HealthCheck.Retries,
			StartPeriod: params.Manifest.HealthCheck.StartPeriod.Duration(),
		}
	}

	// Build host config with resource limits and hardening
	hostConfig := &container.HostConfig{
		PortBindings: portBindings,
		Resources: container.Resources{
			NanoCPUs:   int64(params.Profile.CPUCores * 1e9),
			Memory:     params.Profile.MemoryMB * 1024 * 1024,
			MemorySwap: params.Profile.MemoryMB * 1024 * 1024, // equal to Memory = no swap
			PidsLimit:  params.PidsLimit,
		},
		RestartPolicy: container.RestartPolicy{
			Name: container.RestartPolicyDisabled,
		},
		CapDrop:        []string{"ALL"},
		SecurityOpt:    []string{"no-new-privileges:true"},
		ReadonlyRootfs: params.ReadonlyRootfs,
	}
	tmpfsSize := fmt.Sprintf("size=%dM", params.TmpfsSizeMB)
	if params.ReadonlyRootfs {
		hostConfig.Tmpfs = map[string]string{
			"/tmp": tmpfsSize,
			"/run": tmpfsSize,
		}
		// Add tenant-requested tmpfs mounts (validated in manifest.Validate)
		for _, p := range params.Manifest.Tmpfs {
			hostConfig.Tmpfs[p] = tmpfsSize
		}
	}

	// Ephemeral: override image VOLUME paths with tmpfs to prevent Docker
	// from creating anonymous volumes that bypass quotas. Applied regardless
	// of ReadonlyRootfs to ensure VOLUME paths are always controlled.
	if len(params.VolumeBinds) == 0 && len(params.ImageVolumes) > 0 {
		if hostConfig.Tmpfs == nil {
			hostConfig.Tmpfs = make(map[string]string)
		}
		for _, volPath := range params.ImageVolumes {
			hostConfig.Tmpfs[volPath] = tmpfsSize
		}
	}

	// Auto-detected writable paths for non-root images (e.g.,
	// grafana/grafana's /var/lib/grafana, mysql's /var/run/mysqld).
	// Bind mount from managed volume subdirectories that contain
	// pre-extracted image content.
	if len(params.WritablePathBinds) > 0 {
		for hostPath, containerPath := range params.WritablePathBinds {
			hostConfig.Mounts = append(hostConfig.Mounts, mount.Mount{
				Type:   mount.TypeBind,
				Source: hostPath,
				Target: containerPath,
			})
		}
	}

	// Stateful: bind mount managed volume dirs to image VOLUME paths
	if len(params.VolumeBinds) > 0 {
		for hostPath, containerPath := range params.VolumeBinds {
			hostConfig.Mounts = append(hostConfig.Mounts, mount.Mount{
				Type:   mount.TypeBind,
				Source: hostPath,
				Target: containerPath,
			})
		}
	}

	// Generate container name: stack uses service name, legacy uses instance index only.
	var containerName string
	if params.ServiceName != "" {
		containerName = fmt.Sprintf("fred-%s-%s-%d", params.LeaseUUID, params.ServiceName, params.InstanceIndex)
	} else {
		containerName = fmt.Sprintf("fred-%s-%d", params.LeaseUUID, params.InstanceIndex)
	}

	networkConfig := params.NetworkConfig
	if networkConfig == nil {
		networkConfig = &networktypes.NetworkingConfig{}
	}

	// For stack services, add the service name as a Docker network alias
	// so other services in the stack can resolve it by name (e.g., "redis").
	if params.ServiceName != "" {
		for netName, epCfg := range networkConfig.EndpointsConfig {
			if epCfg == nil {
				epCfg = &networktypes.EndpointSettings{}
				networkConfig.EndpointsConfig[netName] = epCfg
			}
			epCfg.Aliases = append(epCfg.Aliases, params.ServiceName)
		}
	}

	// Attempt creation with retry for ephemeral port conflicts.
	ephemeral := hasEphemeralPorts(params.Manifest.Ports)
	var lastErr error
	for attempt := range portBindRetries {
		resp, err := d.client.ContainerCreate(ctx, config, hostConfig, networkConfig, nil, containerName)
		if err == nil {
			return resp.ID, nil
		}

		if !ephemeral || !isPortBindingError(err) {
			return "", fmt.Errorf("failed to create container: %w", err)
		}

		lastErr = err
		if attempt < portBindRetries-1 {
			select {
			case <-ctx.Done():
				return "", fmt.Errorf("failed to create container: %w (port conflict retries exhausted by context: %w)", lastErr, ctx.Err())
			case <-time.After(portBindRetryDelay):
			}
		}
	}

	return "", fmt.Errorf("failed to create container after %d retries: %w", portBindRetries, lastErr)
}

// StartContainer starts a container.
func (d *DockerClient) StartContainer(ctx context.Context, containerID string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err := d.client.ContainerStart(ctx, containerID, container.StartOptions{})
	if err != nil {
		return fmt.Errorf("failed to start container: %w", err)
	}

	return nil
}

// StopContainer gracefully stops a running container with a timeout.
// After the timeout, the container is forcefully killed.
func (d *DockerClient) StopContainer(ctx context.Context, containerID string, timeout time.Duration) error {
	timeoutSeconds := int(timeout.Seconds())
	stopOpts := container.StopOptions{
		Timeout: &timeoutSeconds,
	}
	if err := d.client.ContainerStop(ctx, containerID, stopOpts); err != nil {
		return fmt.Errorf("failed to stop container: %w", err)
	}
	return nil
}

// RenameContainer changes the name of a container. The container can be
// running or stopped. This is used during updates/restarts to free the
// canonical name for the replacement container while keeping the old one
// available for rollback.
func (d *DockerClient) RenameContainer(ctx context.Context, containerID string, newName string) error {
	if err := d.client.ContainerRename(ctx, containerID, newName); err != nil {
		return fmt.Errorf("failed to rename container: %w", err)
	}
	return nil
}

// ContainerLogs returns the last `tail` lines of combined stdout/stderr for
// a container. If tail is <= 0 it defaults to 100.
func (d *DockerClient) ContainerLogs(ctx context.Context, containerID string, tail int) (string, error) {
	if tail <= 0 {
		tail = 100
	}

	reader, err := d.client.ContainerLogs(ctx, containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Tail:       strconv.Itoa(tail),
	})
	if err != nil {
		return "", fmt.Errorf("failed to get container logs: %w", err)
	}
	defer func() { _ = reader.Close() }()

	// Docker multiplexes stdout/stderr with 8-byte headers per frame
	// when the container is not using a TTY. Use stdcopy.StdCopy to
	// demux, combining both streams into one buffer.
	var buf strings.Builder
	_, err = stdcopy.StdCopy(&buf, &buf, reader)
	if err != nil {
		return "", fmt.Errorf("failed to read container logs: %w", err)
	}

	return buf.String(), nil
}

// RemoveContainer removes a container. It is idempotent — removing an
// already-removed container returns nil.
func (d *DockerClient) RemoveContainer(ctx context.Context, containerID string) error {
	err := d.client.ContainerRemove(ctx, containerID, container.RemoveOptions{
		Force: true,
	})
	if err != nil {
		if client.IsErrNotFound(err) {
			return nil // Already removed
		}
		return fmt.Errorf("failed to remove container: %w", err)
	}

	return nil
}

// labelMeta holds parsed metadata from container labels.
type labelMeta struct {
	InstanceIndex int
	FailCount     int
	CreatedAt     time.Time
}

// parseLabelMeta extracts metadata from container labels. Returns an error if
// a label is present but malformed, which prevents silent defaulting to zero
// values that could cause resource allocation ID collisions.
func parseLabelMeta(labels map[string]string) (labelMeta, error) {
	var m labelMeta
	if idxStr, ok := labels[LabelInstanceIndex]; ok {
		idx, parseErr := strconv.Atoi(idxStr)
		if parseErr != nil {
			return m, fmt.Errorf("invalid %s label %q: %w", LabelInstanceIndex, idxStr, parseErr)
		}
		m.InstanceIndex = idx
	}
	if fcStr, ok := labels[LabelFailCount]; ok {
		fc, parseErr := strconv.Atoi(fcStr)
		if parseErr != nil {
			return m, fmt.Errorf("invalid %s label %q: %w", LabelFailCount, fcStr, parseErr)
		}
		m.FailCount = fc
	}
	if createdStr, ok := labels[LabelCreatedAt]; ok {
		t, parseErr := time.Parse(time.RFC3339, createdStr)
		if parseErr != nil {
			return m, fmt.Errorf("invalid %s label %q: %w", LabelCreatedAt, createdStr, parseErr)
		}
		m.CreatedAt = t
	}
	return m, nil
}

// InspectContainer returns detailed information about a container.
func (d *DockerClient) InspectContainer(ctx context.Context, containerID string) (*ContainerInfo, error) {
	resp, err := d.client.ContainerInspect(ctx, containerID)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect container: %w", err)
	}

	meta, err := parseLabelMeta(resp.Config.Labels)
	if err != nil {
		return nil, fmt.Errorf("container %s: %w", resp.ID, err)
	}

	var health HealthStatus
	if resp.State.Health != nil {
		health = HealthStatus(resp.State.Health.Status)
	}

	info := &ContainerInfo{
		ContainerID:   resp.ID,
		LeaseUUID:     resp.Config.Labels[LabelLeaseUUID],
		Tenant:        resp.Config.Labels[LabelTenant],
		ProviderUUID:  resp.Config.Labels[LabelProviderUUID],
		SKU:           resp.Config.Labels[LabelSKU],
		ServiceName:   resp.Config.Labels[LabelServiceName],
		CallbackURL:   resp.Config.Labels[LabelCallbackURL],
		Image:         resp.Config.Image,
		Status:        resp.State.Status,
		Health:        health,
		ExitCode:      resp.State.ExitCode,
		OOMKilled:     resp.State.OOMKilled,
		InstanceIndex: meta.InstanceIndex,
		FailCount:     meta.FailCount,
		CreatedAt:     meta.CreatedAt,
		Ports:         make(map[string]PortBinding),
		FQDN:          resp.Config.Labels[LabelFQDN],
	}

	// Extract port bindings
	for port, bindings := range resp.NetworkSettings.Ports {
		if len(bindings) > 0 {
			info.Ports[string(port)] = PortBinding{
				HostIP:   bindings[0].HostIP,
				HostPort: bindings[0].HostPort,
			}
		}
	}

	return info, nil
}

// ListManagedContainers returns all containers managed by Fred.
// When backendName is set, only containers belonging to this backend are returned.
func (d *DockerClient) ListManagedContainers(ctx context.Context) ([]ContainerInfo, error) {
	f := filters.NewArgs(
		filters.Arg("label", LabelManaged+"=true"),
	)
	if d.backendName != "" {
		f.Add("label", LabelBackendName+"="+d.backendName)
	}
	containers, err := d.client.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: f,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}

	var result []ContainerInfo
	for _, c := range containers {
		meta, err := parseLabelMeta(c.Labels)
		if err != nil {
			// Skip containers with malformed Fred labels — these can't be
			// reliably tracked for resource accounting.
			slog.Warn("skipping managed container with malformed labels",
				"container_id", c.ID[:12],
				"lease_uuid", c.Labels[LabelLeaseUUID],
				"error", err,
			)
			continue
		}

		info := ContainerInfo{
			ContainerID:   c.ID,
			LeaseUUID:     c.Labels[LabelLeaseUUID],
			Tenant:        c.Labels[LabelTenant],
			ProviderUUID:  c.Labels[LabelProviderUUID],
			SKU:           c.Labels[LabelSKU],
			ServiceName:   c.Labels[LabelServiceName],
			CallbackURL:   c.Labels[LabelCallbackURL],
			Image:         c.Image,
			Status:        c.State,
			InstanceIndex: meta.InstanceIndex,
			FailCount:     meta.FailCount,
			CreatedAt:     meta.CreatedAt,
			Ports:         make(map[string]PortBinding),
			FQDN:          c.Labels[LabelFQDN],
		}

		// Extract port bindings from container ports
		for _, p := range c.Ports {
			portSpec := fmt.Sprintf("%d/%s", p.PrivatePort, p.Type)
			info.Ports[portSpec] = PortBinding{
				HostIP:   p.IP,
				HostPort: strconv.Itoa(int(p.PublicPort)),
			}
		}

		result = append(result, info)
	}

	return result, nil
}

// TenantNetworkName returns a deterministic network name for a tenant address.
func TenantNetworkName(tenant string) string {
	h := sha256.Sum256([]byte(tenant))
	return "fred-tenant-" + hex.EncodeToString(h[:8])
}

// EnsureTenantNetwork creates or returns the existing network for a tenant.
// The network is a per-tenant bridge that provides inter-tenant isolation.
// Note: Internal must be false because Docker internal networks do not
// support port publishing (moby/moby#36174). Outbound internet access
// from containers is a side effect.
func (d *DockerClient) EnsureTenantNetwork(ctx context.Context, tenant string) (string, error) {
	name := TenantNetworkName(tenant)

	// Check if network already exists. The backend_name filter is safe here:
	// every network Fred creates has this label set (see netLabels below),
	// so there are no pre-existing networks without it to miss.
	nf := filters.NewArgs(
		filters.Arg("name", name),
		filters.Arg("label", LabelManaged+"=true"),
	)
	if d.backendName != "" {
		nf.Add("label", LabelBackendName+"="+d.backendName)
	}
	networks, err := d.client.NetworkList(ctx, networktypes.ListOptions{
		Filters: nf,
	})
	if err != nil {
		return "", fmt.Errorf("failed to list networks: %w", err)
	}
	if len(networks) > 0 {
		return networks[0].ID, nil
	}

	// Create the network. If a concurrent caller already created it, the
	// Docker API returns a conflict error. Handle this by re-listing.
	netLabels := map[string]string{
		LabelManaged: "true",
		LabelTenant:  tenant,
	}
	if d.backendName != "" {
		netLabels[LabelBackendName] = d.backendName
	}
	resp, err := d.client.NetworkCreate(ctx, name, networktypes.CreateOptions{
		Driver:   "bridge",
		Internal: false, // Must be false: Internal:true prevents port publishing (moby#36174)
		Labels:   netLabels,
	})
	if err != nil {
		if errdefs.IsConflict(err) {
			// Another goroutine created the network between our list and create.
			networks, listErr := d.client.NetworkList(ctx, networktypes.ListOptions{
				Filters: nf,
			})
			if listErr != nil {
				return "", fmt.Errorf("failed to list networks after conflict: %w", listErr)
			}
			if len(networks) > 0 {
				return networks[0].ID, nil
			}
			return "", fmt.Errorf("network conflict but not found on retry: %w", err)
		}
		return "", fmt.Errorf("failed to create tenant network: %w", err)
	}
	return resp.ID, nil
}

// RemoveTenantNetworkIfEmpty removes the tenant's network if no containers are connected.
func (d *DockerClient) RemoveTenantNetworkIfEmpty(ctx context.Context, tenant string) error {
	name := TenantNetworkName(tenant)

	// Inspect to check connected containers
	resp, err := d.client.NetworkInspect(ctx, name, networktypes.InspectOptions{})
	if err != nil {
		if client.IsErrNotFound(err) {
			return nil // Already removed
		}
		return fmt.Errorf("failed to inspect network: %w", err)
	}

	if len(resp.Containers) > 0 {
		return nil // Still in use
	}

	if err := d.client.NetworkRemove(ctx, resp.ID); err != nil {
		if client.IsErrNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to remove network: %w", err)
	}
	return nil
}

// ListManagedNetworks returns all networks created by Fred, with full details.
// When backendName is set, only networks belonging to this backend are returned.
func (d *DockerClient) ListManagedNetworks(ctx context.Context) ([]networktypes.Inspect, error) {
	f := filters.NewArgs(
		filters.Arg("label", LabelManaged+"=true"),
	)
	if d.backendName != "" {
		f.Add("label", LabelBackendName+"="+d.backendName)
	}
	summaries, err := d.client.NetworkList(ctx, networktypes.ListOptions{
		Filters: f,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list managed networks: %w", err)
	}

	var result []networktypes.Inspect
	for _, s := range summaries {
		inspected, err := d.client.NetworkInspect(ctx, s.ID, networktypes.InspectOptions{})
		if err != nil {
			if !client.IsErrNotFound(err) {
				slog.Warn("failed to inspect network during list; network excluded from results", "network_id", s.ID, "error", err)
			}
			continue
		}
		result = append(result, inspected)
	}
	return result, nil
}

// buildNetworkConfig returns a NetworkingConfig that attaches to the given network,
// or an empty config if networkID is empty.
func buildNetworkConfig(networkID string) *networktypes.NetworkingConfig {
	if networkID == "" {
		return &networktypes.NetworkingConfig{}
	}
	return &networktypes.NetworkingConfig{
		EndpointsConfig: map[string]*networktypes.EndpointSettings{
			networkID: {},
		},
	}
}

// HealthStatus represents the health check status of a Docker container.
type HealthStatus string

const (
	HealthStatusHealthy   HealthStatus = "healthy"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
	HealthStatusStarting  HealthStatus = "starting"
	HealthStatusNone      HealthStatus = "" // No health check configured
)

// ContainerEvents subscribes to Docker container lifecycle events, filtering
// for "die" events on containers managed by fred (label managed-by=fred).
// Returns a channel of ContainerEvent and a channel of errors. Both channels
// are closed when the context is canceled or the Docker event stream closes.
func (d *DockerClient) ContainerEvents(ctx context.Context) (<-chan ContainerEvent, <-chan error) {
	filter := filters.NewArgs(
		filters.Arg("type", string(events.ContainerEventType)),
		filters.Arg("event", "die"),
		filters.Arg("label", LabelManaged+"=true"),
	)
	if d.backendName != "" {
		filter.Add("label", LabelBackendName+"="+d.backendName)
	}

	dockerEvents, dockerErrs := d.client.Events(ctx, events.ListOptions{Filters: filter})

	out := make(chan ContainerEvent)
	errCh := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errCh)

		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-dockerEvents:
				if !ok {
					return
				}
				select {
				case out <- ContainerEvent{
					ContainerID: event.Actor.ID,
					Action:      string(event.Action),
				}:
				case <-ctx.Done():
					return
				}
			case err, ok := <-dockerErrs:
				if !ok {
					return
				}
				select {
				case errCh <- err:
				case <-ctx.Done():
					return
				}
				return
			}
		}
	}()

	return out, errCh
}

// containerStatusToProvisionStatus converts Docker status to provision status.
func containerStatusToProvisionStatus(status string) backend.ProvisionStatus {
	status = strings.ToLower(status)
	switch status {
	case "created", "restarting":
		return backend.ProvisionStatusProvisioning
	case "running", "paused":
		return backend.ProvisionStatusReady
	case "removing", "exited", "dead":
		return backend.ProvisionStatusFailed
	default:
		return backend.ProvisionStatusUnknown
	}
}
