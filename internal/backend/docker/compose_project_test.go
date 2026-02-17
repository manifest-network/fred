package docker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func testConfig() *Config {
	cfg := DefaultConfig()
	cfg.HostBindIP = "0.0.0.0"
	return &cfg
}

func baseProjectParams() composeProjectParams {
	return composeProjectParams{
		LeaseUUID:    "lease-1",
		Tenant:       "tenant-a",
		ProviderUUID: "prov-1",
		CallbackURL:  "http://localhost/callback",
		BackendName:  "docker",
		FailCount:    0,
		Stack: &StackManifest{
			Services: map[string]*DockerManifest{
				"web": {Image: "nginx:latest"},
			},
		},
		Items: []backend.LeaseItem{
			{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		},
		Profiles: map[string]SKUProfile{
			"docker-small": {CPUCores: 0.5, MemoryMB: 512},
		},
		ImageSetups: map[string]*imageSetup{
			"web": {},
		},
		NetworkName: "fred-tenant-abc123",
		Cfg:         testConfig(),
	}
}

func TestBuildComposeProject_BasicMapping(t *testing.T) {
	params := baseProjectParams()
	params.Stack.Services["web"] = &DockerManifest{
		Image:   "nginx:latest",
		Command: []string{"/bin/sh", "-c"},
		Args:    []string{"echo hello"},
		Env:     map[string]string{"FOO": "bar"},
		Ports: map[string]PortConfig{
			"80/tcp": {HostPort: 8080},
		},
	}

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	assert.Equal(t, "fred-lease-1", project.Name)
	require.Contains(t, project.Services, "web")

	svc := project.Services["web"]
	assert.Equal(t, "nginx:latest", svc.Image)
	assert.Equal(t, []string{"/bin/sh", "-c"}, []string(svc.Entrypoint))
	assert.Equal(t, []string{"echo hello"}, []string(svc.Command))

	// Env
	require.NotNil(t, svc.Environment)
	assert.Equal(t, "bar", *svc.Environment["FOO"])

	// Ports
	require.Len(t, svc.Ports, 1)
	assert.Equal(t, uint32(80), svc.Ports[0].Target)
	assert.Equal(t, "tcp", svc.Ports[0].Protocol)
	assert.Equal(t, "8080", svc.Ports[0].Published)
	assert.Equal(t, "0.0.0.0", svc.Ports[0].HostIP)
}

func TestBuildComposeProject_ResourceLimits(t *testing.T) {
	params := baseProjectParams()
	params.Profiles["docker-small"] = SKUProfile{CPUCores: 2.0, MemoryMB: 1024}

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	svc := project.Services["web"]
	require.NotNil(t, svc.Deploy)
	require.NotNil(t, svc.Deploy.Resources.Limits)

	// NanoCPUs: compose-go uses float32 representing cores.
	assert.InDelta(t, float32(2.0), float32(svc.Deploy.Resources.Limits.NanoCPUs), 0.001)

	// MemoryBytes.
	assert.Equal(t, int64(1024*1024*1024), int64(svc.Deploy.Resources.Limits.MemoryBytes))

	// MemSwapLimit == MemLimit (no swap).
	assert.Equal(t, int64(1024*1024*1024), int64(svc.MemSwapLimit))

	// PidsLimit.
	assert.Equal(t, int64(256), svc.PidsLimit)
}

func TestBuildComposeProject_Labels(t *testing.T) {
	params := baseProjectParams()
	params.Stack.Services["web"].Labels = map[string]string{
		"app.version": "1.0",
	}

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	svc := project.Services["web"]
	assert.Equal(t, "true", svc.Labels[LabelManaged])
	assert.Equal(t, "lease-1", svc.Labels[LabelLeaseUUID])
	assert.Equal(t, "tenant-a", svc.Labels[LabelTenant])
	assert.Equal(t, "prov-1", svc.Labels[LabelProviderUUID])
	assert.Equal(t, "docker-small", svc.Labels[LabelSKU])
	assert.Equal(t, "web", svc.Labels[LabelServiceName])
	assert.Equal(t, "0", svc.Labels[LabelInstanceIndex])
	assert.Equal(t, "0", svc.Labels[LabelFailCount])
	assert.Equal(t, "http://localhost/callback", svc.Labels[LabelCallbackURL])
	assert.Equal(t, "docker", svc.Labels[LabelBackendName])
	// User labels included.
	assert.Equal(t, "1.0", svc.Labels["app.version"])
}

func TestBuildComposeProject_HealthCheck(t *testing.T) {
	params := baseProjectParams()
	params.Stack.Services["web"].HealthCheck = &HealthCheckConfig{
		Test:        []string{"CMD-SHELL", "curl -f http://localhost/"},
		Interval:    Duration(10 * time.Second),
		Timeout:     Duration(5 * time.Second),
		Retries:     3,
		StartPeriod: Duration(30 * time.Second),
	}

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	svc := project.Services["web"]
	require.NotNil(t, svc.HealthCheck)
	assert.Equal(t, []string{"CMD-SHELL", "curl -f http://localhost/"}, []string(svc.HealthCheck.Test))
	assert.Equal(t, 10*time.Second, time.Duration(*svc.HealthCheck.Interval))
	assert.Equal(t, 5*time.Second, time.Duration(*svc.HealthCheck.Timeout))
	assert.Equal(t, uint64(3), *svc.HealthCheck.Retries)
	assert.Equal(t, 30*time.Second, time.Duration(*svc.HealthCheck.StartPeriod))
}

func TestBuildComposeProject_NoHealthCheck(t *testing.T) {
	params := baseProjectParams()

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	svc := project.Services["web"]
	assert.Nil(t, svc.HealthCheck)
}

func TestBuildComposeProject_CapDropAll(t *testing.T) {
	params := baseProjectParams()

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	svc := project.Services["web"]
	assert.Contains(t, svc.CapDrop, "ALL")
}

func TestBuildComposeProject_SecurityOpt(t *testing.T) {
	params := baseProjectParams()

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	svc := project.Services["web"]
	assert.Contains(t, svc.SecurityOpt, "no-new-privileges:true")
}

func TestBuildComposeProject_ReadOnlyRootfs(t *testing.T) {
	params := baseProjectParams()

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	svc := project.Services["web"]
	assert.True(t, svc.ReadOnly)
}

func TestBuildComposeProject_RestartPolicyDisabled(t *testing.T) {
	params := baseProjectParams()

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	svc := project.Services["web"]
	// Service-level restart.
	assert.Equal(t, "no", svc.Restart)
	// Deploy-level restart policy.
	require.NotNil(t, svc.Deploy)
	require.NotNil(t, svc.Deploy.RestartPolicy)
	assert.Equal(t, "no", svc.Deploy.RestartPolicy.Condition)
}

func TestBuildComposeProject_PullPolicyNever(t *testing.T) {
	params := baseProjectParams()

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	svc := project.Services["web"]
	assert.Equal(t, "never", svc.PullPolicy)
}

func TestBuildComposeProject_StatefulVolumeBinds(t *testing.T) {
	params := baseProjectParams()
	params.VolBinds = map[string]map[int]serviceVolBinds{
		"web": {
			0: {
				StatefulBinds: map[string]string{
					"/mnt/data/lease-1/web-0/data": "/data",
				},
			},
		},
	}

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	svc := project.Services["web"]
	var bindVols []string
	for _, v := range svc.Volumes {
		if v.Type == "bind" {
			bindVols = append(bindVols, v.Source+"→"+v.Target)
		}
	}
	assert.Contains(t, bindVols, "/mnt/data/lease-1/web-0/data→/data")
}

func TestBuildComposeProject_WritablePathBinds(t *testing.T) {
	params := baseProjectParams()
	params.VolBinds = map[string]map[int]serviceVolBinds{
		"web": {
			0: {
				WritableBinds: map[string]string{
					"/mnt/data/lease-1/_wp/var/cache": "/var/cache",
				},
			},
		},
	}

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	svc := project.Services["web"]
	var found bool
	for _, v := range svc.Volumes {
		if v.Type == "bind" && v.Target == "/var/cache" {
			found = true
			assert.Equal(t, "/mnt/data/lease-1/_wp/var/cache", v.Source)
		}
	}
	assert.True(t, found, "writable path bind should be present")
}

func TestBuildComposeProject_EphemeralVolumeOverride(t *testing.T) {
	params := baseProjectParams()
	params.ImageSetups["web"] = &imageSetup{
		Volumes: []string{"/data", "/var/lib/app"},
	}
	// No VolBinds → should get tmpfs overrides.

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	svc := project.Services["web"]
	var tmpfsTargets []string
	for _, v := range svc.Volumes {
		if v.Type == "tmpfs" {
			tmpfsTargets = append(tmpfsTargets, v.Target)
		}
	}
	assert.Contains(t, tmpfsTargets, "/data")
	assert.Contains(t, tmpfsTargets, "/var/lib/app")
}

func TestBuildComposeProject_TmpfsSizeLimits(t *testing.T) {
	params := baseProjectParams()
	params.Cfg.ContainerTmpfsSizeMB = 128

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	svc := project.Services["web"]
	expectedSize := int64(128 * 1024 * 1024)
	for _, v := range svc.Volumes {
		if v.Type == "tmpfs" && (v.Target == "/tmp" || v.Target == "/run") {
			require.NotNil(t, v.Tmpfs)
			assert.Equal(t, expectedSize, int64(v.Tmpfs.Size), "tmpfs for %s should have correct size", v.Target)
		}
	}
}

func TestBuildComposeProject_NoAnonymousVolumes(t *testing.T) {
	params := baseProjectParams()

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	// No Docker-managed volumes should be in the project.
	assert.Empty(t, project.Volumes)

	// All service volumes should be bind or tmpfs (no "volume" type).
	svc := project.Services["web"]
	for _, v := range svc.Volumes {
		assert.NotEqual(t, "volume", v.Type, "no Docker-managed volumes should be created")
	}
}

func TestBuildComposeProject_QuantityFanOut(t *testing.T) {
	params := baseProjectParams()
	params.Stack.Services["web"] = &DockerManifest{Image: "nginx:latest"}
	params.Items = []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 3, ServiceName: "web"},
	}
	params.ImageSetups["web"] = &imageSetup{}

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	assert.Contains(t, project.Services, "web-0")
	assert.Contains(t, project.Services, "web-1")
	assert.Contains(t, project.Services, "web-2")
	assert.NotContains(t, project.Services, "web")
}

func TestBuildComposeProject_FanOutDNSAlias(t *testing.T) {
	params := baseProjectParams()
	params.Items = []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 2, ServiceName: "web"},
	}
	params.ImageSetups["web"] = &imageSetup{}

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	for _, svcName := range []string{"web-0", "web-1"} {
		svc := project.Services[svcName]
		require.NotNil(t, svc.Networks["default"], "service %s should have default network config", svcName)
		assert.Contains(t, svc.Networks["default"].Aliases, "web",
			"service %s should have 'web' as DNS alias", svcName)
	}
}

func TestBuildComposeProject_ContainerNaming(t *testing.T) {
	params := baseProjectParams()
	params.Items = []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 2, ServiceName: "web"},
	}
	params.ImageSetups["web"] = &imageSetup{}

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	assert.Equal(t, "fred-lease-1-web-0", project.Services["web-0"].ContainerName)
	assert.Equal(t, "fred-lease-1-web-1", project.Services["web-1"].ContainerName)
}

func TestBuildComposeProject_ExternalNetwork(t *testing.T) {
	params := baseProjectParams()
	params.NetworkName = "fred-tenant-abc123"

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	require.Contains(t, project.Networks, "default")
	net := project.Networks["default"]
	assert.Equal(t, "fred-tenant-abc123", net.Name)
	assert.True(t, bool(net.External))
}

func TestBuildComposeProject_NoExtraNetworks(t *testing.T) {
	params := baseProjectParams()

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	// Only "default" network should be present.
	assert.Len(t, project.Networks, 1)
	assert.Contains(t, project.Networks, "default")
}

func TestBuildComposeProject_NoNetworkWhenIsolationDisabled(t *testing.T) {
	params := baseProjectParams()
	params.NetworkName = ""

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	assert.Empty(t, project.Networks)
}

func TestBuildComposeProject_MultiService(t *testing.T) {
	params := baseProjectParams()
	params.Stack = &StackManifest{
		Services: map[string]*DockerManifest{
			"web": {Image: "nginx:latest"},
			"db":  {Image: "postgres:16"},
		},
	}
	params.Items = []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
	}
	params.ImageSetups = map[string]*imageSetup{
		"web": {},
		"db":  {},
	}

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	assert.Len(t, project.Services, 2)
	assert.Contains(t, project.Services, "web")
	assert.Contains(t, project.Services, "db")
	assert.Equal(t, "nginx:latest", project.Services["web"].Image)
	assert.Equal(t, "postgres:16", project.Services["db"].Image)
}

func TestBuildComposeProject_UserFromImageSetup(t *testing.T) {
	params := baseProjectParams()
	params.ImageSetups["web"] = &imageSetup{
		ContainerUser: "999:999",
	}

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	svc := project.Services["web"]
	assert.Equal(t, "999:999", svc.User)
}

func TestBuildComposeProject_TmpfsMounts(t *testing.T) {
	params := baseProjectParams()
	params.Stack.Services["web"].Tmpfs = []string{"/var/cache/nginx", "/var/log/nginx"}

	project, err := buildComposeProject(params)
	require.NoError(t, err)

	svc := project.Services["web"]
	var tmpfsTargets []string
	for _, v := range svc.Volumes {
		if v.Type == "tmpfs" {
			tmpfsTargets = append(tmpfsTargets, v.Target)
		}
	}
	assert.Contains(t, tmpfsTargets, "/tmp")
	assert.Contains(t, tmpfsTargets, "/run")
	assert.Contains(t, tmpfsTargets, "/var/cache/nginx")
	assert.Contains(t, tmpfsTargets, "/var/log/nginx")
}
