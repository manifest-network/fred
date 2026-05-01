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

	project := buildComposeProject(params)

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

	project := buildComposeProject(params)

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

	project := buildComposeProject(params)

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

	project := buildComposeProject(params)

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

	project := buildComposeProject(params)

	svc := project.Services["web"]
	assert.Nil(t, svc.HealthCheck)
}

func TestBuildComposeProject_CapDropAll(t *testing.T) {
	params := baseProjectParams()

	project := buildComposeProject(params)

	svc := project.Services["web"]
	assert.Contains(t, svc.CapDrop, "ALL")
}

func TestBuildComposeProject_SecurityOpt(t *testing.T) {
	params := baseProjectParams()

	project := buildComposeProject(params)

	svc := project.Services["web"]
	assert.Contains(t, svc.SecurityOpt, "no-new-privileges:true")
}

func TestBuildComposeProject_ReadOnlyRootfs(t *testing.T) {
	params := baseProjectParams()

	project := buildComposeProject(params)

	svc := project.Services["web"]
	assert.True(t, svc.ReadOnly)
}

func TestBuildComposeProject_RestartPolicyDisabled(t *testing.T) {
	params := baseProjectParams()

	project := buildComposeProject(params)

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

	project := buildComposeProject(params)

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

	project := buildComposeProject(params)

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

	project := buildComposeProject(params)

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

	project := buildComposeProject(params)

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

	project := buildComposeProject(params)

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

	project := buildComposeProject(params)

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

	project := buildComposeProject(params)

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

	project := buildComposeProject(params)

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

	project := buildComposeProject(params)

	assert.Equal(t, "fred-lease-1-web-0", project.Services["web-0"].ContainerName)
	assert.Equal(t, "fred-lease-1-web-1", project.Services["web-1"].ContainerName)
}

func TestBuildComposeProject_ExternalNetwork(t *testing.T) {
	params := baseProjectParams()
	params.NetworkName = "fred-tenant-abc123"

	project := buildComposeProject(params)

	require.Contains(t, project.Networks, "default")
	net := project.Networks["default"]
	assert.Equal(t, "fred-tenant-abc123", net.Name)
	assert.True(t, bool(net.External))
}

func TestBuildComposeProject_NoExtraNetworks(t *testing.T) {
	params := baseProjectParams()

	project := buildComposeProject(params)

	// Only "default" network should be present.
	assert.Len(t, project.Networks, 1)
	assert.Contains(t, project.Networks, "default")
}

func TestBuildComposeProject_NoNetworkWhenIsolationDisabled(t *testing.T) {
	params := baseProjectParams()
	params.NetworkName = ""

	project := buildComposeProject(params)

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

	project := buildComposeProject(params)

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

	project := buildComposeProject(params)

	svc := project.Services["web"]
	assert.Equal(t, "999:999", svc.User)
}

func TestBuildComposeProject_TmpfsMounts(t *testing.T) {
	params := baseProjectParams()
	params.Stack.Services["web"].Tmpfs = []string{"/var/cache/nginx", "/var/log/nginx"}

	project := buildComposeProject(params)

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

// --- depends_on compose mapping tests ---

func TestBuildComposeProject_DependsOn_Simple(t *testing.T) {
	params := baseProjectParams()
	params.Stack = &StackManifest{
		Services: map[string]*DockerManifest{
			"web": {
				Image: "nginx",
				DependsOn: map[string]DependsOnCondition{
					"db": {Condition: "service_started"},
				},
			},
			"db": {Image: "postgres"},
		},
	}
	params.Items = []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
	}
	params.ImageSetups = map[string]*imageSetup{"web": {}, "db": {}}

	project := buildComposeProject(params)

	svc := project.Services["web"]
	require.Contains(t, svc.DependsOn, "db")
	assert.Equal(t, "service_started", svc.DependsOn["db"].Condition)
	assert.True(t, svc.DependsOn["db"].Required)

	// db should have no depends_on.
	dbSvc := project.Services["db"]
	assert.Empty(t, dbSvc.DependsOn)
}

func TestBuildComposeProject_DependsOn_FanOutDep(t *testing.T) {
	// web (qty 1) depends on db (qty 2) → web depends on db-0 and db-1.
	params := baseProjectParams()
	params.Stack = &StackManifest{
		Services: map[string]*DockerManifest{
			"web": {
				Image: "nginx",
				DependsOn: map[string]DependsOnCondition{
					"db": {Condition: "service_healthy"},
				},
			},
			"db": {
				Image: "postgres",
				HealthCheck: &HealthCheckConfig{
					Test: []string{"CMD", "pg_isready"},
				},
			},
		},
	}
	params.Items = []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		{SKU: "docker-small", Quantity: 2, ServiceName: "db"},
	}
	params.ImageSetups = map[string]*imageSetup{"web": {}, "db": {}}

	project := buildComposeProject(params)

	svc := project.Services["web"]
	require.Len(t, svc.DependsOn, 2)
	require.Contains(t, svc.DependsOn, "db-0")
	require.Contains(t, svc.DependsOn, "db-1")
	assert.Equal(t, "service_healthy", svc.DependsOn["db-0"].Condition)
	assert.Equal(t, "service_healthy", svc.DependsOn["db-1"].Condition)
}

func TestBuildComposeProject_DependsOn_BothFanOut(t *testing.T) {
	// web (qty 2) depends on db (qty 2) → web-0 and web-1 both depend on db-0 and db-1.
	params := baseProjectParams()
	params.Stack = &StackManifest{
		Services: map[string]*DockerManifest{
			"web": {
				Image: "nginx",
				DependsOn: map[string]DependsOnCondition{
					"db": {Condition: "service_started"},
				},
			},
			"db": {Image: "postgres"},
		},
	}
	params.Items = []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 2, ServiceName: "web"},
		{SKU: "docker-small", Quantity: 2, ServiceName: "db"},
	}
	params.ImageSetups = map[string]*imageSetup{"web": {}, "db": {}}

	project := buildComposeProject(params)

	for _, name := range []string{"web-0", "web-1"} {
		svc := project.Services[name]
		require.Len(t, svc.DependsOn, 2, "service %s should depend on 2 instances", name)
		assert.Contains(t, svc.DependsOn, "db-0")
		assert.Contains(t, svc.DependsOn, "db-1")
	}
}

// --- stop_grace_period compose mapping tests ---

func TestBuildComposeProject_StopGracePeriod_Set(t *testing.T) {
	params := baseProjectParams()
	d := Duration(30 * time.Second)
	params.Stack.Services["web"].StopGracePeriod = &d

	project := buildComposeProject(params)

	svc := project.Services["web"]
	require.NotNil(t, svc.StopGracePeriod)
	assert.Equal(t, 30*time.Second, time.Duration(*svc.StopGracePeriod))
}

func TestBuildComposeProject_StopGracePeriod_NotSet(t *testing.T) {
	params := baseProjectParams()

	project := buildComposeProject(params)

	svc := project.Services["web"]
	assert.Nil(t, svc.StopGracePeriod)
}

// --- init compose mapping tests ---

func TestBuildComposeProject_Init_True(t *testing.T) {
	params := baseProjectParams()
	trueVal := true
	params.Stack.Services["web"].Init = &trueVal

	project := buildComposeProject(params)

	svc := project.Services["web"]
	require.NotNil(t, svc.Init)
	assert.True(t, *svc.Init)
}

func TestBuildComposeProject_Init_False(t *testing.T) {
	params := baseProjectParams()
	falseVal := false
	params.Stack.Services["web"].Init = &falseVal

	project := buildComposeProject(params)

	svc := project.Services["web"]
	require.NotNil(t, svc.Init)
	assert.False(t, *svc.Init)
}

func TestBuildComposeProject_Init_NotSet(t *testing.T) {
	params := baseProjectParams()

	project := buildComposeProject(params)

	svc := project.Services["web"]
	assert.Nil(t, svc.Init)
}

// --- expose compose mapping tests ---

func TestBuildComposeProject_Expose(t *testing.T) {
	params := baseProjectParams()
	params.Stack.Services["web"].Expose = []string{"3000", "8080"}

	project := buildComposeProject(params)

	svc := project.Services["web"]
	require.Len(t, svc.Expose, 2)
	assert.Contains(t, []string(svc.Expose), "3000")
	assert.Contains(t, []string(svc.Expose), "8080")
}

// --- ServiceConfig.Name tests ---

func TestBuildComposeProject_ServiceConfigName(t *testing.T) {
	t.Run("single instance", func(t *testing.T) {
		params := baseProjectParams()
		project := buildComposeProject(params)

		svc := project.Services["web"]
		assert.Equal(t, "web", svc.Name)
	})

	t.Run("fan-out instances", func(t *testing.T) {
		params := baseProjectParams()
		params.Items = []backend.LeaseItem{
			{SKU: "docker-small", Quantity: 2, ServiceName: "web"},
		}
		params.ImageSetups["web"] = &imageSetup{}

		project := buildComposeProject(params)

		assert.Equal(t, "web-0", project.Services["web-0"].Name)
		assert.Equal(t, "web-1", project.Services["web-1"].Name)
	})
}

func TestBuildComposeProject_DependsOn_ComposeGraphResolvable(t *testing.T) {
	// Regression test: Compose's NewGraph keys vertices by ServiceConfig.Name.
	// If Name is empty, depends_on resolution fails with "could not find: not found".
	params := baseProjectParams()
	params.Stack = &StackManifest{
		Services: map[string]*DockerManifest{
			"web": {
				Image: "nginx",
				DependsOn: map[string]DependsOnCondition{
					"db": {Condition: "service_started"},
				},
			},
			"db": {Image: "postgres"},
		},
	}
	params.Items = []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
	}
	params.ImageSetups = map[string]*imageSetup{"web": {}, "db": {}}

	project := buildComposeProject(params)

	// Verify ServiceConfig.Name is set for all services — this is what
	// Compose's dependency graph uses as vertex keys.
	for mapKey, svc := range project.Services {
		assert.Equal(t, mapKey, svc.Name, "ServiceConfig.Name must match the Services map key")
	}
}

func TestBuildComposeProject_CustomLabels(t *testing.T) {
	t.Run("single instance", func(t *testing.T) {
		params := baseProjectParams()
		project := buildComposeProject(params)

		svc := project.Services["web"]
		assert.Equal(t, composeProjectName("lease-1"), svc.CustomLabels["com.docker.compose.project"])
		assert.Equal(t, "web", svc.CustomLabels["com.docker.compose.service"])
		assert.Equal(t, "False", svc.CustomLabels["com.docker.compose.oneoff"])
		assert.Contains(t, svc.CustomLabels, "com.docker.compose.version")
	})

	t.Run("fan-out instances", func(t *testing.T) {
		params := baseProjectParams()
		params.Items = []backend.LeaseItem{
			{SKU: "docker-small", Quantity: 2, ServiceName: "web"},
		}
		project := buildComposeProject(params)

		expectedProject := composeProjectName("lease-1")
		for _, name := range []string{"web-0", "web-1"} {
			svc := project.Services[name]
			assert.Equal(t, expectedProject, svc.CustomLabels["com.docker.compose.project"])
			assert.Equal(t, name, svc.CustomLabels["com.docker.compose.service"])
			assert.Equal(t, "False", svc.CustomLabels["com.docker.compose.oneoff"])
		}
	})
}

func TestBuildComposeProject_IngressEnabled(t *testing.T) {
	ingress := IngressConfig{
		Enabled:        true,
		WildcardDomain: "barney8.manifest0.net",
		Entrypoint:     "websecure",
	}

	t.Run("routable service gets traefik labels with tenant network", func(t *testing.T) {
		params := baseProjectParams()
		params.Stack.Services["web"] = &DockerManifest{
			Image: "nginx:latest",
			Ports: map[string]PortConfig{"80/tcp": {}},
		}
		params.Ingress = ingress

		project := buildComposeProject(params)

		// Project should only have the "default" (tenant) network — no shared ingress network.
		require.Contains(t, project.Networks, "default")
		assert.NotContains(t, project.Networks, "ingress")

		// Service should only be on the default network.
		svc := project.Services["web"]
		require.NotNil(t, svc.Networks["default"])
		assert.Nil(t, svc.Networks["ingress"])

		// Traefik labels should point to the tenant network.
		assert.Equal(t, "true", svc.Labels["traefik.enable"])
		assert.Equal(t, params.NetworkName, svc.Labels["traefik.docker.network"])
		assert.NotEmpty(t, svc.Labels[LabelFQDN])
		assert.Contains(t, svc.Labels[LabelFQDN], "barney8.manifest0.net")

		// Router must declare tls=true and no certresolver (wildcard cert is
		// provisioned at the Traefik level, not via per-router ACME).
		routerName := RouterName(params.LeaseUUID, "web", 0, 1)
		assert.Equal(t, "true", svc.Labels["traefik.http.routers."+routerName+".tls"])
		assert.NotContains(t, svc.Labels, "traefik.http.routers."+routerName+".tls.certresolver")
	})

	t.Run("non-routable service does not get traefik labels", func(t *testing.T) {
		params := baseProjectParams()
		params.Stack = &StackManifest{
			Services: map[string]*DockerManifest{
				"web":   {Image: "nginx:latest", Ports: map[string]PortConfig{"80/tcp": {}}},
				"redis": {Image: "redis:7"}, // no ports
			},
		}
		params.Items = []backend.LeaseItem{
			{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
			{SKU: "docker-small", Quantity: 1, ServiceName: "redis"},
		}
		params.ImageSetups = map[string]*imageSetup{"web": {}, "redis": {}}
		params.Ingress = ingress

		project := buildComposeProject(params)

		// web should have traefik labels.
		webSvc := project.Services["web"]
		assert.Equal(t, "true", webSvc.Labels["traefik.enable"])

		// redis should NOT have traefik labels.
		redisSvc := project.Services["redis"]
		assert.Empty(t, redisSvc.Labels["traefik.enable"])
	})

	t.Run("disabled ingress produces no traefik labels", func(t *testing.T) {
		params := baseProjectParams()
		params.Stack.Services["web"] = &DockerManifest{
			Image: "nginx:latest",
			Ports: map[string]PortConfig{"80/tcp": {}},
		}
		// params.Ingress is zero value (disabled)

		project := buildComposeProject(params)

		assert.NotContains(t, project.Networks, "ingress")
		svc := project.Services["web"]
		assert.Empty(t, svc.Labels["traefik.enable"])
		assert.Empty(t, svc.Labels[LabelFQDN])
	})

	t.Run("custom_domain emits secondary router on single-instance service", func(t *testing.T) {
		params := baseProjectParams()
		params.Stack.Services["web"] = &DockerManifest{
			Image: "nginx:latest",
			Ports: map[string]PortConfig{"80/tcp": {}},
		}
		params.Items[0].CustomDomain = "foo.example.com"
		params.Ingress = ingress

		project := buildComposeProject(params)
		svc := project.Services["web"]

		// Primary still present.
		primaryRouter := RouterName(params.LeaseUUID, "web", 0, 1)
		assert.Equal(t, "true", svc.Labels["traefik.http.routers."+primaryRouter+".tls"])

		// Secondary present, with the per-service router name and a -svc service.
		customRouter := CustomDomainRouterName(params.LeaseUUID, "web")
		assert.Equal(t, "Host(`foo.example.com`)", svc.Labels["traefik.http.routers."+customRouter+".rule"])
		assert.Equal(t, "http01", svc.Labels["traefik.http.routers."+customRouter+".tls.certresolver"])
		assert.Equal(t, customRouter+"-svc", svc.Labels["traefik.http.routers."+customRouter+".service"])
		assert.Equal(t, "80", svc.Labels["traefik.http.services."+customRouter+"-svc.loadbalancer.server.port"])
		assert.Equal(t, "foo.example.com", svc.Labels[LabelCustomDomain])
	})

	t.Run("multi-instance emits byte-identical secondary labels", func(t *testing.T) {
		params := baseProjectParams()
		params.Stack.Services["web"] = &DockerManifest{
			Image: "nginx:latest",
			Ports: map[string]PortConfig{"80/tcp": {}},
		}
		params.Items[0].Quantity = 3
		params.Items[0].CustomDomain = "foo.example.com"
		params.Ingress = ingress

		project := buildComposeProject(params)

		// Compose service names are svcName-{i} when quantity > 1.
		require.Contains(t, project.Services, "web-0")
		require.Contains(t, project.Services, "web-1")
		require.Contains(t, project.Services, "web-2")

		customRouter := CustomDomainRouterName(params.LeaseUUID, "web")
		secondaryKeys := []string{
			"traefik.http.routers." + customRouter + ".rule",
			"traefik.http.routers." + customRouter + ".entrypoints",
			"traefik.http.routers." + customRouter + ".tls",
			"traefik.http.routers." + customRouter + ".tls.certresolver",
			"traefik.http.routers." + customRouter + ".middlewares",
			"traefik.http.routers." + customRouter + ".service",
			"traefik.http.services." + customRouter + "-svc.loadbalancer.server.port",
		}

		// Load-balancing contract: every instance container must carry the
		// same secondary labels with the same values so Traefik aggregates
		// them into one router + one service with N backends.
		ref := project.Services["web-0"].Labels
		for _, name := range []string{"web-0", "web-1", "web-2"} {
			labels := project.Services[name].Labels
			for _, k := range secondaryKeys {
				assert.Equal(t, ref[k], labels[k], "secondary label %q must be byte-identical on every instance (got difference on %s)", k, name)
				assert.NotEmpty(t, labels[k], "secondary label %q must be set on %s", k, name)
			}
			assert.Equal(t, "foo.example.com", labels[LabelCustomDomain])
		}

		// Primary routers are per-instance; each instance has its own.
		for i, name := range []string{"web-0", "web-1", "web-2"} {
			labels := project.Services[name].Labels
			primaryRouter := RouterName(params.LeaseUUID, "web", i, 3)
			assert.Equal(t, "true", labels["traefik.http.routers."+primaryRouter+".tls"])
		}
	})

	t.Run("multi-service stack emits secondary only on the item that owns it", func(t *testing.T) {
		params := baseProjectParams()
		params.Stack = &StackManifest{
			Services: map[string]*DockerManifest{
				"frontend": {Image: "nginx:latest", Ports: map[string]PortConfig{"80/tcp": {}}},
				"db":       {Image: "postgres:15", Ports: map[string]PortConfig{"80/tcp": {}}}, // give a routable HTTP port to isolate "no custom_domain on this item"
			},
		}
		params.Items = []backend.LeaseItem{
			{SKU: "docker-small", Quantity: 1, ServiceName: "frontend", CustomDomain: "myblog.com"},
			{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
		}
		params.ImageSetups = map[string]*imageSetup{"frontend": {}, "db": {}}
		params.Ingress = ingress

		project := buildComposeProject(params)

		feLabels := project.Services["frontend"].Labels
		dbLabels := project.Services["db"].Labels

		assert.Equal(t, "myblog.com", feLabels[LabelCustomDomain])
		assert.Empty(t, dbLabels[LabelCustomDomain])

		feRouter := CustomDomainRouterName(params.LeaseUUID, "frontend")
		dbRouter := CustomDomainRouterName(params.LeaseUUID, "db")
		assert.Equal(t, "Host(`myblog.com`)", feLabels["traefik.http.routers."+feRouter+".rule"])
		assert.NotContains(t, dbLabels, "traefik.http.routers."+dbRouter+".rule")
	})

	t.Run("invalid custom_domain skips secondary, primary keeps working", func(t *testing.T) {
		params := baseProjectParams()
		params.Stack.Services["web"] = &DockerManifest{
			Image: "nginx:latest",
			Ports: map[string]PortConfig{"80/tcp": {}},
		}
		// Subdomain of the wildcard — chain would reject this on
		// MsgSetItemCustomDomain, but Fred validates defense-in-depth.
		params.Items[0].CustomDomain = "evil." + ingress.WildcardDomain
		params.Ingress = ingress

		project := buildComposeProject(params)
		svc := project.Services["web"]

		// Primary still emitted.
		primaryRouter := RouterName(params.LeaseUUID, "web", 0, 1)
		assert.Equal(t, "true", svc.Labels["traefik.http.routers."+primaryRouter+".tls"])

		// Secondary skipped — no custom-domain label, no secondary router rule.
		customRouter := CustomDomainRouterName(params.LeaseUUID, "web")
		assert.NotContains(t, svc.Labels, "traefik.http.routers."+customRouter+".rule")
		assert.Empty(t, svc.Labels[LabelCustomDomain])
	})

	t.Run("custom_domain on service with no routable port is skipped", func(t *testing.T) {
		params := baseProjectParams()
		params.Stack.Services["web"] = &DockerManifest{
			Image: "redis:7", // no ports
		}
		params.Items[0].CustomDomain = "foo.example.com"
		params.Ingress = ingress

		project := buildComposeProject(params)
		svc := project.Services["web"]

		// No primary, no secondary.
		assert.Empty(t, svc.Labels["traefik.enable"])
		customRouter := CustomDomainRouterName(params.LeaseUUID, "web")
		assert.NotContains(t, svc.Labels, "traefik.http.routers."+customRouter+".rule")
		assert.Empty(t, svc.Labels[LabelCustomDomain])
	})

	t.Run("empty custom_domain does not change behavior", func(t *testing.T) {
		// Regression guard: items without CustomDomain should produce the
		// same labels as before this feature shipped.
		paramsA := baseProjectParams()
		paramsA.Stack.Services["web"] = &DockerManifest{
			Image: "nginx:latest",
			Ports: map[string]PortConfig{"80/tcp": {}},
		}
		paramsA.Ingress = ingress

		paramsB := baseProjectParams()
		paramsB.Stack.Services["web"] = &DockerManifest{
			Image: "nginx:latest",
			Ports: map[string]PortConfig{"80/tcp": {}},
		}
		paramsB.Items[0].CustomDomain = ""
		paramsB.Ingress = ingress

		assert.Equal(t,
			buildComposeProject(paramsA).Services["web"].Labels,
			buildComposeProject(paramsB).Services["web"].Labels,
		)
	})
}
