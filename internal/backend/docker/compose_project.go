package docker

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"

	"github.com/manifest-network/fred/internal/backend"
)

// composeProjectParams holds all inputs for building a Compose project.
type composeProjectParams struct {
	LeaseUUID    string
	Tenant       string
	ProviderUUID string
	CallbackURL  string
	BackendName  string
	FailCount    int
	Stack        *StackManifest
	Items        []backend.LeaseItem
	Profiles     map[string]SKUProfile
	ImageSetups  map[string]*imageSetup
	NetworkName  string                            // pre-created tenant network name (empty if isolation disabled)
	VolBinds     map[string]map[int]serviceVolBinds // svc → instance → binds
	Cfg          *Config
}

// serviceVolBinds holds volume binds for a single service instance.
type serviceVolBinds struct {
	StatefulBinds map[string]string // hostPath → containerPath
	WritableBinds map[string]string // hostPath → containerPath
}

// buildComposeProject generates a compose-go Project from the parameters.
// The project is fully in-memory — no YAML files or project directories.
func buildComposeProject(params composeProjectParams) (*composetypes.Project, error) {
	projectName := composeProjectName(params.LeaseUUID)
	services := make(composetypes.Services)

	for _, item := range params.Items {
		svcName := item.ServiceName
		svc := params.Stack.Services[svcName]
		profile := params.Profiles[item.SKU]
		imgSetup := params.ImageSetups[svcName]

		for i := 0; i < item.Quantity; i++ {
			composeSvcName := svcName
			if item.Quantity > 1 {
				composeSvcName = fmt.Sprintf("%s-%d", svcName, i)
			}

			svcConfig, err := buildComposeServiceConfig(composeServiceParams{
				LeaseUUID:    params.LeaseUUID,
				Tenant:       params.Tenant,
				ProviderUUID: params.ProviderUUID,
				CallbackURL:  params.CallbackURL,
				BackendName:  params.BackendName,
				FailCount:    params.FailCount,
				ServiceName:  svcName,
				Instance:     i,
				SKU:          item.SKU,
				Manifest:     svc,
				Profile:      profile,
				ImgSetup:     imgSetup,
				NetworkName:  params.NetworkName,
				Cfg:          params.Cfg,
			})
			if err != nil {
				return nil, fmt.Errorf("service %s instance %d: %w", svcName, i, err)
			}

			// Apply volume binds if present.
			if params.VolBinds != nil {
				if instanceBinds, ok := params.VolBinds[svcName]; ok {
					if binds, ok := instanceBinds[i]; ok {
						applyVolumeBinds(&svcConfig, binds)
					}
				}
			}

			// Apply ephemeral volume overrides (tmpfs) for image VOLUMEs
			// when no stateful volume binds are present.
			if imgSetup != nil && len(imgSetup.Volumes) > 0 {
				hasStatefulBind := false
				if params.VolBinds != nil {
					if instanceBinds, ok := params.VolBinds[svcName]; ok {
						if binds, ok := instanceBinds[i]; ok {
							hasStatefulBind = len(binds.StatefulBinds) > 0
						}
					}
				}
				if !hasStatefulBind {
					tmpfsSize := int64(params.Cfg.GetTmpfsSizeMB()) * 1024 * 1024
					for _, volPath := range imgSetup.Volumes {
						svcConfig.Volumes = append(svcConfig.Volumes, composetypes.ServiceVolumeConfig{
							Type:   "tmpfs",
							Target: volPath,
							Tmpfs:  &composetypes.ServiceVolumeTmpfs{Size: composetypes.UnitBytes(tmpfsSize)},
						})
					}
				}
			}

			// Set container name.
			svcConfig.ContainerName = fmt.Sprintf("fred-%s-%s-%d", params.LeaseUUID, svcName, i)

			// Set network alias so all instances of the same service share the base name.
			if params.NetworkName != "" {
				svcConfig.Networks = map[string]*composetypes.ServiceNetworkConfig{
					"default": {
						Aliases: []string{svcName},
					},
				}
			}

			services[composeSvcName] = svcConfig
		}
	}

	project := &composetypes.Project{
		Name:     projectName,
		Services: services,
	}

	// External network configuration.
	if params.NetworkName != "" {
		project.Networks = composetypes.Networks{
			"default": composetypes.NetworkConfig{
				Name:     params.NetworkName,
				External: true,
			},
		}
	}

	return project, nil
}

// composeServiceParams holds inputs for building a single Compose service config.
type composeServiceParams struct {
	LeaseUUID    string
	Tenant       string
	ProviderUUID string
	CallbackURL  string
	BackendName  string
	FailCount    int
	ServiceName  string
	Instance     int
	SKU          string
	Manifest     *DockerManifest
	Profile      SKUProfile
	ImgSetup     *imageSetup
	NetworkName  string
	Cfg          *Config
}

func buildComposeServiceConfig(p composeServiceParams) (composetypes.ServiceConfig, error) {
	svc := composetypes.ServiceConfig{
		Image:      p.Manifest.Image,
		PullPolicy: composetypes.PullPolicyNever,
		CapDrop:    []string{"ALL"},
		SecurityOpt: []string{"no-new-privileges:true"},
		ReadOnly:   p.Cfg.IsReadonlyRootfs(),
		PidsLimit:  *p.Cfg.GetPidsLimit(),
		Restart:    composetypes.RestartPolicyNo,
	}

	// Deploy: resource limits and restart policy.
	memBytes := composetypes.UnitBytes(p.Profile.MemoryMB * 1024 * 1024)
	svc.Deploy = &composetypes.DeployConfig{
		Resources: composetypes.Resources{
			Limits: &composetypes.Resource{
				NanoCPUs:    composetypes.NanoCPUs(p.Profile.CPUCores),
				MemoryBytes: memBytes,
			},
		},
		RestartPolicy: &composetypes.RestartPolicy{
			Condition: "no",
		},
	}
	// MemSwapLimit equal to MemLimit = no swap.
	svc.MemSwapLimit = memBytes

	// Entrypoint and command.
	if len(p.Manifest.Command) > 0 {
		svc.Entrypoint = composetypes.ShellCommand(p.Manifest.Command)
	}
	if len(p.Manifest.Args) > 0 {
		svc.Command = composetypes.ShellCommand(p.Manifest.Args)
	}

	// Environment variables.
	if len(p.Manifest.Env) > 0 {
		env := make(composetypes.MappingWithEquals, len(p.Manifest.Env))
		for k, v := range p.Manifest.Env {
			val := v
			env[k] = &val
		}
		svc.Environment = env
	}

	// Ports.
	if len(p.Manifest.Ports) > 0 {
		ports := make([]composetypes.ServicePortConfig, 0, len(p.Manifest.Ports))
		// Sort port specs for deterministic output.
		portSpecs := make([]string, 0, len(p.Manifest.Ports))
		for spec := range p.Manifest.Ports {
			portSpecs = append(portSpecs, spec)
		}
		slices.Sort(portSpecs)

		for _, spec := range portSpecs {
			cfg := p.Manifest.Ports[spec]
			parts := strings.SplitN(spec, "/", 2)
			port, _ := strconv.Atoi(parts[0])
			proto := parts[1]

			portConfig := composetypes.ServicePortConfig{
				Target:   uint32(port),
				Protocol: proto,
				HostIP:   p.Cfg.GetHostBindIP(),
			}
			if cfg.HostPort > 0 {
				portConfig.Published = strconv.Itoa(cfg.HostPort)
			}
			ports = append(ports, portConfig)
		}
		svc.Ports = ports
	}

	// Health check.
	if p.Manifest.HealthCheck != nil {
		hc := p.Manifest.HealthCheck
		svc.HealthCheck = &composetypes.HealthCheckConfig{
			Test: composetypes.HealthCheckTest(hc.Test),
		}
		if hc.Interval > 0 {
			d := composetypes.Duration(hc.Interval.Duration())
			svc.HealthCheck.Interval = &d
		}
		if hc.Timeout > 0 {
			d := composetypes.Duration(hc.Timeout.Duration())
			svc.HealthCheck.Timeout = &d
		}
		if hc.Retries > 0 {
			r := uint64(hc.Retries)
			svc.HealthCheck.Retries = &r
		}
		if hc.StartPeriod > 0 {
			d := composetypes.Duration(hc.StartPeriod.Duration())
			svc.HealthCheck.StartPeriod = &d
		}
	}

	// User.
	if p.ImgSetup != nil && p.ImgSetup.ContainerUser != "" {
		svc.User = p.ImgSetup.ContainerUser
	}

	// Tmpfs mounts for readonly rootfs.
	if p.Cfg.IsReadonlyRootfs() {
		tmpfsSize := int64(p.Cfg.GetTmpfsSizeMB()) * 1024 * 1024
		// /tmp and /run always mounted.
		svc.Volumes = append(svc.Volumes,
			composetypes.ServiceVolumeConfig{
				Type:   "tmpfs",
				Target: "/tmp",
				Tmpfs:  &composetypes.ServiceVolumeTmpfs{Size: composetypes.UnitBytes(tmpfsSize)},
			},
			composetypes.ServiceVolumeConfig{
				Type:   "tmpfs",
				Target: "/run",
				Tmpfs:  &composetypes.ServiceVolumeTmpfs{Size: composetypes.UnitBytes(tmpfsSize)},
			},
		)
		// Tenant-requested tmpfs mounts.
		for _, path := range p.Manifest.Tmpfs {
			svc.Volumes = append(svc.Volumes, composetypes.ServiceVolumeConfig{
				Type:   "tmpfs",
				Target: path,
				Tmpfs:  &composetypes.ServiceVolumeTmpfs{Size: composetypes.UnitBytes(tmpfsSize)},
			})
		}
	}

	// Labels — all fred.* labels preserved identically to CreateContainer.
	labels := composetypes.Labels{
		LabelManaged:       "true",
		LabelLeaseUUID:     p.LeaseUUID,
		LabelTenant:        p.Tenant,
		LabelProviderUUID:  p.ProviderUUID,
		LabelSKU:           p.SKU,
		LabelCreatedAt:     time.Now().Format(time.RFC3339),
		LabelInstanceIndex: strconv.Itoa(p.Instance),
		LabelFailCount:     strconv.Itoa(p.FailCount),
		LabelCallbackURL:   p.CallbackURL,
		LabelBackendName:   p.BackendName,
		LabelServiceName:   p.ServiceName,
	}
	// Add user labels (already validated to not conflict with fred.*).
	for k, v := range p.Manifest.Labels {
		labels[k] = v
	}
	svc.Labels = labels

	return svc, nil
}

// applyVolumeBinds adds bind mount volumes to a service config.
func applyVolumeBinds(svc *composetypes.ServiceConfig, binds serviceVolBinds) {
	for hostPath, containerPath := range binds.StatefulBinds {
		svc.Volumes = append(svc.Volumes, composetypes.ServiceVolumeConfig{
			Type:   "bind",
			Source: hostPath,
			Target: containerPath,
		})
	}
	for hostPath, containerPath := range binds.WritableBinds {
		svc.Volumes = append(svc.Volumes, composetypes.ServiceVolumeConfig{
			Type:   "bind",
			Source: hostPath,
			Target: containerPath,
		})
	}
}
