package docker

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	composeapi "github.com/docker/compose/v5/pkg/api"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// composeProjectParams holds all inputs for building a Compose project.
type composeProjectParams struct {
	LeaseUUID            string
	Tenant               string
	ProviderUUID         string
	CallbackURL          string
	LifecycleCallbackURL string
	MaintenanceID        shared.MaintenanceID
	BackendName          string
	FailCount            int
	Stack                *manifest.StackManifest
	Items                []backend.LeaseItem
	Profiles             map[string]SKUProfile
	ImageSetups          map[string]*imageSetup
	// Network is the verified authority over the lease's own network. It is
	// required when network isolation is enabled and the project attaches by
	// its ID; with isolation disabled it must be the zero value.
	Network  leaseNetworkAuthority
	VolBinds map[string]map[int]serviceVolBinds // svc → instance → binds
	Cfg      *Config
	Ingress  IngressConfig
}

// serviceVolBinds holds volume binds for a single service instance.
type serviceVolBinds struct {
	StatefulBinds map[string]string // hostPath → containerPath
	WritableBinds map[string]string // hostPath → containerPath
}

// leaseNetworkKey is the project and service key of the lease network. It is
// not Compose's implicit "default": Compose hashes each service's network map,
// so the key change makes the first Up after an upgrade recreate every service
// of a lease that still runs on its tenant network, including a stopped one
// that a network-attachment check alone would restart in place.
const leaseNetworkKey = "lease"

// projectNetwork resolves the network a lease's project attaches to. With
// isolation enabled the authority must be this lease's and this backend's; the
// project then names the network by its verified ID as an external network,
// so Compose neither creates nor removes it and Docker attaches to that exact
// network. The returned name is the network's human-readable name, which
// Traefik matches against a container's attached networks.
func projectNetwork(params composeProjectParams) (id, name string, err error) {
	if params.Cfg == nil || !params.Cfg.IsNetworkIsolation() {
		if params.Network.established() {
			return "", "", fmt.Errorf("%w: network isolation is disabled", errLeaseNetworkOwnership)
		}
		return "", "", nil
	}
	if err := params.Network.authorizes(params.LeaseUUID, params.BackendName); err != nil {
		return "", "", err
	}
	return params.Network.networkID, leaseNetworkName(params.LeaseUUID), nil
}

// buildPlannedComposeProject generates a compose-go Project from the parameters
// and the exact ingress decision that owns emitted item metadata.
// The project is fully in-memory — no YAML files or project directories.
func buildPlannedComposeProject(params composeProjectParams, ingress effectiveIngressPlan) (*composetypes.Project, error) {
	projectName := composeProjectName(params.LeaseUUID)
	services := make(composetypes.Services)
	networkID, networkName, err := projectNetwork(params)
	if err != nil {
		return nil, err
	}

	for _, item := range ingress.items {
		svcName := item.ServiceName
		svc := params.Stack.Services[svcName]
		profile := params.Profiles[item.SKU]
		imgSetup := params.ImageSetups[svcName]

		for i := range item.Quantity {
			composeSvcName := composeServiceName(item, i)

			svcConfig := buildComposeServiceConfig(composeServiceParams{
				LeaseUUID:            params.LeaseUUID,
				Tenant:               params.Tenant,
				ProviderUUID:         params.ProviderUUID,
				CallbackURL:          params.CallbackURL,
				LifecycleCallbackURL: params.LifecycleCallbackURL,
				MaintenanceID:        params.MaintenanceID,
				BackendName:          params.BackendName,
				FailCount:            params.FailCount,
				ServiceName:          svcName,
				Instance:             i,
				SKU:                  item.SKU,
				Manifest:             svc,
				Profile:              profile,
				ImgSetup:             imgSetup,
				NetworkName:          networkName,
				Cfg:                  params.Cfg,
				Ingress:              ingress.routes[svcName],
				Quantity:             item.Quantity,
			})

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

			// Set service name — required by Compose's dependency graph resolver
			// which keys vertices by ServiceConfig.Name, not the Services map key.
			svcConfig.Name = composeSvcName

			// Set CustomLabels — required by Compose's internal container
			// tracking. The CLI layer normally populates these, but since we
			// build projects in-memory we must set them ourselves. Without
			// these labels, start() cannot find containers after create().
			svcConfig.CustomLabels = composetypes.Labels{
				composeapi.ProjectLabel: composeProjectName(params.LeaseUUID),
				composeapi.ServiceLabel: composeSvcName,
				composeapi.VersionLabel: composeapi.ComposeVersion,
				composeapi.OneoffLabel:  "False",
			}

			// Set container name.
			svcConfig.ContainerName = fmt.Sprintf("fred-%s-%s-%d", params.LeaseUUID, svcName, i)

			// Set network alias so all instances of the same service share the base name.
			// The alias lives on the lease's own network, so it resolves only to
			// this lease's containers.
			if networkName != "" {
				svcConfig.Networks = map[string]*composetypes.ServiceNetworkConfig{
					leaseNetworkKey: {
						Aliases: []string{svcName},
					},
				}
			}

			services[composeSvcName] = svcConfig
		}
	}

	// Apply depends_on after all services are built.
	applyDependsOn(services, params.Stack, params.Items)

	project := &composetypes.Project{
		Name:     projectName,
		Services: services,
	}

	// The lease's own bridge network, named by its verified ID. External: Fred
	// created or verified it, and Compose must neither create nor remove it.
	if networkID != "" {
		project.Networks = composetypes.Networks{
			leaseNetworkKey: composetypes.NetworkConfig{
				Name:     networkID,
				External: true,
			},
		}
	}

	return project, nil
}

// applyDependsOn maps manifest-level depends_on to Compose DependsOnConfig.
// It handles fan-out expansion: if service "web" depends on "db" and "db"
// has quantity 2, then each "web" instance depends on both "db-0" and "db-1".
func applyDependsOn(services composetypes.Services, stack *manifest.StackManifest, items []backend.LeaseItem) {
	// Build a map of service name → quantity for fan-out.
	qty := make(map[string]int, len(items))
	for _, item := range items {
		qty[item.ServiceName] = item.Quantity
	}

	for _, item := range items {
		svcName := item.ServiceName
		m := stack.Services[svcName]
		if len(m.DependsOn) == 0 {
			continue
		}

		// Build the Compose DependsOnConfig for this service.
		depConfig := make(composetypes.DependsOnConfig)
		for depName, cond := range m.DependsOn {
			depQty := qty[depName]
			if depQty <= 1 {
				// Simple: depends on the single instance (no suffix).
				depConfig[depName] = composetypes.ServiceDependency{
					Condition: cond.Condition,
					Required:  true,
				}
			} else {
				// Fan-out: depends on all instances of the dependency.
				for i := range depQty {
					composeName := fmt.Sprintf("%s-%d", depName, i)
					depConfig[composeName] = composetypes.ServiceDependency{
						Condition: cond.Condition,
						Required:  true,
					}
				}
			}
		}

		// Apply to all instances of the source service.
		if item.Quantity <= 1 {
			svc := services[svcName]
			svc.DependsOn = depConfig
			services[svcName] = svc
		} else {
			for i := range item.Quantity {
				composeName := fmt.Sprintf("%s-%d", svcName, i)
				svc := services[composeName]
				svc.DependsOn = depConfig
				services[composeName] = svc
			}
		}
	}
}

// composeServiceParams holds inputs for building a single Compose service config.
type composeServiceParams struct {
	LeaseUUID            string
	Tenant               string
	ProviderUUID         string
	CallbackURL          string
	LifecycleCallbackURL string
	MaintenanceID        shared.MaintenanceID
	BackendName          string
	FailCount            int
	ServiceName          string
	Instance             int
	SKU                  string
	Manifest             *manifest.Manifest
	Profile              SKUProfile
	ImgSetup             *imageSetup
	NetworkName          string
	Cfg                  *Config
	Ingress              ingressRoute
	Quantity             int
}

func buildComposeServiceConfig(p composeServiceParams) composetypes.ServiceConfig {
	svc := composetypes.ServiceConfig{
		Image:       p.Manifest.Image,
		PullPolicy:  composetypes.PullPolicyNever,
		CapDrop:     []string{"ALL"},
		SecurityOpt: []string{"no-new-privileges:true"},
		ReadOnly:    p.Cfg.IsReadonlyRootfs(),
		PidsLimit:   *p.Cfg.GetPidsLimit(),
		Restart:     composetypes.RestartPolicyNo,
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
		portSpecs := slices.Sorted(maps.Keys(p.Manifest.Ports))

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
		// Override is the only reader of a manifest health timing: it yields a
		// value Docker accepts, or false so the image/daemon default applies.
		svc.HealthCheck.Interval = composeHealthDuration(hc.Interval)
		svc.HealthCheck.Timeout = composeHealthDuration(hc.Timeout)
		if hc.Retries > 0 {
			r := uint64(hc.Retries)
			svc.HealthCheck.Retries = &r
		}
		svc.HealthCheck.StartPeriod = composeHealthDuration(hc.StartPeriod)
	}

	// Stop grace period.
	if p.Manifest.StopGracePeriod != nil {
		d := composetypes.Duration(p.Manifest.StopGracePeriod.Duration())
		svc.StopGracePeriod = &d
	}

	// Init process (tini).
	if p.Manifest.Init != nil {
		val := *p.Manifest.Init
		svc.Init = &val
	}

	// Expose (documentary ports, no host binding).
	if len(p.Manifest.Expose) > 0 {
		svc.Expose = composetypes.StringOrNumberList(p.Manifest.Expose)
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
		LabelManaged:              "true",
		LabelLeaseUUID:            p.LeaseUUID,
		LabelTenant:               p.Tenant,
		LabelProviderUUID:         p.ProviderUUID,
		LabelSKU:                  p.SKU,
		LabelCreatedAt:            time.Now().Format(time.RFC3339),
		LabelInstanceIndex:        strconv.Itoa(p.Instance),
		LabelFailCount:            strconv.Itoa(p.FailCount),
		LabelCallbackURL:          p.CallbackURL,
		LabelLifecycleCallbackURL: p.LifecycleCallbackURL,
		LabelMaintenanceID:        p.MaintenanceID.String(),
		LabelRuntimeContract:      currentRuntimeContract.label(),
		LabelBackendName:          p.BackendName,
		LabelServiceName:          p.ServiceName,
	}
	// Stored manifests may predate namespace admission rules. Never materialize
	// tenant labels that can override substrate ownership or ingress authority.
	for k, v := range p.Manifest.Labels {
		if manifest.IsReservedLabelKey(k) {
			continue
		}
		labels[k] = v
	}

	// Inject ingress labels for auto-discovery routing.
	applyIngressLabels(labels, ingressLabelParams{
		LeaseUUID:   p.LeaseUUID,
		ServiceName: p.ServiceName,
		Instance:    p.Instance,
		Quantity:    p.Quantity,
		NetworkName: p.NetworkName,
	}, p.Ingress)

	svc.Labels = labels

	return svc
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

// composeHealthDuration translates one manifest health timing. nil leaves the
// image's/daemon's default in place; a non-nil value is always one Docker
// accepts, because manifest.HealthDuration exposes nothing else (ENG-1127).
func composeHealthDuration(timing manifest.HealthDuration) *composetypes.Duration {
	value, ok := timing.Override()
	if !ok {
		return nil
	}
	d := composetypes.Duration(value)
	return &d
}

// composeProjectImages maps expanded instance services to the one image admitted
// for their manifest service. The compiler requires a complete matching set.
func composeProjectImages(project *composetypes.Project, setups map[string]*imageSetup) map[string]imageexec.Image {
	images := make(map[string]imageexec.Image, len(project.Services))
	for name, service := range project.Services {
		if setup := setups[service.Labels[LabelServiceName]]; setup != nil {
			images[name] = setup.Image
		}
	}
	return images
}
