package docker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/manifest-network/fred/internal/backend"
)

// The stored plan interns immutable service configuration instead of charging
// each replica for the same environment. Instance identity stays separate and
// is reconstructed before the source-authority decoder can admit the plan.
type storedCompensationPlan struct {
	Version      uint8                         `json:"version"`
	CommonLabels map[string]string             `json:"common_labels,omitempty"`
	LabelValues  map[string]string             `json:"label_values,omitempty"`
	Configs      map[string]container.Config   `json:"configs"`
	Containers   []storedCompensationContainer `json:"containers"`
	VolumeRoots  []compensationVolumeRoot      `json:"volume_roots"`
}

type storedCompensationContainer struct {
	Name      string                    `json:"name"`
	ImageID   string                    `json:"image_id"`
	Platform  ocispec.Platform          `json:"platform"`
	Config    string                    `json:"config"`
	Hostname  string                    `json:"hostname"`
	Labels    map[string]string         `json:"labels,omitempty"`
	LabelRefs map[string]string         `json:"label_refs,omitempty"`
	Host      *container.HostConfig     `json:"host"`
	Networks  *network.NetworkingConfig `json:"networks"`
	Mounts    []ContainerMount          `json:"mounts"`
}

func compensationConfigDigest(config container.Config) (string, error) {
	encoded, err := json.Marshal(config)
	if err != nil {
		return "", err
	}
	return digest.FromBytes(encoded).String(), nil
}

func encodeCompensationSourcePlan(plan compensationSourcePlan) ([]byte, error) {
	if plan.Version != 1 || len(plan.Containers) == 0 || plan.Containers[0].Config == nil {
		return nil, errors.New("source codec requires a complete captured plan")
	}
	stored := storedCompensationPlan{Version: 1, Configs: make(map[string]container.Config),
		CommonLabels: maps.Clone(plan.Containers[0].Config.Labels), LabelValues: make(map[string]string), VolumeRoots: plan.VolumeRoots}
	for _, snapshot := range plan.Containers {
		if snapshot.Config == nil {
			return nil, errors.New("source codec cannot encode absent configuration")
		}
		for key, value := range stored.CommonLabels {
			if other, present := snapshot.Config.Labels[key]; !present || other != value {
				delete(stored.CommonLabels, key)
			}
		}
	}
	for _, snapshot := range plan.Containers {
		config := *snapshot.Config
		config.Labels, config.Hostname = nil, ""
		key, err := compensationConfigDigest(config)
		if err != nil {
			return nil, err
		}
		stored.Configs[key] = config
		instance := storedCompensationContainer{Name: snapshot.Name, ImageID: snapshot.ImageID, Platform: snapshot.Platform,
			Config: key, Hostname: snapshot.Config.Hostname, Labels: make(map[string]string), LabelRefs: make(map[string]string),
			Host: snapshot.Host, Networks: snapshot.Networks, Mounts: snapshot.Mounts}
		for label, value := range snapshot.Config.Labels {
			if _, common := stored.CommonLabels[label]; common {
				continue
			}
			if len(value) >= 1024 {
				ref := digest.FromString(value).String()
				stored.LabelValues[ref] = value
				instance.LabelRefs[label] = ref
			} else {
				instance.Labels[label] = value
			}
		}
		stored.Containers = append(stored.Containers, instance)
	}
	return json.Marshal(stored)
}

func decodeCompensationSourceSnapshot(encoded []byte) (compensationSourcePlan, error) {
	var stored storedCompensationPlan
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&stored); err != nil {
		return compensationSourcePlan{}, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return compensationSourcePlan{}, errors.New("source execution plan has trailing JSON")
	}
	if stored.Version != 1 || len(stored.Containers) == 0 || len(stored.Containers) > backend.MaxOperationQuantity || len(stored.Configs) == 0 {
		return compensationSourcePlan{}, errors.New("invalid normalized source execution plan")
	}
	for key, config := range stored.Configs {
		actual, err := compensationConfigDigest(config)
		if err != nil || key != actual || len(config.Labels) != 0 || config.Hostname != "" {
			return compensationSourcePlan{}, errors.New("source configuration digest or instance separation is invalid")
		}
	}
	for key, value := range stored.LabelValues {
		if key != digest.FromString(value).String() {
			return compensationSourcePlan{}, errors.New("source label content digest is invalid")
		}
	}
	usedConfigs, usedLabels := make(map[string]bool), make(map[string]bool)
	plan := compensationSourcePlan{Version: 1, VolumeRoots: stored.VolumeRoots}
	for _, instance := range stored.Containers {
		config, present := stored.Configs[instance.Config]
		if !present {
			return compensationSourcePlan{}, errors.New("source instance references missing configuration")
		}
		usedConfigs[instance.Config] = true
		// Interned configuration remains immutable during admission. The raw
		// creation boundary clones mutable members for the one instance being
		// dispatched, avoiding replica-sized allocations while decoding.
		config.Hostname = instance.Hostname
		config.Labels = make(map[string]string, len(stored.CommonLabels)+len(instance.Labels)+len(instance.LabelRefs))
		maps.Copy(config.Labels, stored.CommonLabels)
		for label, value := range instance.Labels {
			if _, duplicate := config.Labels[label]; duplicate {
				return compensationSourcePlan{}, errors.New("source instance overrides a common label")
			}
			config.Labels[label] = value
		}
		for label, key := range instance.LabelRefs {
			value, present := stored.LabelValues[key]
			if _, duplicate := config.Labels[label]; duplicate || !present {
				return compensationSourcePlan{}, fmt.Errorf("source instance label reference %q is invalid", label)
			}
			usedLabels[key] = true
			config.Labels[label] = value
		}
		plan.Containers = append(plan.Containers, compensationContainerRecord{Name: instance.Name, ImageID: instance.ImageID, Platform: instance.Platform,
			Config: &config, Host: instance.Host, Networks: instance.Networks, Mounts: instance.Mounts})
	}
	if len(usedConfigs) != len(stored.Configs) || len(usedLabels) != len(stored.LabelValues) {
		return compensationSourcePlan{}, errors.New("source execution plan contains unreferenced interned content")
	}
	return plan, nil
}

func cloneCompensationConfig(config container.Config) container.Config {
	config.Env = slices.Clone(config.Env)
	config.Cmd = slices.Clone(config.Cmd)
	config.Entrypoint = slices.Clone(config.Entrypoint)
	config.Shell = slices.Clone(config.Shell)
	config.OnBuild = slices.Clone(config.OnBuild)
	config.Volumes = maps.Clone(config.Volumes)
	config.ExposedPorts = maps.Clone(config.ExposedPorts)
	if config.Healthcheck != nil {
		health := *config.Healthcheck
		health.Test = slices.Clone(health.Test)
		config.Healthcheck = &health
	}
	if config.StopTimeout != nil {
		timeout := *config.StopTimeout
		config.StopTimeout = &timeout
	}
	return config
}
