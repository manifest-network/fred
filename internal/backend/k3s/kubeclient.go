package k3s

import (
	"errors"
	"fmt"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// kubeClientTimeout is the hardcoded reachability-probe timeout for
// ENG-133's /health endpoint. Applied at the rest.Config layer because
// client-go's Discovery().ServerVersion() does not take a context — the
// rest-layer timeout is the only place to bound the probe. ENG-134+
// methods that consume ctx-aware client-go APIs will continue to honor
// per-call contexts; this default doesn't affect them.
const kubeClientTimeout = 5 * time.Second

// buildKubeClient returns a typed clientset configured to reach the K3s
// API server. Resolution order (first hit wins):
//
//  1. cfg.KubeconfigPath non-empty: load that file. Prefer an absolute
//     path — clientcmd.BuildConfigFromFlags does NOT expand "~". Relative
//     paths work but are resolved against the process CWD.
//  2. In-cluster (rest.InClusterConfig): works only when the binary
//     runs inside a Pod that has the default service-account mounts at
//     /var/run/secrets/kubernetes.io/serviceaccount/. Falls through on
//     rest.ErrNotInCluster (the typical host-installed K3s case).
//  3. Default loading rules: $KUBECONFIG env var, then ~/.kube/config.
//     client-go expands "~" in this branch.
//
// A 5s timeout is applied at the rest.Config layer when the resolved
// config doesn't already set one (see kubeClientTimeout).
func buildKubeClient(cfg Config) (kubernetes.Interface, error) {
	rc, err := resolveRESTConfig(cfg)
	if err != nil {
		return nil, err
	}
	if rc.Timeout == 0 {
		rc.Timeout = kubeClientTimeout
	}
	return kubernetes.NewForConfig(rc)
}

// resolveRESTConfig walks the three-tier resolution order above and
// returns the first successful *rest.Config.
func resolveRESTConfig(cfg Config) (*rest.Config, error) {
	if cfg.KubeconfigPath != "" {
		rc, err := clientcmd.BuildConfigFromFlags("", cfg.KubeconfigPath)
		if err != nil {
			return nil, fmt.Errorf("loading kubeconfig %q: %w", cfg.KubeconfigPath, err)
		}
		return rc, nil
	}

	if rc, err := rest.InClusterConfig(); err == nil {
		return rc, nil
	} else if !errors.Is(err, rest.ErrNotInCluster) {
		return nil, fmt.Errorf("in-cluster config: %w", err)
	}

	loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{},
	)
	rc, err := loader.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("default kubeconfig loader: %w", err)
	}
	return rc, nil
}
