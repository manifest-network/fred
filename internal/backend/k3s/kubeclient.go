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
// rest-layer timeout is the only place to bound the probe.
//
// IMPORTANT: rest.Config.Timeout is a client-wide HTTP timeout, not a
// per-request one. Once ENG-134+ wires the clientset into Pod/Service/PVC
// operations, every request made with this clientset will be capped at
// 5s regardless of the caller's context deadline. If real provisioner
// methods need to make longer-running calls (e.g., Watch streams, log
// streaming), ENG-134+ should either build a separate clientset for
// those paths or refactor to use per-request transports.
const kubeClientTimeout = 5 * time.Second

// kubeClientQPS / kubeClientBurst raise client-go's default 5/10
// rate-limit ceiling. ENG-133 issues a single Discovery() request per
// /health call, so the defaults would suffice today — but ENG-134+'s
// provisioner will fan out concurrent Pod/Service/PVC reads and writes
// per lease, and 5 QPS is too low to avoid silent throttling at that
// volume. Setting these now is cheap insurance and keeps the limit in
// one place.
//
// Standard kubeconfig YAML has no fields for QPS/Burst (they're not part
// of the kubeconfig schema), so these values are effectively hardcoded
// from an operator's perspective. The if-zero check below only preserves
// a value that Go code earlier in the resolver chain has set on the
// rest.Config — not a kubeconfig override. Adding config-driven
// tunability is a documented follow-up.
const (
	kubeClientQPS   = 50
	kubeClientBurst = 100
)

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
	if rc.QPS == 0 {
		rc.QPS = kubeClientQPS
	}
	if rc.Burst == 0 {
		rc.Burst = kubeClientBurst
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
