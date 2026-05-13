package k3s

import (
	"context"
	"fmt"

	"k8s.io/client-go/kubernetes"
)

// Health probes the K3s API server's reachability with a single
// Discovery().ServerVersion() call. The typed clientset is built
// lazily on the first call (under sync.Once); failures are cached
// for the process lifetime — fixing a bad kubeconfig requires a
// restart. This is acceptable for the ENG-133 scaffold (/health is
// the only consumer); ENG-134+ may revisit when Provision/etc.
// start consuming the client.
//
// On success returns nil. On any failure (build error or ServerVersion
// error) returns fmt.Errorf("k3s API unreachable: %w", err) so the
// HTTP handler in T5 can uniformly map it to 503 — mirroring docker
// backend's existing 503 path for unreachable backends.
//
// The ctx parameter is accepted for signature parity with the
// backendService interface but is not consumed by ServerVersion()
// directly. The probe's effective timeout is set on rest.Config at
// build time (see kubeclient.go).
func (b *Backend) Health(ctx context.Context) error {
	// TODO(ENG-134+): wire ctx through once provisioner methods consume
	// ctx-aware client-go APIs. The current ServerVersion() reachability
	// probe does not accept ctx; its effective bound is rest.Config.Timeout
	// set at build time (see kubeclient.go:kubeClientTimeout).
	_ = ctx

	b.kubeBuildOnce.Do(func() {
		cs, err := buildKubeClient(b.cfg)
		if err != nil {
			b.kubeBuildErr = fmt.Errorf("k3s API unreachable: %w", err)
			return
		}
		// kubernetes.NewForConfig returns *kubernetes.Clientset
		// (satisfies kubernetes.Interface). Store the concrete type
		// so atomic.Pointer[Clientset] can hold it.
		concrete, ok := cs.(*kubernetes.Clientset)
		if !ok {
			b.kubeBuildErr = fmt.Errorf("k3s API unreachable: unexpected client type %T", cs)
			return
		}
		b.kube.Store(concrete)
	})
	if b.kubeBuildErr != nil {
		return b.kubeBuildErr
	}
	cs := b.kube.Load()
	if cs == nil {
		return fmt.Errorf("k3s API unreachable: client build returned no client and no error")
	}
	if _, err := cs.Discovery().ServerVersion(); err != nil {
		return fmt.Errorf("k3s API unreachable: %w", err)
	}
	return nil
}
