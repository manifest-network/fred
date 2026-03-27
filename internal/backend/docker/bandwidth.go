package docker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"syscall"
	"time"

	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"

	"github.com/manifest-network/fred/internal/backend"
)

// bandwidthManager applies and removes network bandwidth limits on containers.
type bandwidthManager interface {
	// Apply sets egress+ingress bandwidth limits on a running container.
	// Returns an error if egress shaping fails. Ingress shaping failures are
	// logged internally and do not cause Apply to return an error.
	// Callers should treat Apply errors as best-effort (log and continue).
	Apply(ctx context.Context, containerID string, rateMbps int64, burstKB int, latencyMs int, kernelHZ int) error

	// Remove removes the IFB device used for ingress shaping. The egress TBF
	// qdisc on the host-side veth is not explicitly removed — it is cleaned up
	// when the container (and its veth) is destroyed.
	// Idempotent: no error if the IFB device was never created or already removed.
	Remove(ctx context.Context, containerID string) error

	// Validate checks for IFB kernel module availability and CAP_NET_ADMIN.
	// Returns an error if CAP_NET_ADMIN is missing (bandwidth shaping cannot work).
	// If only IFB is unavailable, ingress shaping is disabled (egress-only mode) and a warning is logged.
	Validate() error
}

// containerPIDResolver resolves a container ID to its host PID.
type containerPIDResolver interface {
	ContainerPID(ctx context.Context, containerID string) (int, error)
}

// noopBandwidthManager is used when no SKUs have bandwidth_mbps > 0.
type noopBandwidthManager struct{}

func (n *noopBandwidthManager) Apply(_ context.Context, _ string, _ int64, _ int, _ int, _ int) error {
	return nil
}

func (n *noopBandwidthManager) Remove(_ context.Context, _ string) error {
	return nil
}

func (n *noopBandwidthManager) Validate() error {
	return nil
}

// tcBandwidthManager uses Linux tc (traffic control) with TBF via netlink.
type tcBandwidthManager struct {
	pidResolver containerPIDResolver
	logger      *slog.Logger
	ifbSupport  bool // set by Validate
}

// newBandwidthManager creates a bandwidthManager appropriate for the config.
func newBandwidthManager(pidResolver containerPIDResolver, hasBandwidthSKUs bool, logger *slog.Logger) bandwidthManager {
	if !hasBandwidthSKUs {
		return &noopBandwidthManager{}
	}
	return &tcBandwidthManager{
		pidResolver: pidResolver,
		logger:      logger,
	}
}

// Validate probes for IFB kernel module support by creating a temporary IFB device.
// If the probe fails due to missing CAP_NET_ADMIN, returns a hard error (egress
// shaping also requires it). If the probe fails for other reasons (missing IFB
// module), ingress shaping is disabled but egress-only mode still works.
func (m *tcBandwidthManager) Validate() error {
	const probeName = "ifb-fred-probe"

	ifb := &netlink.Ifb{LinkAttrs: netlink.LinkAttrs{Name: probeName}}
	err := netlink.LinkAdd(ifb)

	// If EEXIST, a previous run left a stale probe — delete it and retry once.
	if errors.Is(err, syscall.EEXIST) {
		if stale, lookupErr := netlink.LinkByName(probeName); lookupErr == nil {
			_ = netlink.LinkDel(stale) //nolint:errcheck // best-effort cleanup
		}
		err = netlink.LinkAdd(ifb)
	}

	if err != nil {
		// If we lack CAP_NET_ADMIN, egress shaping will also fail.
		if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
			return fmt.Errorf("CAP_NET_ADMIN required for bandwidth shaping: %w", err)
		}
		// Persistent EEXIST after delete+retry means the IFB module IS loaded
		// (the device exists), but the stale probe could not be removed. Treat
		// IFB as available since the module is clearly present.
		if errors.Is(err, syscall.EEXIST) {
			m.logger.Warn("stale IFB probe device could not be removed; treating IFB as available",
				"device", probeName, "error", err)
			m.ifbSupport = true
			return nil
		}
		m.logger.Warn("IFB kernel module not available — ingress (upload) bandwidth shaping will be disabled; "+
			"egress (download) shaping still works. Load the ifb module: modprobe ifb",
			"error", err)
		return nil
	}
	// Probe succeeded — clean up and record support.
	if delErr := netlink.LinkDel(ifb); delErr != nil {
		m.logger.Warn("failed to clean up IFB probe device; stale device may affect future startups",
			"device", probeName, "error", delErr)
	}
	m.ifbSupport = true
	m.logger.Info("bandwidth manager validated: IFB device creation succeeded (implies CAP_NET_ADMIN available)")
	return nil
}

// Apply sets egress+ingress bandwidth limits on a running container.
func (m *tcBandwidthManager) Apply(ctx context.Context, containerID string, rateMbps int64, burstKB int, latencyMs int, kernelHZ int) error {
	if rateMbps <= 0 {
		return nil
	}

	pid, err := m.pidResolver.ContainerPID(ctx, containerID)
	if err != nil {
		return fmt.Errorf("resolve container PID: %w", err)
	}

	// Find host-side veth with retry. The veth peer may not be immediately visible
	// after StartContainer returns because Docker's network setup is asynchronous.
	// Three attempts at 100ms intervals covers the typical window.
	var hostVeth netlink.Link
	for attempt := range 3 {
		hostVeth, err = findHostVeth(pid)
		if err == nil {
			break
		}
		if attempt < 2 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	if err != nil {
		return fmt.Errorf("find host veth for PID %d: %w", pid, err)
	}

	rate, burst, limit := tbfParams(rateMbps, burstKB, latencyMs, kernelHZ)

	// Egress shaping (limits container download): TBF on host-veth egress.
	if err := applyTBF(hostVeth, rate, burst, limit); err != nil {
		return fmt.Errorf("apply egress TBF on %s: %w", hostVeth.Attrs().Name, err)
	}

	// Ingress shaping (limits container upload): IFB redirect.
	if m.ifbSupport {
		ifbName := ifbDeviceName(containerID)
		if err := applyIngressViaIFB(hostVeth, ifbName, rate, burst, limit); err != nil {
			m.logger.Warn("ingress shaping failed, egress-only active",
				"veth", hostVeth.Attrs().Name, "error", err)
		}
	}

	return nil
}

// Remove deletes the IFB device used for ingress shaping. The egress TBF qdisc
// on the host-side veth is not explicitly removed — it is cleaned up when the
// container (and its veth) is destroyed. Idempotent: returns nil if the device
// was never created or already removed.
func (m *tcBandwidthManager) Remove(_ context.Context, containerID string) error {
	ifbName := ifbDeviceName(containerID)
	link, err := netlink.LinkByName(ifbName)
	if err != nil {
		// LinkNotFoundError means the device was already removed or never created.
		var lnf netlink.LinkNotFoundError
		if errors.As(err, &lnf) {
			return nil
		}
		return fmt.Errorf("lookup IFB device %s: %w", ifbName, err)
	}
	if err := netlink.LinkDel(link); err != nil {
		return fmt.Errorf("delete IFB device %s: %w", ifbName, err)
	}
	return nil
}

// ifbDeviceName returns the IFB device name for a container.
// 4 ("ifb-") + 11 chars = 15 chars, the maximum interface name length (IFNAMSIZ-1).
func ifbDeviceName(containerID string) string {
	if len(containerID) > 11 {
		containerID = containerID[:11]
	}
	return "ifb-" + containerID
}

// findHostVeth enters the container's network namespace, finds the eth0 peer
// index, then looks up the corresponding veth on the host.
func findHostVeth(pid int) (_ netlink.Link, retErr error) {
	// Lock OS thread because netns operations change the calling thread's namespace.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Save current (host) netns.
	hostNS, err := netns.Get()
	if err != nil {
		return nil, fmt.Errorf("get host netns: %w", err)
	}
	defer hostNS.Close() //nolint:errcheck // netns file descriptor close

	// Enter container netns.
	containerNS, err := netns.GetFromPid(pid)
	if err != nil {
		return nil, fmt.Errorf("get container netns for PID %d: %w", pid, err)
	}
	defer containerNS.Close() //nolint:errcheck // netns file descriptor close

	if err := netns.Set(containerNS); err != nil {
		return nil, fmt.Errorf("enter container netns: %w", err)
	}
	// Always restore host netns before returning. This prevents leaking a
	// thread stuck in the container netns back into the Go scheduler. The
	// explicit restore below handles the happy path; the defer is a safety
	// net for early error returns (redundant but harmless on the happy path).
	defer func() {
		if restoreErr := netns.Set(hostNS); restoreErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("CRITICAL: failed to restore host netns: %w", restoreErr))
		}
	}()

	// Find eth0 inside container namespace and get its peer index.
	eth0, err := netlink.LinkByName("eth0")
	if err != nil {
		return nil, fmt.Errorf("find eth0 in container netns: %w", err)
	}
	peerIndex := eth0.Attrs().ParentIndex

	// Return to host netns before looking up the veth on the host side.
	if err := netns.Set(hostNS); err != nil {
		return nil, fmt.Errorf("restore host netns: %w", err)
	}

	hostVeth, err := netlink.LinkByIndex(peerIndex)
	if err != nil {
		return nil, fmt.Errorf("find host veth by index %d: %w", peerIndex, err)
	}

	return hostVeth, nil
}

// tbfParams calculates TBF qdisc parameters from human-friendly inputs.
// Zero latencyMs defaults to 50ms; zero kernelHZ defaults to 250.
// Burst is auto-calculated when burstKB is 0, with a minimum of
// max(32KB, ceil(rate/HZ)) and an absolute floor of 1500 bytes (MTU).
func tbfParams(rateMbps int64, burstKB int, latencyMs int, kernelHZ int) (rate uint64, burst uint32, limit uint32) {
	const maxUint32 = uint64(^uint32(0))

	rate = uint64(rateMbps) * 1_000_000 / 8 // Mbps → bytes/sec

	if latencyMs <= 0 {
		latencyMs = 50
	}
	if kernelHZ <= 0 {
		kernelHZ = 250
	}

	if burstKB > 0 {
		burstBytes := uint64(burstKB) * 1024
		if burstBytes > maxUint32 {
			burstBytes = maxUint32
		}
		burst = uint32(burstBytes)
	} else {
		// Auto-calculate: max(32KB, ceil(rate / kernelHZ))
		autoBurst := (rate + uint64(kernelHZ) - 1) / uint64(kernelHZ)
		if autoBurst > maxUint32 {
			autoBurst = maxUint32
		}
		burst = max(32*1024, uint32(autoBurst))
	}
	// Floor: must be >= MTU (1500 bytes)
	burst = max(burst, 1500)

	// limit = rate * latency_seconds + burst
	// Saturate to uint32 max to prevent silent overflow at extreme rates.
	limitU64 := rate*uint64(latencyMs)/1000 + uint64(burst)
	if limitU64 > maxUint32 {
		limitU64 = maxUint32
	}
	limit = uint32(limitU64)
	return
}

// applyTBF replaces the root qdisc on a link with a TBF (Token Bucket Filter).
func applyTBF(link netlink.Link, rate uint64, burst, limit uint32) error {
	qdisc := &netlink.Tbf{
		QdiscAttrs: netlink.QdiscAttrs{
			LinkIndex: link.Attrs().Index,
			Handle:    netlink.MakeHandle(1, 0),
			Parent:    netlink.HANDLE_ROOT,
		},
		Rate:   rate,
		Limit:  limit,
		Buffer: burst,
	}
	return netlink.QdiscReplace(qdisc)
}

// applyIngressViaIFB creates an IFB device, attaches an ingress qdisc to the
// veth, redirects all ingress traffic to the IFB, and applies TBF on the IFB.
// If the IFB already exists (EEXIST), it is reused to make Apply idempotent.
// On failure: if we created the IFB, both the IFB device and the ingress qdisc
// are cleaned up. If we reused a pre-existing IFB, cleanup is skipped to
// preserve the old working setup.
func applyIngressViaIFB(veth netlink.Link, ifbName string, rate uint64, burst, limit uint32) (retErr error) {
	// Create IFB device. If it already exists (e.g., stale from a previous
	// Apply), reuse it to make Apply idempotent for a given containerID.
	ifb := &netlink.Ifb{LinkAttrs: netlink.LinkAttrs{Name: ifbName}}
	createdIFB := true
	if err := netlink.LinkAdd(ifb); err != nil {
		if errors.Is(err, syscall.EEXIST) {
			createdIFB = false
		} else {
			return fmt.Errorf("create IFB device %s: %w", ifbName, err)
		}
	}

	// Clean up on failure. When we created the IFB, clean up both the ingress
	// qdisc and the IFB device. When reusing a pre-existing IFB, skip cleanup
	// entirely to preserve the old working setup.
	success := false
	defer func() {
		if success || !createdIFB {
			return
		}
		// Best-effort: remove ingress qdisc from the veth.
		ingress := &netlink.Ingress{
			QdiscAttrs: netlink.QdiscAttrs{
				LinkIndex: veth.Attrs().Index,
				Handle:    netlink.MakeHandle(0xffff, 0),
				Parent:    netlink.HANDLE_INGRESS,
			},
		}
		_ = netlink.QdiscDel(ingress) //nolint:errcheck // best-effort cleanup
		if delErr := netlink.LinkDel(ifb); delErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("IFB cleanup of %s failed: %w", ifbName, delErr))
		}
	}()

	ifbLink, err := netlink.LinkByName(ifbName)
	if err != nil {
		return fmt.Errorf("lookup IFB device %s: %w", ifbName, err)
	}

	if err := netlink.LinkSetUp(ifbLink); err != nil {
		return fmt.Errorf("bring up IFB device %s: %w", ifbName, err)
	}

	// Add ingress qdisc to the veth.
	ingress := &netlink.Ingress{
		QdiscAttrs: netlink.QdiscAttrs{
			LinkIndex: veth.Attrs().Index,
			Handle:    netlink.MakeHandle(0xffff, 0),
			Parent:    netlink.HANDLE_INGRESS,
		},
	}
	if err := netlink.QdiscReplace(ingress); err != nil {
		return fmt.Errorf("add ingress qdisc to %s: %w", veth.Attrs().Name, err)
	}

	// Redirect all ingress traffic to the IFB device.
	filter := &netlink.U32{
		FilterAttrs: netlink.FilterAttrs{
			LinkIndex: veth.Attrs().Index,
			Parent:    netlink.MakeHandle(0xffff, 0),
			Priority:  1,
			Protocol:  0x0003, // ETH_P_ALL
		},
		Actions: []netlink.Action{
			&netlink.MirredAction{
				ActionAttrs:  netlink.ActionAttrs{Action: netlink.TC_ACT_STOLEN},
				MirredAction: netlink.TCA_EGRESS_REDIR,
				Ifindex:      ifbLink.Attrs().Index,
			},
		},
	}
	if err := netlink.FilterReplace(filter); err != nil {
		return fmt.Errorf("add redirect filter on %s: %w", veth.Attrs().Name, err)
	}

	// Apply TBF on the IFB device's egress.
	if err := applyTBF(ifbLink, rate, burst, limit); err != nil {
		return fmt.Errorf("apply TBF on IFB device %s: %w", ifbName, err)
	}

	success = true
	return nil
}

// applyBandwidthLimit is a Backend helper that applies bandwidth limits to a
// container using the configured bandwidthManager. Best-effort: failures are
// logged as warnings and do not fail the provision.
func (b *Backend) applyBandwidthLimit(ctx context.Context, containerID string, profile SKUProfile, logger *slog.Logger) {
	if profile.BandwidthMbps <= 0 {
		return
	}
	if bwErr := b.bandwidth.Apply(ctx, containerID, profile.BandwidthMbps,
		profile.BurstKB, profile.LatencyMs, b.cfg.GetKernelHZ()); bwErr != nil {
		logger.Warn("failed to apply bandwidth limit",
			"bandwidth_mbps", profile.BandwidthMbps,
			"container_id", shortID(containerID),
			"error", bwErr)
		bandwidthApplyTotal.WithLabelValues("failure").Inc()
		return
	}
	logger.Info("bandwidth limit applied",
		"bandwidth_mbps", profile.BandwidthMbps,
		"container_id", shortID(containerID))
	bandwidthApplyTotal.WithLabelValues("success").Inc()
}

// removeBandwidthLimit is a Backend helper that removes bandwidth limits (IFB
// device) for a container. Best-effort: failures are logged as warnings.
func (b *Backend) removeBandwidthLimit(ctx context.Context, containerID string, logger *slog.Logger) {
	if err := b.bandwidth.Remove(ctx, containerID); err != nil {
		logger.Warn("failed to remove IFB device",
			"container_id", shortID(containerID),
			"error", err)
	}
}

// applyStackBandwidthLimits applies bandwidth limits to all containers in a
// stack deployment. Each service's containers get the bandwidth limit from
// their SKU profile.
func (b *Backend) applyStackBandwidthLimits(ctx context.Context, items []backend.LeaseItem, profiles map[string]SKUProfile, serviceContainers map[string][]string, logger *slog.Logger) {
	for _, item := range items {
		profile, ok := profiles[item.SKU]
		if !ok {
			logger.Warn("missing bandwidth profile for SKU",
				"service", item.ServiceName, "sku", item.SKU)
			continue
		}
		if profile.BandwidthMbps <= 0 {
			continue
		}
		svcLogger := logger.With("service", item.ServiceName)
		for _, cid := range serviceContainers[item.ServiceName] {
			b.applyBandwidthLimit(ctx, cid, profile, svcLogger)
		}
	}
}
