package inventory

// SingleReporterObservation is complete, lease-local evidence from one sealed
// collection epoch. Every configured backend supplied paired identity-bearing
// endpoints, exactly one reported this lease through a trusted arm, and every
// peer reported it absent. Ambiguity about a different lease grants no authority
// for this one and does not invalidate this evidence.
//
// This observation does not resolve historical ownership, establish a storage
// identity, or settle a lifecycle generation. The Store retains those decisions.
// Its zero value is invalid.
type SingleReporterObservation struct {
	snapshot    Snapshot
	leaseUUID   string
	backendName string
}

// PairedTopologyObservation proves endpoint coverage for the whole configured
// topology, independently of individual lease ambiguity. It carries no trusted
// lease payload, admission baseline, drain, or lifecycle authority.
type PairedTopologyObservation struct {
	snapshot Snapshot
	binding  Binding
}

func (snapshot Snapshot) PairedTopology(binding Binding) PairedTopologyObservation {
	if !snapshot.ValidFor(binding) {
		return PairedTopologyObservation{}
	}
	for _, backendName := range snapshot.collector.topology {
		provision, provisioned := snapshot.provision[backendName]
		retention, retained := snapshot.retention[backendName]
		if !provisioned || !retained || !provision.storageID.Valid() ||
			provision.storageID != retention.storageID {
			return PairedTopologyObservation{}
		}
	}
	return PairedTopologyObservation{snapshot: snapshot, binding: binding}
}

func (observation PairedTopologyObservation) ValidFor(binding Binding) bool {
	return observation.snapshot.ValidFor(binding)
}

// PairedCoverageObservation proves paired endpoint coverage for one exact,
// caller-named subset of the configured topology: every named backend answered
// both endpoints in this sealed collection with one valid storage identity. It
// lets an interrupted sweep's inherited fence be retired by re-observing only
// the backends that could have reported a lost positive, rather than every
// configured backend. Like PairedTopologyObservation it carries no lease
// payload, admission baseline, drain, or lifecycle authority, and callers stay
// responsible for binding each identity to its durable pin. Its zero value is
// invalid.
type PairedCoverageObservation struct {
	snapshot Snapshot
}

// PairedCoverage refuses a name repeated in backends. A name outside the
// configured topology has no endpoints to pair, because the collector records
// only configured backends, so it is refused as unobserved.
func (snapshot Snapshot) PairedCoverage(binding Binding, backends []string) PairedCoverageObservation {
	if !snapshot.ValidFor(binding) {
		return PairedCoverageObservation{}
	}
	covered := make(map[string]struct{}, len(backends))
	for _, backendName := range backends {
		if _, duplicate := covered[backendName]; duplicate {
			return PairedCoverageObservation{}
		}
		provision, provisioned := snapshot.provision[backendName]
		retention, retained := snapshot.retention[backendName]
		if !provisioned || !retained || !provision.storageID.Valid() ||
			provision.storageID != retention.storageID {
			return PairedCoverageObservation{}
		}
		covered[backendName] = struct{}{}
	}
	return PairedCoverageObservation{snapshot: snapshot}
}

func (observation PairedCoverageObservation) ValidFor(binding Binding) bool {
	return observation.snapshot.ValidFor(binding)
}

// SingleReporter derives the reporter itself; callers cannot select an owner
// or substitute an absent-only observation for positive membership.
func (coverage PairedTopologyObservation) SingleReporter(leaseUUID string) SingleReporterObservation {
	binding := coverage.binding
	if leaseUUID == "" || !coverage.ValidFor(binding) {
		return SingleReporterObservation{}
	}
	snapshot := coverage.snapshot
	var reporter string
	for _, backendName := range snapshot.collector.topology {
		provision := snapshot.provision[backendName]
		if snapshot.UntrustedReporter(binding, backendName, leaseUUID) {
			return SingleReporterObservation{}
		}
		if snapshot.TrustedReporter(binding, backendName, leaseUUID) {
			if reporter != "" {
				return SingleReporterObservation{}
			}
			reporter = backendName
		} else if !snapshot.OwnerAbsent(binding, backendName, provision.storageID, leaseUUID) {
			return SingleReporterObservation{}
		}
	}
	if reporter == "" {
		return SingleReporterObservation{}
	}
	return SingleReporterObservation{
		snapshot: snapshot, leaseUUID: leaseUUID, backendName: reporter,
	}
}

// Matches reattests the exact collector epoch, lease and derived reporter.
func (observation SingleReporterObservation) Matches(binding Binding, leaseUUID, backendName string) bool {
	return leaseUUID != "" && backendName != "" &&
		observation.leaseUUID == leaseUUID && observation.backendName == backendName &&
		observation.snapshot.ValidFor(binding)
}
