package docker

import "strconv"

// runtimeContract versions the container-level runtime every launch produces:
// its network shape, security profile and other properties a running
// container keeps until it is recreated. Each container records the version it
// was created under in LabelRuntimeContract, so a container created under an
// older contract is a recorded fact rather than an inference. Bump
// currentRuntimeContract whenever such a property changes, and record why
// below.
//
//   - 1: the lease's own network (no shared per-tenant network).
type runtimeContract uint32

// currentRuntimeContract is the contract every launch of this release records.
const currentRuntimeContract runtimeContract = 1

// label is the LabelRuntimeContract value that records c.
func (c runtimeContract) label() string {
	return strconv.FormatUint(uint64(c), 10)
}

// predatesCurrent reports whether a container created under c must be
// recreated to run under the current contract. A container recorded under a
// newer contract, by a newer release, is not recreated by this one.
func (c runtimeContract) predatesCurrent() bool {
	return c < currentRuntimeContract
}

// parseRuntimeContract reads LabelRuntimeContract. A container without the
// label, or with a value this release cannot read, predates the contract and
// reads as zero.
func parseRuntimeContract(labels map[string]string) runtimeContract {
	parsed, err := strconv.ParseUint(labels[LabelRuntimeContract], 10, 32)
	if err != nil {
		return 0
	}
	return runtimeContract(parsed)
}
