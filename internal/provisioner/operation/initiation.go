package operation

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/manifest-network/fred/internal/backend"
)

// operationSpec is the private, validated sum carried by every Registry
// operation. Production callers can construct only one of the purpose-specific
// arms below, so provision, restore, and recovery invariants do not depend on a
// defaultable public struct or a caller-selected Kind field.
type operationSpec struct {
	leaseUUID string
	tenant    string
	items     []backend.LeaseItem
	backend   string
	kind      Kind
}

func (spec operationSpec) valid() bool {
	if spec.leaseUUID == "" || spec.tenant == "" || len(spec.items) == 0 || !spec.kind.valid() {
		return false
	}
	switch spec.kind {
	case KindProvision:
		return spec.backend != ""
	case KindRestore:
		// A fresh restore is unbound until placement atomically derives its
		// retained source backend. Recovered restores already know that backend.
		return true
	default:
		return false
	}
}

func newOperationSpec(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
	kind Kind,
	allowRestoreBackend bool,
) (operationSpec, error) {
	if strings.TrimSpace(leaseUUID) == "" || !utf8.ValidString(leaseUUID) {
		return operationSpec{}, errors.New("operation lease UUID is required and must be valid UTF-8")
	}
	if strings.TrimSpace(tenant) == "" || !utf8.ValidString(tenant) {
		return operationSpec{}, errors.New("operation tenant is required and must be valid UTF-8")
	}
	if len(items) == 0 {
		return operationSpec{}, errors.New("operation items are required")
	}
	for index, item := range items {
		if strings.TrimSpace(item.SKU) == "" || !utf8.ValidString(item.SKU) ||
			!utf8.ValidString(item.ServiceName) || !utf8.ValidString(item.CustomDomain) {
			return operationSpec{}, fmt.Errorf("operation item %d contains invalid identity", index)
		}
	}
	if _, err := backend.ValidateOperationQuantities(items); err != nil {
		return operationSpec{}, fmt.Errorf("operation item quantities: %w", err)
	}
	if !kind.valid() {
		return operationSpec{}, ErrInvalidKind
	}
	if kind == KindProvision || allowRestoreBackend {
		if strings.TrimSpace(backendName) == "" || !utf8.ValidString(backendName) {
			return operationSpec{}, errors.New("operation backend is required and must be valid UTF-8")
		}
	} else if backendName != "" {
		return operationSpec{}, errors.New("fresh restore operation cannot select a backend")
	}
	spec := operationSpec{
		leaseUUID: leaseUUID,
		tenant:    tenant,
		items:     slices.Clone(items),
		backend:   backendName,
		kind:      kind,
	}
	if !spec.valid() {
		return operationSpec{}, errors.New("operation specification is invalid")
	}
	return spec, nil
}

// ProvisionInitiation is a validated fresh-provision arm. Its backend is
// mandatory at construction because provision routing precedes initiation.
// The zero value is invalid.
type ProvisionInitiation struct{ spec operationSpec }

func NewProvisionInitiation(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) (ProvisionInitiation, error) {
	spec, err := newOperationSpec(
		leaseUUID, tenant, items, backendName, KindProvision, false,
	)
	if err != nil {
		return ProvisionInitiation{}, err
	}
	return ProvisionInitiation{spec: spec}, nil
}

func (initiation ProvisionInitiation) valid() bool {
	return initiation.spec.valid() && initiation.spec.kind == KindProvision &&
		initiation.spec.backend != ""
}

// RestoreInitiation is a validated fresh-restore arm. It deliberately has no
// backend constructor argument; placement must derive and bind the retained
// source backend after durable admission. The zero value is invalid.
type RestoreInitiation struct{ spec operationSpec }

func NewRestoreInitiation(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
) (RestoreInitiation, error) {
	spec, err := newOperationSpec(
		leaseUUID, tenant, items, "", KindRestore, false,
	)
	if err != nil {
		return RestoreInitiation{}, err
	}
	return RestoreInitiation{spec: spec}, nil
}

func (initiation RestoreInitiation) valid() bool {
	return initiation.spec.valid() && initiation.spec.kind == KindRestore &&
		initiation.spec.backend == ""
}

// RecoveredOperation is the validated recovery arm. Recovery always requires
// an exact durable backend, including restore, and cannot allocate a new kind.
// The zero value is invalid.
type RecoveredOperation struct{ spec operationSpec }

func NewRecoveredProvision(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) (RecoveredOperation, error) {
	return newRecoveredOperation(leaseUUID, tenant, items, backendName, KindProvision)
}

func NewRecoveredRestore(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) (RecoveredOperation, error) {
	return newRecoveredOperation(leaseUUID, tenant, items, backendName, KindRestore)
}

func newRecoveredOperation(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
	kind Kind,
) (RecoveredOperation, error) {
	spec, err := newOperationSpec(
		leaseUUID, tenant, items, backendName, kind, true,
	)
	if err != nil {
		return RecoveredOperation{}, err
	}
	return RecoveredOperation{spec: spec}, nil
}

func (recovered RecoveredOperation) valid() bool {
	return recovered.spec.valid() && recovered.spec.backend != ""
}
