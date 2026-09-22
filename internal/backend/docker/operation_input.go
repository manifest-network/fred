package docker

import (
	"bytes"
	"slices"

	"github.com/manifest-network/fred/internal/backend"
)

// provisionOperationInput is the backend-owned copy of a caller's provision
// request. The public DTO contains slices, so copying the struct alone does not
// transfer ownership: a caller could otherwise mutate Items or Payload after
// Provision returns while the lease actor's worker still consumes them.
//
// Keeping the owned form as a distinct, unexported type makes the ownership
// transition explicit. Async work is reconstructed from the sealed durable
// claim rather than directly from this value.
type provisionOperationInput backend.ProvisionRequest

func newProvisionOperationInput(request backend.ProvisionRequest) provisionOperationInput {
	request.Items = slices.Clone(request.Items)
	request.Payload = bytes.Clone(request.Payload)
	return provisionOperationInput(request)
}

func (input *provisionOperationInput) normalizeItems() error {
	request := backend.ProvisionRequest(*input)
	if err := backend.NormalizeProvisionRequest(&request); err != nil {
		return err
	}
	*input = provisionOperationInput(request)
	return nil
}

func (input provisionOperationInput) routingSKU() string {
	if len(input.Items) == 0 {
		return ""
	}
	return input.Items[0].SKU
}

// restoreOperationInput is the backend-owned copy of a caller's restore DTO.
// LeaseItem is value-only today, so cloning the slice is a complete deep copy.
// The distinct type prevents the async path from accepting the caller DTO by
// accident.
type restoreOperationInput backend.RestoreRequest

func newRestoreOperationInput(request backend.RestoreRequest) restoreOperationInput {
	request.Items = slices.Clone(request.Items)
	return restoreOperationInput(request)
}

func (input *restoreOperationInput) normalizeItems() error {
	request := backend.ProvisionRequest{Items: input.Items}
	if err := backend.NormalizeProvisionRequest(&request); err != nil {
		return err
	}
	input.Items = request.Items
	return nil
}
