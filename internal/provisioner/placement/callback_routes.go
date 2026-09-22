package placement

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/manifest-network/fred/internal/callbackurl"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

// CallbackRouteFactory is the sole callback origin bound to backend execution.
// Its parsed URL is opaque and immutable; purpose facets can mint typed pairs
// but cannot replace the origin independently.
type CallbackRouteFactory struct {
	base            callbackurl.Base
	lifecycleRoutes *lifecycle.RouteFactory
	valid           bool
}

func NewCallbackRouteFactory(rawBase string) (*CallbackRouteFactory, error) {
	base, err := callbackurl.ParseBase(rawBase)
	if err != nil {
		return nil, fmt.Errorf("callback route factory: %w", err)
	}
	lifecycleRoutes, err := lifecycle.NewRouteFactory(rawBase)
	if err != nil {
		return nil, fmt.Errorf("callback lifecycle route factory: %w", err)
	}
	return &CallbackRouteFactory{
		base: base, lifecycleRoutes: lifecycleRoutes, valid: true,
	}, nil
}

func (factory *CallbackRouteFactory) Valid() bool {
	return factory != nil && factory.valid && factory.base.String() != "" &&
		factory.lifecycleRoutes != nil && factory.lifecycleRoutes.Valid()
}

// ForOperation derives the inseparable operation and lifecycle callback pair
// for one typed operation. Callers cannot supply the lifecycle identity
// independently, so the two routes always name the same generation.
func (factory *CallbackRouteFactory) ForOperation(id operation.OperationID) (CallbackPair, error) {
	if !factory.Valid() || !id.Valid() {
		return CallbackPair{}, errors.New("valid callback route factory and operation are required")
	}
	operationURL, err := factory.url(operation.QueryParameter, id.String())
	if err != nil {
		return CallbackPair{}, err
	}
	lifecycleID, err := lifecycle.FromOperationID(id)
	if err != nil {
		return CallbackPair{}, fmt.Errorf("derive lifecycle callback identity: %w", err)
	}
	lifecycleRoute, err := factory.lifecycleRoutes.For(lifecycleID)
	if err != nil {
		return CallbackPair{}, err
	}
	return newCallbackPair(id, operationURL, lifecycleRoute.URL())
}

func (factory *CallbackRouteFactory) url(parameter, value string) (string, error) {
	endpoint, err := factory.base.ProvisionURL()
	if err != nil {
		return "", err
	}
	selector := url.QueryEscape(parameter) + "=" + url.QueryEscape(value)
	if endpoint.RawQuery == "" {
		endpoint.RawQuery = selector
	} else {
		endpoint.RawQuery += "&" + selector
	}
	return endpoint.String(), nil
}
