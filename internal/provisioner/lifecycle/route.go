package lifecycle

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"

	"github.com/manifest-network/fred/internal/callbackurl"
)

// RouteFactory is a construction-time capability bound to one validated
// callback base. Runtime requests can ask it for an exact lifecycle route, but
// cannot supply or splice a destination of their own.
//
// The zero value is invalid.
type RouteFactory struct {
	base callbackurl.Base
}

// NewRouteFactory validates and binds the sole callback origin/path prefix
// from which this factory may mint lifecycle routes.
func NewRouteFactory(rawBase string) (*RouteFactory, error) {
	base, err := callbackurl.ParseBase(rawBase)
	if err != nil {
		return nil, fmt.Errorf("lifecycle callback base: %w", err)
	}
	return &RouteFactory{base: base}, nil
}

// Valid reports whether factory was constructed from a validated callback
// base.
func (factory *RouteFactory) Valid() bool {
	return factory != nil && factory.base.String() != ""
}

// Route is an immutable lifecycle callback destination minted by a
// RouteFactory. Its fields are private so a command caller cannot pair a valid
// lifecycle ID with a different callback origin.
//
// The zero value is invalid.
type Route struct {
	kind   routeKind
	id     ID
	url    string
	issuer *RouteFactory
}

var (
	_ fmt.Formatter  = Route{}
	_ slog.LogValuer = Route{}
)

const routeDiagnostic = "lifecycle.Route{redacted}"

// Format keeps generic formatting from reflecting the capability-bearing URL
// or lifecycle generation. Wire code must request those values explicitly.
func (route Route) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte(routeDiagnostic))
}

// LogValue makes direct structured logging of a Route diagnostic-only.
func (route Route) LogValue() slog.Value {
	return slog.StringValue(routeDiagnostic)
}

type routeKind uint8

const (
	routeInvalid routeKind = iota
	routeLegacy
	routeTyped
)

// For mints the exact route for id from this factory's fixed base.
func (factory *RouteFactory) For(id ID) (Route, error) {
	if !factory.Valid() {
		return Route{}, errors.New("lifecycle callback route factory is invalid")
	}
	text, err := id.MarshalText()
	if err != nil {
		return Route{}, fmt.Errorf("lifecycle callback route ID: %w", err)
	}
	endpoint, err := factory.base.ProvisionURL()
	if err != nil {
		return Route{}, fmt.Errorf("lifecycle callback route: %w", err)
	}
	selector := url.QueryEscape(QueryParameter) + "=" + url.QueryEscape(string(text))
	if endpoint.RawQuery == "" {
		endpoint.RawQuery = selector
	} else {
		endpoint.RawQuery += "&" + selector
	}
	return Route{
		kind: routeTyped, id: id, url: endpoint.String(), issuer: factory,
	}, nil
}

// Legacy mints the explicit tokenless route used only by a lifecycle that the
// placement store provenance-gated as v0.13 authority. The distinct route kind
// prevents an invalid typed ID from being mistaken for legacy authority.
func (factory *RouteFactory) Legacy() (Route, error) {
	if !factory.Valid() {
		return Route{}, errors.New("lifecycle callback route factory is invalid")
	}
	endpoint, err := factory.base.ProvisionURL()
	if err != nil {
		return Route{}, fmt.Errorf("legacy lifecycle callback route: %w", err)
	}
	return Route{kind: routeLegacy, url: endpoint.String(), issuer: factory}, nil
}

// Valid reports whether route was minted by a live factory for a valid typed
// lifecycle generation.
func (route Route) Valid() bool {
	if route.issuer == nil || !route.issuer.Valid() || route.url == "" {
		return false
	}
	switch route.kind {
	case routeLegacy:
		return !route.id.Valid()
	case routeTyped:
		return route.id.Valid()
	default:
		return false
	}
}

// IsLegacy reports whether route is the explicit tokenless generation minted
// for a provenance-gated pre-v0.14 lifecycle. Invalid routes return false.
func (route Route) IsLegacy() bool { return route.Valid() && route.kind == routeLegacy }

// ID returns the lifecycle generation carried by this route. Invalid routes
// return the zero ID.
func (route Route) ID() ID {
	if !route.Valid() || route.kind != routeTyped {
		return ID{}
	}
	return route.id
}

// URL returns the immutable wire destination. Invalid routes return empty.
func (route Route) URL() string {
	if !route.Valid() {
		return ""
	}
	return route.url
}
