package docker

import (
	"context"
	"fmt"
	"slices"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// ingressRoute is the only input accepted by the label writer. Port selection
// and custom-domain admission belong to construction, never to rendering.
// A zero route emits no ingress labels.
type ingressRoute struct {
	config IngressConfig
	port   int
	domain string
}

func newIngressRoute(config IngressConfig, ports map[string]manifest.PortConfig, domain string) ingressRoute {
	if !config.Enabled {
		return ingressRoute{}
	}
	port, ok := SelectIngressPort(ports)
	if !ok {
		return ingressRoute{}
	}
	config.CustomDomainMiddlewares = slices.Clone(config.CustomDomainMiddlewares)
	route := ingressRoute{config: config, port: port}
	if domain != "" && validateCustomDomain(domain, config.WildcardDomain) == nil {
		route.domain = domain
	}
	return route
}

// effectiveIngressPlan owns the metadata that will actually be emitted. Desired
// chain items remain caller-owned input; journal acceptance receives a detached
// projection from this plan, and renderers receive its per-service routes.
type effectiveIngressPlan struct {
	items  []backend.LeaseItem
	routes map[string]ingressRoute
}

func newEffectiveIngressPlan(config IngressConfig, stack *manifest.StackManifest, desired []backend.LeaseItem) (effectiveIngressPlan, error) {
	if stack == nil {
		return effectiveIngressPlan{}, fmt.Errorf("ingress planning requires a manifest")
	}
	plan := effectiveIngressPlan{items: slices.Clone(desired), routes: make(map[string]ingressRoute, len(desired))}
	for index, item := range desired {
		service := stack.Services[item.ServiceName]
		if service == nil {
			return effectiveIngressPlan{}, fmt.Errorf("ingress service %q is absent from its manifest", item.ServiceName)
		}
		route := newIngressRoute(config, service.Ports, item.CustomDomain)
		plan.routes[item.ServiceName] = route
		plan.items[index].CustomDomain = route.domain
	}
	return plan, nil
}

func (plan effectiveIngressPlan) effectiveItems() []backend.LeaseItem {
	return slices.Clone(plan.items)
}

// admitIngressPlan performs DNS I/O once, before durable acceptance. No live
// provision projection is mutated, and execution never revisits this decision.
func (b *Backend) admitIngressPlan(ctx context.Context, stack *manifest.StackManifest, desired []backend.LeaseItem) (effectiveIngressPlan, error) {
	plan, err := newEffectiveIngressPlan(b.cfg.Ingress, stack, desired)
	if err != nil {
		return effectiveIngressPlan{}, err
	}
	for index, item := range plan.items {
		if item.CustomDomain == "" || b.dnsGateAllows(ctx, item.CustomDomain) {
			continue
		}
		route := plan.routes[item.ServiceName]
		route.domain = ""
		plan.routes[item.ServiceName] = route
		plan.items[index].CustomDomain = ""
	}
	return plan, nil
}

// restoreIngressPlan reconstructs rendering from an already accepted durable
// decision, without DNS. Configuration changes cannot silently discard a domain
// that the exact operation/release still authorizes.
func restoreIngressPlan(config IngressConfig, stack *manifest.StackManifest, effective []backend.LeaseItem) (effectiveIngressPlan, error) {
	plan, err := newEffectiveIngressPlan(config, stack, effective)
	if err != nil {
		return effectiveIngressPlan{}, err
	}
	if !slices.Equal(plan.items, effective) {
		return effectiveIngressPlan{}, fmt.Errorf("durable effective ingress differs from current rendering capability")
	}
	return plan, nil
}
