package placementprobe

import (
	"errors"
	"fmt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/config"
)

// AuthenticatedFleet freezes every configured connection before an offline
// command opens mutation authority. It has no raw-config or policy accessor;
// later config edits cannot substitute an unauthenticated observation source.
type AuthenticatedFleet struct{ members []authenticatedMember }

type authenticatedMember struct {
	name   string
	policy backend.AuthenticatedEvidencePolicy
}

func NewAuthenticatedFleet(cfg *config.Config) (AuthenticatedFleet, error) {
	if cfg == nil || len(cfg.Backends) == 0 {
		return AuthenticatedFleet{}, errors.New("authenticated backend evidence requires a configured fleet")
	}
	members := make([]authenticatedMember, 0, len(cfg.Backends))
	for _, entry := range cfg.Backends {
		connection, err := cfg.BackendConnectionPolicy(entry.Name)
		if err != nil {
			return AuthenticatedFleet{}, err
		}
		policy, err := backend.NewAuthenticatedEvidencePolicy(connection)
		if err != nil {
			return AuthenticatedFleet{}, fmt.Errorf("backend %q: %w", entry.Name, err)
		}
		members = append(members, authenticatedMember{name: entry.Name, policy: policy})
	}
	return AuthenticatedFleet{members: members}, nil
}

func (fleet AuthenticatedFleet) NewClients() ([]Client, error) {
	if len(fleet.members) == 0 {
		return nil, errors.New("authenticated backend fleet is required")
	}
	clients := make([]Client, 0, len(fleet.members))
	for _, member := range fleet.members {
		client, err := member.policy.NewInventoryClient()
		if err != nil {
			return nil, err
		}
		clients = append(clients, client)
	}
	return clients, nil
}

func (fleet AuthenticatedFleet) NewIdentityBoundClients(resolver backend.BackendStorageIdentityResolver) ([]Client, error) {
	if len(fleet.members) == 0 {
		return nil, errors.New("authenticated backend fleet is required")
	}
	names := make([]string, 0, len(fleet.members))
	for _, member := range fleet.members {
		names = append(names, member.name)
	}
	pins, err := pinIdentities(names, resolver)
	if err != nil {
		return nil, err
	}
	clients := make([]Client, 0, len(fleet.members))
	for _, member := range fleet.members {
		client, err := member.policy.NewIdentityBoundInventoryClient(pins)
		if err != nil {
			return nil, err
		}
		clients = append(clients, client)
	}
	return clients, nil
}
