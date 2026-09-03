package shared

// ReleaseAuthorityClass identifies which disjoint on-disk authority a release
// carries. The zero value is invalid.
type ReleaseAuthorityClass uint8

const (
	ReleaseAuthorityTyped ReleaseAuthorityClass = iota + 1
	ReleaseAuthorityLegacy
)

// ReleaseRuntimeIdentity is an immutable read-only projection of either
// durable release-authority class. Its fields are private and its zero value is
// invalid, so consumers can handle v0.13 and current releases without being
// able to manufacture or mix their authority.
type ReleaseRuntimeIdentity struct {
	operationID          OperationID
	tenant               string
	providerUUID         string
	callbackURL          string
	lifecycleCallbackURL string
	class                ReleaseAuthorityClass
}

func (identity ReleaseRuntimeIdentity) OperationID() OperationID { return identity.operationID }
func (identity ReleaseRuntimeIdentity) Tenant() string           { return identity.tenant }
func (identity ReleaseRuntimeIdentity) ProviderUUID() string     { return identity.providerUUID }
func (identity ReleaseRuntimeIdentity) CallbackURL() string      { return identity.callbackURL }
func (identity ReleaseRuntimeIdentity) LifecycleCallbackURL() string {
	return identity.lifecycleCallbackURL
}
func (identity ReleaseRuntimeIdentity) Class() ReleaseAuthorityClass { return identity.class }

// RuntimeIdentity returns the only authority projection a Release can expose.
// It rejects invalid, mixed, or mismatched shapes even when a caller built the
// Release directly instead of reading it through the validated store.
func (release Release) RuntimeIdentity() (ReleaseRuntimeIdentity, bool) {
	if authority := release.RuntimeAuthority; authority != nil {
		if release.LegacyRuntimeAuthority != nil || !authority.valid ||
			!release.OperationID.Valid() || authority.operationID != release.OperationID {
			return ReleaseRuntimeIdentity{}, false
		}
		return ReleaseRuntimeIdentity{
			operationID:          authority.OperationID(),
			tenant:               authority.Tenant(),
			providerUUID:         authority.ProviderUUID(),
			callbackURL:          authority.CallbackURL(),
			lifecycleCallbackURL: authority.LifecycleCallbackURL(),
			class:                ReleaseAuthorityTyped,
		}, true
	}
	if authority := release.LegacyRuntimeAuthority; authority != nil {
		if !authority.valid || release.OperationID != "" {
			return ReleaseRuntimeIdentity{}, false
		}
		return ReleaseRuntimeIdentity{
			tenant:               authority.Tenant(),
			providerUUID:         authority.ProviderUUID(),
			callbackURL:          authority.CallbackURL(),
			lifecycleCallbackURL: authority.LifecycleCallbackURL(),
			class:                ReleaseAuthorityLegacy,
		}, true
	}
	return ReleaseRuntimeIdentity{}, false
}

func releaseRuntimeIdentityFor(release Release) (ReleaseRuntimeIdentity, bool) {
	return release.RuntimeIdentity()
}
