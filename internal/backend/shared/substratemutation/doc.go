// Package substratemutation defines the causal protocol between a durable
// mutation journal and an attested physical-substrate executor.
//
// Domain construction has two load-bearing value requirements which Go cannot
// express as generic constraints. Subject must be a concrete opaque, immutable,
// comparable capability (normally a private pointer-backed handle) whose
// detached payload came from the exact durable CAS or recovery read that mints
// its execution. Evidence must be a closed, immutable domain value and must
// deep-copy any source slices, maps, byte buffers, or pointers before the
// classifier returns it. Exporting aliases or accepting interface-shaped
// Subjects would let outside mutation weaken the causal binding.
//
// A Guard receives only a domain's narrow facade. Runner and the exact Subject
// are visible solely to that facade's construction closure, so the facade can
// derive every physical target from durable authority instead of accepting a
// caller-selected lease, container, or path. Runner becomes inert when Execute
// returns and never contains a raw backend writer. Step marks a tenant effect;
// Prepare brackets auxiliary cache/inspection work without turning a pre-effect
// refusal into an ambiguous tenant outcome. Every error remains aggregated, so
// an ignored preparation failure still poisons any later Step. A live result is
// Attested only after all entered tenant steps succeed and the construction-bound
// classifier proves one exact Evidence variant. Recovery uses the same
// classifier after re-reading durable Started authority, but its distinct
// execution type cannot replay live work. RunStep exposes the same panic-safe
// authorization/action/completion/release bracket for construction-bound,
// idempotent background convergence; its StepResult reports the effect boundary
// and recovered-panic fact without carrying a writer, target, or domain proof.
package substratemutation
