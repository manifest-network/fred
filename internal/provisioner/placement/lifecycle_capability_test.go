package placement

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
)

// evidenceFreeSentinelRow is the durable row of a capability that may carry no
// authority and names neither an owner nor an attempt.
const evidenceFreeSentinelRow = `{"schema":1,"unusable":true}`

// forEachLifecycleCapability visits every combination of lifecycleCapability's
// fields: each of backends, or none, as owner and as attempt, with or without
// each ID, principal, retirement, quarantine, raw corruption and pending
// persistence. The literal is exhaustruct-enforced, so a new field must be
// named here before lint passes. Lint cannot tell whether the field then
// ranges over its domain: give it a bit, as every field below has.
func forEachLifecycleCapability(t *testing.T, backends []string, visit func(lifecycleCapability)) {
	t.Helper()
	ownerID := requireLifecycleID(t, "502")
	attemptID := requireLifecycleID(t, "503")
	principal := runtimePrincipal{tenant: "tenant-test", providerUUID: freshTestProviderUUID}
	names := append([]string{""}, backends...)
	pickID := func(set bool, id lifecycle.ID) lifecycle.ID {
		if set {
			return id
		}
		return lifecycle.ID{}
	}
	for _, backendName := range names {
		for _, attemptBackend := range names {
			for bits := range 1 << 7 {
				set := func(bit int) bool { return bits&(1<<bit) != 0 }
				var owner runtimePrincipal
				if set(1) {
					owner = principal
				}
				visit(lifecycleCapability{ //exhaustruct:enforce
					backend:          backendName,
					id:               pickID(set(0), ownerID),
					principal:        owner,
					retired:          set(2),
					attemptBackend:   attemptBackend,
					attemptID:        pickID(set(3), attemptID),
					quarantined:      set(4),
					rawCorrupt:       set(5),
					needsPersistence: set(6),
				})
			}
		}
	}
}

// TestLifecycleCapabilityUsabilityIsDerivedFromEvidence pins ENG-1119's P2: no
// capability is both ownerless and usable, because usability is derived from
// evidence; usability never makes the encoder refuse; the row written decodes
// back to the same authority and evidence; and asWritten, the form the cache
// holds, is exactly what that row decodes to.
func TestLifecycleCapabilityUsabilityIsDerivedFromEvidence(t *testing.T) {
	shapes, ownerless, ownerlessWritten, written := 0, 0, 0, 0
	forEachLifecycleCapability(t, []string{"backend-a"}, func(capability lifecycleCapability) {
		shapes++
		evidence := capability.backend != "" || capability.attemptBackend != ""
		require.Equal(t, !capability.quarantined && !capability.rawCorrupt && evidence, capability.usable(),
			"%+v", capability)
		if !evidence {
			ownerless++
		}
		// The encoder's acceptance, stated independently of
		// validateLifecycleCapability: it refuses raw corruption and an
		// incomplete shape, and nothing else. Owner and attempt evidence is not
		// part of it, so an ownerless capability with a complete shape encodes.
		principal := capability.principal
		completeShape := !capability.rawCorrupt &&
			(principal == (runtimePrincipal{}) || (principal.tenant != "" && principal.providerUUID != "")) &&
			(capability.backend != "" || !capability.id.Valid()) &&
			(capability.backend != "" || !capability.retired) &&
			(capability.attemptBackend == "") == !capability.attemptID.Valid()
		if !capability.usable() {
			authorization := authorizeLifecycleCapability(capability, capability.id)
			assert.Equal(t, LifecycleVerdictUnusable, authorization.Verdict(), "%+v", capability)
			assert.Empty(t, authorization.Backend(), "%+v", capability)
			assert.False(t, maintenanceAuthorityAvailable(
				Placement{Backend: capability.backend, revision: 1}, capability, freshTestProviderUUID,
			), "%+v", capability)
		}

		row, err := encodeLifecycleCapability(capability)
		if !completeShape {
			require.Error(t, err, "%+v", capability)
			return
		}
		require.NoError(t, err, "a complete shape encodes, with or without an owner: %+v", capability)
		written++
		if !evidence {
			ownerlessWritten++
		}
		decoded, err := decodeLifecycleCapability(row)
		require.NoError(t, err, "every written row decodes: %+v", capability)
		assert.Equal(t, capability.asWritten(), decoded,
			"the cache's written form is what reopening decodes: %+v", capability)
		assert.Equal(t, capability.usable(), decoded.usable(), "%+v", capability)
		assert.Equal(t, !capability.usable(), decoded.quarantined,
			"the row records exactly whether it may carry authority: %+v", capability)
		assert.Equal(t, capability.backend, decoded.backend)
		assert.Equal(t, capability.id, decoded.id)
		assert.Equal(t, capability.principal, decoded.principal)
		assert.Equal(t, capability.retired, decoded.retired)
		assert.Equal(t, capability.attemptBackend, decoded.attemptBackend)
		assert.Equal(t, capability.attemptID, decoded.attemptID)
		if !evidence && capability.principal == (runtimePrincipal{}) {
			assert.JSONEq(t, evidenceFreeSentinelRow, string(row), "%+v", capability)
		}
		rewritten, err := encodeLifecycleCapability(decoded)
		require.NoError(t, err)
		assert.Equal(t, row, rewritten, "a decoded row re-encodes to the same bytes: %+v", capability)
	})
	require.Equal(t, 2*2*(1<<7), shapes, "owner and attempt each range over none and backend-a")
	require.NotZero(t, ownerless)
	require.NotZero(t, ownerlessWritten, "ownerless capabilities with a complete shape were enumerated")
	require.NotZero(t, written)
}

// TestLifecycleCacheIsWrittenOnlyInItsWrittenForm pins the choke point that
// keeps one in-memory form per durable row: after the store loads, no code in
// the package assigns a lifecycleCache entry except
// cacheWrittenLifecycleLocked, which stores asWritten.
func TestLifecycleCacheIsWrittenOnlyInItsWrittenForm(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fileSet := token.NewFileSet()
	var writers []string
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fileSet, name, nil, 0)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				assign, ok := node.(*ast.AssignStmt)
				if !ok {
					return true
				}
				for _, target := range assign.Lhs {
					index, ok := target.(*ast.IndexExpr)
					if !ok {
						continue
					}
					if selector, ok := index.X.(*ast.SelectorExpr); ok && selector.Sel.Name == "lifecycleCache" {
						writers = append(writers, fmt.Sprintf("%s (%s)",
							function.Name.Name, fileSet.Position(assign.Pos())))
					}
				}
				return true
			})
		}
	}
	require.Len(t, writers, 1, "lifecycleCache entry writers: %v", writers)
	assert.True(t, strings.HasPrefix(writers[0], "cacheWrittenLifecycleLocked "), writers[0])
}

// TestLifecycleCapabilityWithoutEvidenceIsTheQuarantineSentinel covers the
// three shapes the ENG-1119 audit named: the zero value a map miss returns,
// the "keep nothing" value of a placement deletion, and an attempt-only
// capability whose attempt was cleared. Each withholds authority and encodes
// as the evidence-free sentinel instead of failing the write that carries it.
func TestLifecycleCapabilityWithoutEvidenceIsTheQuarantineSentinel(t *testing.T) {
	operationID := requireOperationID(t, "504")
	attemptOnly := lifecycleCapability{
		attemptBackend: "backend-a", attemptID: lifecycleIDFromOperation(t, operationID),
	}
	require.True(t, attemptOnly.usable(), "a first attempt marker is usable evidence")
	deleted, keep := lifecycleAfterPlacementDelete(attemptOnly, Placement{Attempt: "backend-a"})
	require.False(t, keep, "a deletion keeps no capability without an owner")
	for name, capability := range map[string]lifecycleCapability{
		"zero value":                      {},
		"placement deletion keep-nothing": deleted,
		"cleared attempt-only capability": clearAttemptLifecycle(attemptOnly, "backend-a", operationID),
	} {
		t.Run(name, func(t *testing.T) {
			assert.False(t, capability.usable())
			assert.Equal(t, LifecycleVerdictUnusable,
				authorizeLifecycleCapability(capability, lifecycle.ID{}).Verdict())
			row, err := encodeLifecycleCapability(capability)
			require.NoError(t, err)
			assert.JSONEq(t, evidenceFreeSentinelRow, string(row))
			decoded, err := decodeLifecycleCapability(row)
			require.NoError(t, err)
			assert.Equal(t, quarantinedLifecycle(), decoded)
		})
	}

	t.Run("a row claiming authority without evidence is not one Fred wrote", func(t *testing.T) {
		_, err := decodeLifecycleCapability([]byte(`{"schema":1}`))
		require.ErrorContains(t, err, "no current owner or attempt",
			"loading treats it as raw corruption and keeps its bytes")
	})
}

// TestLifecycleWithAttemptKeepsAQuarantineButNotAMapMiss pins how a new
// attempt treats a map miss. A lease with neither a lifecycle row nor a
// confirmed owner starts from nothing, so its first attempt marker is usable;
// a confirmed owner without a row, and every row without usable evidence
// (including the decoded sentinel), stays quarantined beside a new attempt
// until that operation settles. Authorization reads a map miss as Missing,
// not as a quarantine, but grants nothing either way.
func TestLifecycleWithAttemptKeepsAQuarantineButNotAMapMiss(t *testing.T) {
	operationID := requireOperationID(t, "505")
	attemptID := lifecycleIDFromOperation(t, operationID)
	confirmed := Placement{Backend: "backend-a", revision: 1}
	owner := lifecycleCapability{backend: "backend-a", id: requireLifecycleID(t, "506")}
	evidenceFree := lifecycleCapability{}
	sentinel := quarantinedLifecycle()
	for _, test := range []struct {
		name       string
		capability *lifecycleCapability
		placement  *Placement
		want       lifecycleCapability
	}{
		{
			name: "no row and no owner",
			want: lifecycleCapability{attemptBackend: "backend-a", attemptID: attemptID},
		},
		{
			name: "confirmed owner without a row", placement: &confirmed,
			want: lifecycleCapability{quarantined: true, attemptBackend: "backend-a", attemptID: attemptID},
		},
		{
			name: "row without evidence", capability: &evidenceFree,
			want: lifecycleCapability{quarantined: true, attemptBackend: "backend-a", attemptID: attemptID},
		},
		{
			name: "decoded sentinel", capability: &sentinel,
			want: lifecycleCapability{quarantined: true, attemptBackend: "backend-a", attemptID: attemptID},
		},
		{
			name: "usable owner", capability: &owner, placement: &confirmed,
			want: lifecycleCapability{
				backend: "backend-a", id: owner.id, attemptBackend: "backend-a", attemptID: attemptID,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newTestStore(t)
			s.mu.Lock()
			defer s.mu.Unlock()
			const leaseUUID = "00000000-0000-4000-8000-000000000505"
			if test.capability != nil {
				s.lifecycleCache[leaseUUID] = *test.capability
			}
			if test.placement != nil {
				s.cache[leaseUUID] = *test.placement
			}
			got, err := s.lifecycleWithAttemptLocked(leaseUUID, "backend-a", operationID)
			require.NoError(t, err)
			assert.Equal(t, test.want, got)
			_, err = encodeLifecycleCapability(got)
			require.NoError(t, err)
		})
	}
}
