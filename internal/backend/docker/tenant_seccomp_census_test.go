package docker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type censusContainer struct {
	id          string
	state       string // running, restarting, paused or exited
	securityOpt []string
	privileged  bool
	missing     bool // removed between list and inspect
	failing     bool // inspection fails
}

// newCensusDockerClient serves a daemon that lists containers and answers
// their inspections, through the real client and its SDK view.
func newCensusDockerClient(t *testing.T, backendName string, containers []censusContainer) *DockerClient {
	t.Helper()
	host := newTenantSeccompTestDaemon(t, func(w http.ResponseWriter, r *http.Request) bool {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/containers/json"):
			var query map[string]map[string]bool
			assert.NoError(t, json.Unmarshal([]byte(r.URL.Query().Get("filters")), &query))
			assert.True(t, query["label"][LabelManaged+"=true"], "the census lists only fred-managed containers")
			assert.True(t, query["label"][LabelBackendName+"="+backendName], "the census lists only this backend's containers")
			assert.Equal(t, "1", r.URL.Query().Get("all"), "the census lists stopped containers too")
			summaries := make([]map[string]any, 0, len(containers))
			for _, c := range containers {
				summaries = append(summaries, map[string]any{"Id": c.id, "State": c.state, "Labels": map[string]string{LabelManaged: "true", LabelInstanceIndex: "not-a-number"}})
			}
			_ = json.NewEncoder(w).Encode(summaries)
			return true
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/containers/") && strings.HasSuffix(r.URL.Path, "/json"):
			id := strings.TrimSuffix(r.URL.Path[strings.LastIndex(r.URL.Path, "/containers/")+len("/containers/"):], "/json")
			for _, c := range containers {
				if c.id != id {
					continue
				}
				switch {
				case c.missing:
					w.WriteHeader(http.StatusNotFound)
					_, _ = w.Write([]byte(`{"message":"No such container"}`))
				case c.failing:
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte(`{"message":"daemon failure"}`))
				default:
					_ = json.NewEncoder(w).Encode(map[string]any{
						"Id": c.id, "Name": "/" + c.id,
						"State": map[string]any{"Status": c.state, "Running": c.state == "running" || c.state == "paused" || c.state == "restarting",
							"Paused": c.state == "paused", "Restarting": c.state == "restarting"},
						"HostConfig": map[string]any{"SecurityOpt": c.securityOpt, "Privileged": c.privileged, "CapDrop": []string{"ALL"}},
						"Config":     map[string]any{"Labels": map[string]string{LabelManaged: "true", LabelInstanceIndex: "not-a-number"}},
					})
				}
				return true
			}
			return false
		}
		return false
	})
	docker, err := NewDockerClient(t.Context(), host, backendName)
	require.NoError(t, err)
	t.Cleanup(func() { _ = docker.Close() })
	return docker
}

func TestTenantSeccompCensusJudgesTheEffectiveProfile(t *testing.T) {
	valid := tenantSeccompSecurityOpt(t, "no-new-privileges:true")
	appendOption := func(option string) []string { return append(append([]string(nil), valid...), option) }
	for name, tc := range map[string]struct {
		containers []censusContainer
		want       int
	}{
		"inline profile":        {containers: []censusContainer{{id: "a", state: "running", securityOpt: valid}}, want: 0},
		"unconfined appended":   {containers: []censusContainer{{id: "a", state: "running", securityOpt: appendOption("seccomp:unconfined")}}, want: 1},
		"unconfined":            {containers: []censusContainer{{id: "a", state: "running", securityOpt: []string{"seccomp=unconfined"}}}, want: 1},
		"builtin":               {containers: []censusContainer{{id: "a", state: "running", securityOpt: []string{"seccomp=builtin"}}}, want: 1},
		"created before fred":   {containers: []censusContainer{{id: "a", state: "running", securityOpt: []string{"no-new-privileges:true"}}}, want: 1},
		"privileged":            {containers: []censusContainer{{id: "a", state: "running", securityOpt: valid, privileged: true}}, want: 1},
		"paused and restarting": {containers: []censusContainer{{id: "a", state: "paused"}, {id: "b", state: "restarting"}}, want: 2},
		"stopped or removed": {containers: []censusContainer{
			{id: "a", state: "exited"}, {id: "b", state: "running", missing: true}, {id: "c", state: "running", securityOpt: valid},
		}, want: 0},
	} {
		t.Run(name, func(t *testing.T) {
			docker := newCensusDockerClient(t, "census", tc.containers)
			census, err := docker.TenantSeccompCensus(t.Context())
			require.NoError(t, err)
			require.Equal(t, tc.want, census.withoutCurrent)
		})
	}

	docker := newCensusDockerClient(t, "census", []censusContainer{{id: "a", state: "running", securityOpt: valid}, {id: "b", state: "running", failing: true}})
	_, err := docker.TenantSeccompCensus(t.Context())
	require.Error(t, err, "an incomplete pass yields no count")
}

func TestTenantSeccompCensusPublishesOnlyCompletedPasses(t *testing.T) {
	var census tenantSeccompCensus
	var censusErr error
	mock := &mockDockerClient{TenantSeccompCensusFn: func(context.Context) (tenantSeccompCensus, error) { return census, censusErr }}
	b := newBackendForTest(mock, nil)
	t.Cleanup(b.stopCancel)
	ok := tenantSeccompCensusTotal.WithLabelValues(tenantSeccompCensusOK)
	failed := tenantSeccompCensusTotal.WithLabelValues(tenantSeccompCensusError)
	okBefore, failedBefore := testutil.ToFloat64(ok), testutil.ToFloat64(failed)

	census = tenantSeccompCensus{withoutCurrent: 3}
	b.runTenantSeccompCensus()
	require.Equal(t, 3.0, testutil.ToFloat64(tenantContainersWithoutCurrentSeccomp))
	require.Equal(t, okBefore+1, testutil.ToFloat64(ok))

	census, censusErr = tenantSeccompCensus{}, errors.New("daemon unreachable")
	b.runTenantSeccompCensus()
	require.Equal(t, 3.0, testutil.ToFloat64(tenantContainersWithoutCurrentSeccomp), "a failed pass keeps the last completed count")
	require.Equal(t, failedBefore+1, testutil.ToFloat64(failed))

	census, censusErr = tenantSeccompCensus{withoutCurrent: 0}, nil
	b.runTenantSeccompCensus()
	require.Equal(t, 0.0, testutil.ToFloat64(tenantContainersWithoutCurrentSeccomp))
	require.Equal(t, okBefore+2, testutil.ToFloat64(ok))

	// A pass cut short by shutdown is not an outcome.
	b.stopCancel()
	censusErr = context.Canceled
	b.runTenantSeccompCensus()
	require.Equal(t, failedBefore+1, testutil.ToFloat64(failed))
	require.Equal(t, okBefore+2, testutil.ToFloat64(ok))
}
