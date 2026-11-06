package backend

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClientBindsEveryOperationsCountersWhenBuilt pins that a client's request
// series exist before its first request: building the client binds every
// operation it can send, and recording only selects a bound series.
func TestClientBindsEveryOperationsCountersWhenBuilt(t *testing.T) {
	requests := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "bind_test_requests"},
		[]string{"backend", "operation", "status"})
	malformed := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "bind_test_malformed"},
		[]string{"backend", "operation"})
	client := newUnboundHTTPClientForTest(HTTPClientConfig{
		Name: "bind-test", BaseURL: "http://localhost",
		RequestsTotal: requests, MalformedErrorBodyTotal: malformed,
	})

	require.Len(t, client.requests, len(requestOperations))
	assert.Equal(t, 4*len(requestOperations), promtestutil.CollectAndCount(requests),
		"every operation's four statuses exist before any request")
	assert.Equal(t, len(requestOperations), promtestutil.CollectAndCount(malformed))

	client.recordMetrics(requestRestore, time.Now(), nil)
	assert.Equal(t, 1.0, promtestutil.ToFloat64(requests.WithLabelValues("bind-test", "restore", "success")))
	assert.Equal(t, 4*len(requestOperations), promtestutil.CollectAndCount(requests),
		"recording creates no series")
}

// TestRequestOperationsAreDeclaredOnce pins that declareRequestOperation is the
// only place a requestOperation is built, so requestOperations, which every
// client binds, holds every operation the client can record.
func TestRequestOperationsAreDeclaredOnce(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
	var findings []string
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			function, isFunction := decl.(*ast.FuncDecl)
			if isFunction && function.Recv == nil && function.Name.Name == "declareRequestOperation" {
				continue
			}
			ast.Inspect(decl, func(node ast.Node) bool {
				if literal, ok := node.(*ast.CompositeLit); ok {
					if identifier, ok := literal.Type.(*ast.Ident); ok && identifier.Name == "requestOperation" {
						findings = append(findings, fset.Position(literal.Pos()).String())
					}
				}
				return true
			})
		}
	}
	assert.Empty(t, findings, "a requestOperation built outside declareRequestOperation is missing from requestOperations")

	names := make(map[string]bool, len(requestOperations))
	for _, operation := range requestOperations {
		assert.NotEmpty(t, operation.name)
		assert.False(t, names[operation.name], "operation %s is declared once", operation)
		names[operation.name] = true
	}
}
