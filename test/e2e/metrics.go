//go:build integration

package e2e

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// MetricLines returns the sample lines of one metric from a Prometheus
// endpoint. Counters are how a test learns that a process looked at something
// and decided to leave it, or refused something before acting on it.
func MetricLines(t *testing.T, client *http.Client, url, name string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode, "GET %s", url)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var lines []string
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, name+" ") || strings.HasPrefix(line, name+"{") {
			lines = append(lines, line)
		}
	}
	return lines
}

// SumMetric adds up the samples whose labels include every given
// `name="value"` pair. No matching sample sums to 0.
func SumMetric(t *testing.T, lines []string, labels ...string) float64 {
	t.Helper()
	var sum float64
samples:
	for _, line := range lines {
		series, value, _ := strings.Cut(line, " ")
		for _, label := range labels {
			if !strings.Contains(series, label) {
				continue samples
			}
		}
		sample, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		require.NoError(t, err, "metric line %q", line)
		sum += sample
	}
	return sum
}

// Metric sums the samples of one of this backend's metrics whose labels
// include every given `name="value"` pair.
func (b *Backend) Metric(t *testing.T, name string, labels ...string) float64 {
	t.Helper()
	return SumMetric(t, MetricLines(t, b.Client.http, b.URL+"/metrics", name), labels...)
}

// metricOrZero is Metric for use where a failed scrape must not fail the
// test, such as inside a retried condition: it reads as 0.
func (b *Backend) metricOrZero(name string) float64 {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.URL+"/metrics", nil)
	if err != nil {
		return 0
	}
	resp, err := b.Client.http.Do(req)
	if err != nil {
		return 0
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		return 0
	}
	var sum float64
	for _, line := range strings.Split(string(body), "\n") {
		if series, value, found := strings.Cut(line, " "); found && (series == name || strings.HasPrefix(series, name+"{")) {
			if sample, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err == nil {
				sum += sample
			}
		}
	}
	return sum
}
