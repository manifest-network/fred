package docker

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestFailedLogMergeChargesCompleteOutputAndPreservesUTF8(t *testing.T) {
	live := strings.Repeat("x", maxTotalLogBytes)
	const unavailable = "<log unavailable>"
	for _, remaining := range []int{0, 1, 5, len(aggregateLogLimitMessage), len(aggregateLogLimitMessage) + 1, len(aggregateLogLimitMessage) + 4, 1024} {
		t.Run(fmt.Sprintf("remaining_%d", remaining), func(t *testing.T) {
			livePrefix := live[:maxTotalLogBytes-remaining-len(unavailable)]
			result := map[string]string{"web/0": livePrefix, "web/1": unavailable}
			failed := map[string]string{"web/0": strings.Repeat("💥", 100), "web/1": "second failed instance"}
			appendFailedLogsWithinBudget(result, failed)
			require.Equal(t, livePrefix, result["web/0"])
			require.Equal(t, unavailable, result["web/1"])
			total := 0
			for _, output := range result {
				total += len(output)
				require.True(t, utf8.ValidString(output))
			}
			require.LessOrEqual(t, total, maxTotalLogBytes, "truncation markers and existing placeholders consume the same response budget")
			if remaining == 0 {
				require.NotContains(t, result, "failed/web/0")
			} else if remaining <= len(aggregateLogLimitMessage) {
				require.Equal(t, aggregateLogLimitMessage[:remaining], result["failed/web/0"])
				require.NotContains(t, result, "failed/web/1")
			} else if remaining == 1024 {
				require.Equal(t, failed["web/0"], result["failed/web/0"])
				require.Equal(t, failed["web/1"], result["failed/web/1"])
			}
		})
	}
}
