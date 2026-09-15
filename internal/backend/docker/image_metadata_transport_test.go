package docker

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
)

func TestImageInspectResponseBudgetPrecedesSDKDecodingAndAdmission(t *testing.T) {
	for _, extra := range []int{-1, 0, 1} {
		t.Run(fmt.Sprintf("limit%+d", extra), func(t *testing.T) {
			core := `{"Id":"` + testImageID + `","Os":"linux","Architecture":"amd64","Config":{}}`
			body := core + strings.Repeat(" ", imageexec.MaxInspectResponseBytes+extra-len(core))
			cli := newImageSecurityDockerClient(t, func(*http.Request) (*http.Response, error) {
				return imageSecurityResponse(http.StatusOK, body), nil
			})
			admitted, err := cli.AdmitImage(t.Context(), "fixture:latest")
			if extra > 0 {
				require.ErrorContains(t, err, "metadata byte budget")
				require.Empty(t, admitted.ID(), "overflow cannot mint helper or workload authority")
			} else {
				require.NoError(t, err)
				require.Equal(t, testImageID, admitted.ID())
			}
		})
	}
}
