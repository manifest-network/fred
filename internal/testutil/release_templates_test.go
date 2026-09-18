package testutil

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"text/template"

	"github.com/distribution/reference"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestReleaseImageTemplatesDoNotPublishPrereleaseAliases(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", ".goreleaser.yaml"))
	require.NoError(t, err)
	var config struct {
		Dockers []struct {
			ImageTemplates []string `yaml:"image_templates"`
		} `yaml:"dockers"`
	}
	require.NoError(t, yaml.Unmarshal(data, &config))
	require.Len(t, config.Dockers, 1)
	for _, prerelease := range []string{"", "rc.1"} {
		t.Run("prerelease="+prerelease, func(t *testing.T) {
			version := "1.2.3"
			if prerelease != "" {
				version += "-" + prerelease
			}
			var images []string
			for _, source := range config.Dockers[0].ImageTemplates {
				tmpl, err := template.New("image").Parse(source)
				require.NoError(t, err)
				var rendered bytes.Buffer
				require.NoError(t, tmpl.Execute(&rendered, map[string]any{
					"Version": version, "Prerelease": prerelease, "Major": 1, "Minor": 2,
				}))
				if rendered.Len() == 0 { // GoReleaser omits empty image templates.
					continue
				}
				_, err = reference.ParseNormalizedNamed(rendered.String())
				require.NoError(t, err, "release image tag must remain valid")
				images = append(images, rendered.String())
			}
			want := []string{"ghcr.io/manifest-network/fred:" + version}
			if prerelease == "" {
				want = append(want, "ghcr.io/manifest-network/fred:latest",
					"ghcr.io/manifest-network/fred:1.2", "ghcr.io/manifest-network/fred:1")
			}
			require.Equal(t, want, images)
		})
	}
}
