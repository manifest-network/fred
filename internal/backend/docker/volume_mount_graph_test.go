package docker

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProtectedVolumeMountTrieMatchesAncestorRule(t *testing.T) {
	type site struct {
		path, service string
		write         bool
	}
	paths := []string{".", "data", "data/child", "data/child/deep", "data-other", "other/child"}
	services := []string{"first", "second", "third"}
	random := rand.New(rand.NewPCG(240, 946))
	for range 500 {
		var graph launchMountNode
		var sites []site
		for range 1 + random.IntN(30) {
			mount := site{path: paths[random.IntN(len(paths))], service: services[random.IntN(len(services))], write: random.IntN(2) == 1}
			sites = append(sites, mount)
			graph.add(mount.path, mount.service, mount.write)
		}
		conflict := false
		for _, writer := range sites {
			for _, pending := range sites {
				conflict = conflict || (writer.write && writer.service != pending.service && writer.path != pending.path && pathContains(writer.path, pending.path))
			}
		}
		require.Equal(t, conflict, graph.validate(launchMountWriters{}) != nil, "mounts: %+v", sites)
	}
}

func TestProtectedVolumeMountTrieLargeDisjointGraph(t *testing.T) {
	var graph launchMountNode
	for index := range 10000 {
		// A large image-declared mount set should be proportional to its path
		// length. Equal sources across services do not introduce ancestor edges.
		path := "data/" + string(rune(0x1000+index))
		graph.add(path, "first", true)
		graph.add(path, "second", true)
	}
	require.NoError(t, graph.validate(launchMountWriters{}))
	graph.add("data", "third", true)
	require.ErrorContains(t, graph.validate(launchMountWriters{}), "pending bind source")
}
