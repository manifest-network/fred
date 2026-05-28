package docker

// defaultTestSKUProfiles returns the four standard tier profiles that
// DefaultConfig used to ship before ENG-238. Tests that previously relied
// on DefaultConfig pre-populating SKUProfiles use this helper to keep the
// same shape locally without re-introducing the production-side trap
// (yaml.v3 map merge against a pre-populated default).
func defaultTestSKUProfiles() map[string]SKUProfile {
	return map[string]SKUProfile{
		"docker-micro": {
			CPUCores: 0.25,
			MemoryMB: 256,
			DiskMB:   512,
		},
		"docker-small": {
			CPUCores: 0.5,
			MemoryMB: 512,
			DiskMB:   1024,
		},
		"docker-medium": {
			CPUCores: 1.0,
			MemoryMB: 1024,
			DiskMB:   2048,
		},
		"docker-large": {
			CPUCores: 2.0,
			MemoryMB: 2048,
			DiskMB:   4096,
		},
	}
}
