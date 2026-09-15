package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"

	"github.com/manifest-network/fred/internal/configyaml"
	"github.com/manifest-network/fred/internal/strictjson"
)

// readConfigFile validates YAML and JSON before Viper's lossy key casefold.
// Defaults and environment overrides remain Viper's responsibility. Other
// previously supported formats retain Viper's decoder and UnmarshalExact.
func readConfigFile(v *viper.Viper, path string) error {
	v.SetConfigFile(path)
	format := strings.ToLower(filepath.Ext(path))
	if format != ".yaml" && format != ".yml" && format != ".json" {
		return v.ReadInConfig()
	}
	data, err := os.ReadFile(path) // #nosec G304 -- explicit operator-selected configuration path
	if err != nil {
		return err
	}
	var values map[string]any
	if format == ".json" {
		err = strictjson.DecodeObject(data, len(data), &values)
	} else {
		err = configyaml.Decode(data, &values)
	}
	if err != nil {
		return err
	}
	if err := validateUnambiguousConfigKeys(values); err != nil {
		return err
	}
	return v.MergeConfigMap(values)
}

func validateUnambiguousConfigKeys(value any) error {
	switch value := value.(type) {
	case map[string]any:
		seen := make(map[string]string, len(value))
		for key, nested := range value {
			folded := strings.ToLower(key) // Must match Viper's normalization.
			if previous, exists := seen[folded]; exists {
				return fmt.Errorf("case-insensitive configuration keys %q and %q conflict", previous, key)
			}
			seen[folded] = key
			if err := validateUnambiguousConfigKeys(nested); err != nil {
				return err
			}
		}
	case []any:
		for _, nested := range value {
			if err := validateUnambiguousConfigKeys(nested); err != nil {
				return err
			}
		}
	}
	return nil
}
