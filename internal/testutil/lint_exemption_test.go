package testutil

// This file pins the per-file forbidigo exemptions of .golangci.yml
// (ENG-1117, ENG-799, ENG-1125). An exemption names the rules it lifts by a bracketed tag that
// starts their messages, and golangci-lint matches an exclusion's text against
// the whole issue text. Keyed on prose instead, a new rule that reused the
// phrase would silently be exempt too; keyed on the wrong tag, or widened to
// more files, a guarded call would stop being refused where it must be.
// This test fails on either drift: every tag is carried by exactly its rules,
// every exemption lifts exactly one tag in exactly its files, and nothing else
// outside test files lifts forbidigo.
//
// That the rules fire at all is proven against the pinned linter when a rule
// changes (inject a violation, see it reported, revert); this test pins the
// configuration that decides where they fire.

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type golangciForbidRule struct {
	Pattern string `yaml:"pattern"`
	Msg     string `yaml:"msg"`
}

type golangciExclusion struct {
	Path    string   `yaml:"path"`
	Linters []string `yaml:"linters"`
	Text    string   `yaml:"text"`
}

type golangciConfig struct {
	Linters struct {
		Settings struct {
			Forbidigo struct {
				Forbid []golangciForbidRule `yaml:"forbid"`
			} `yaml:"forbidigo"`
		} `yaml:"settings"`
		Exclusions struct {
			Rules []golangciExclusion `yaml:"rules"`
		} `yaml:"exclusions"`
	} `yaml:"linters"`
}

// forbidigoExemptions is the reviewed set: each tag, the rules that carry it,
// and the only files where it is lifted.
var forbidigoExemptions = map[string]struct {
	patterns []string
	files    []string
}{
	"[composition-writer]": {
		patterns: []string{
			`\.Destroy$`, `volumes\.Create$`, `\.RetryHeldVolumeDelete$`, `\.DeferDeletesUntilExecutorRuns$`,
		},
		files: []string{"internal/backend/docker/storage_mutation_guard.go"},
	},
	"[tree-removal-wiring]": {
		patterns: []string{`fstree\.RemoveBeneath$`},
		files: []string{
			"internal/backend/docker/storage_mutation_guard.go",
			"internal/backend/docker/volume_xfs.go",
		},
	},
	// The on-chain lease close hops (ENG-799).
	"[raw-lease-close]": {
		patterns: []string{`\.closeActiveLeaseOnChain$`, `\.CloseObserved$`},
		files:    []string{"internal/provisioner/reconcile_close.go"},
	},
	"[placement-lease-close]": {
		patterns: []string{`\.closeLease$`},
		files:    []string{"internal/provisioner/placement/reconciliation_chain.go"},
	},
	"[chain-lease-close]": {
		patterns: []string{`\.CloseLeases$`},
		files: []string{
			"internal/provisioner/manager.go",
			"internal/provisioner/placement/provider_control_plane.go",
			"internal/scheduler/withdraw.go",
		},
	},
	// Startup failure evidence (ENG-1125).
	"[startup-evidence]": {
		patterns: []string{`\.NewOperationStartupFailed$`},
		files:    []string{"internal/backend/docker/startup_failure.go"},
	},
}

// forbidigoIssueText renders an issue the way golangci-lint's forbidigo
// reports it, which is the text an exclusion's regex is matched against.
func forbidigoIssueText(use, msg string) string {
	return "use of `" + use + "` forbidden because \"" + msg + "\""
}

func loadGolangciConfig(t *testing.T) golangciConfig {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repoRoot(t), ".golangci.yml"))
	if err != nil {
		t.Fatalf("read .golangci.yml: %v", err)
	}
	var cfg golangciConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("parse .golangci.yml: %v", err)
	}
	return cfg
}

// productionGoFiles lists every non-test Go file in the repository, relative
// to its root with forward slashes, as golangci-lint matches paths.
func productionGoFiles(t *testing.T) []string {
	t.Helper()
	root := repoRoot(t)
	var files []string
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, relErr := filepath.Rel(root, path)
			if relErr != nil {
				return relErr
			}
			files = append(files, filepath.ToSlash(rel))
			return nil
		})
		if err != nil {
			t.Fatalf("list Go files under %s: %v", dir, err)
		}
	}
	return files
}

func TestForbidigoExemptionsAreKeyedOnTheirRulesTags(t *testing.T) {
	cfg := loadGolangciConfig(t)
	rules := cfg.Linters.Settings.Forbidigo.Forbid
	if len(rules) == 0 {
		t.Fatal("no forbidigo rules found: the config shape changed")
	}

	// Every tag is carried by exactly its reviewed rules, at the start of the
	// message, and by no other rule.
	for tag, want := range forbidigoExemptions {
		var carriers []string
		for _, rule := range rules {
			if strings.Contains(rule.Msg, tag) {
				if !strings.HasPrefix(rule.Msg, tag+" ") {
					t.Errorf("rule %q carries %s other than as its message's prefix", rule.Pattern, tag)
				}
				carriers = append(carriers, rule.Pattern)
			}
		}
		slices.Sort(carriers)
		wantPatterns := slices.Sorted(slices.Values(want.patterns))
		if !slices.Equal(carriers, wantPatterns) {
			t.Errorf("rules tagged %s = %q, want %q", tag, carriers, wantPatterns)
		}
	}

	files := productionGoFiles(t)
	exemptions := 0
	for _, exclusion := range cfg.Linters.Exclusions.Rules {
		if !slices.Contains(exclusion.Linters, "forbidigo") {
			continue
		}
		if exclusion.Path == `_test\.go` {
			continue // test files are outside the rules' scope
		}
		exemptions++
		if !slices.Equal(exclusion.Linters, []string{"forbidigo"}) {
			t.Errorf("exclusion for %q lifts %q with forbidigo; keep a tag exemption to forbidigo alone",
				exclusion.Path, exclusion.Linters)
		}
		text, err := regexp.Compile(exclusion.Text)
		if err != nil || exclusion.Text == "" {
			t.Errorf("exclusion for %q has no usable text: %q (%v)", exclusion.Path, exclusion.Text, err)
			continue
		}
		// The exclusion lifts exactly the rules of one tag.
		var lifted []string
		for _, rule := range rules {
			if text.MatchString(forbidigoIssueText("x.Destroy", rule.Msg)) {
				lifted = append(lifted, rule.Pattern)
			}
		}
		slices.Sort(lifted)
		var tag string
		for candidate, want := range forbidigoExemptions {
			if slices.Equal(lifted, slices.Sorted(slices.Values(want.patterns))) {
				tag = candidate
			}
		}
		if tag == "" {
			t.Errorf("exclusion for %q (text %q) lifts %q, which is no reviewed tag's rule set",
				exclusion.Path, exclusion.Text, lifted)
			continue
		}
		// Keyed on the tag itself: prose that lifts the same rules today
		// would also lift any later rule that reused it.
		if exclusion.Text != regexp.QuoteMeta(tag) {
			t.Errorf("exclusion for %q is keyed on %q, want exactly the tag %q",
				exclusion.Path, exclusion.Text, regexp.QuoteMeta(tag))
		}
		// It applies in exactly that tag's files.
		path, err := regexp.Compile(exclusion.Path)
		if err != nil {
			t.Errorf("exclusion path %q: %v", exclusion.Path, err)
			continue
		}
		var matched []string
		for _, file := range files {
			if path.MatchString(file) {
				matched = append(matched, file)
			}
		}
		wantFiles := slices.Sorted(slices.Values(forbidigoExemptions[tag].files))
		slices.Sort(matched)
		if !slices.Equal(matched, wantFiles) {
			t.Errorf("exclusion of %s applies to %q, want %q", tag, matched, wantFiles)
		}
	}
	if exemptions != len(forbidigoExemptions) {
		t.Errorf("found %d non-test forbidigo exclusions, want one per reviewed tag (%d)",
			exemptions, len(forbidigoExemptions))
	}
}

// The tag matching above must itself be able to fail: a prose-keyed
// exclusion, or a tag regex that also matches an untagged rule, is caught.
func TestForbidigoExemptionCheckRejectsProseKeys(t *testing.T) {
	writer := "[composition-writer] managed-volume destruction must use the construction-bound substrate mutation executor"
	removal := "recursive removal must use internal/fstree.RemoveBeneath, which bounds descriptors, depth and work"
	prose := regexp.MustCompile("construction-bound substrate mutation executor")
	reuse := "a future rule that reuses the construction-bound substrate mutation executor phrase"
	if !prose.MatchString(forbidigoIssueText("x", writer)) || !prose.MatchString(forbidigoIssueText("x", reuse)) {
		t.Fatal("positive control: a prose key matches the writer rule and also a reuse of its phrase")
	}
	tag := regexp.MustCompile(`\[composition-writer\]`)
	if !tag.MatchString(forbidigoIssueText("x", writer)) {
		t.Fatal("the tag key must match its own rule")
	}
	for _, other := range []string{reuse, removal} {
		if tag.MatchString(forbidigoIssueText("x", other)) {
			t.Errorf("the tag key must not match an untagged rule: %q", other)
		}
	}
}
