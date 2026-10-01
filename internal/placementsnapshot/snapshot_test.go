package placementsnapshot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
)

const (
	otherProvider = "6f1d1f1e-3c1a-4d55-9c43-1f7d6a2e9b01"
	storedLease   = "00000000-0000-4000-8000-0000000000b1"
)

type fixture struct {
	placements *placement.Store
	payloads   *payload.Store
	live       LiveDatabases
	dir        string
	snapshots  *Directory
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	liveDir := t.TempDir()
	live := LiveDatabases{
		Placements: filepath.Join(liveDir, "placements.db"),
		Payloads:   filepath.Join(liveDir, "payloads.db"),
	}
	placements, err := placementstore.NewStore(live.Placements)
	require.NoError(t, err)
	t.Cleanup(func() { _ = placements.Close() })
	payloads, err := payload.NewStore(payload.StoreConfig{DBPath: live.Payloads})
	require.NoError(t, err)
	t.Cleanup(func() { _ = payloads.Close() })
	require.True(t, payloads.Store(storedLease, []byte("payload")))

	dir := filepath.Join(t.TempDir(), "snapshots")
	require.NoError(t, os.Mkdir(dir, 0o700))
	snapshots, err := OpenDirectory(dir, placementstore.ProviderUUID, live)
	require.NoError(t, err)
	t.Cleanup(func() { _ = snapshots.Close() })
	return &fixture{placements: placements, payloads: payloads, live: live, dir: dir, snapshots: snapshots}
}

func (f *fixture) capture(t *testing.T, deadline time.Duration) placement.ConsistentCut {
	t.Helper()
	cut, err := f.placements.CaptureConsistentCut(f.payloads, deadline)
	require.NoError(t, err)
	return cut
}

// publishAt publishes one set created at the given time.
func (f *fixture) publishAt(t *testing.T, at time.Time) setName {
	t.Helper()
	set, err := f.snapshots.publish(t.Context(), f.capture(t, time.Minute), at)
	require.NoError(t, err)
	return set
}

func (f *fixture) entries(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(f.dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func (f *fixture) setFiles(set setName) []string {
	return []string{
		f.snapshots.names.file(set, fileKindManifest),
		f.snapshots.names.file(set, fileKindPlacements),
		f.snapshots.names.file(set, fileKindPayloads),
	}
}

func fileSHA256(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

var (
	baseTime = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	// pruneTime is a day after every fixture set, so none is dated ahead of it.
	pruneTime = baseTime.Add(24 * time.Hour)
)

func TestNamesRoundTripAndIgnoreEveryOtherName(t *testing.T) {
	names, err := newNamer(placementstore.ProviderUUID)
	require.NoError(t, err)
	set, err := newSetName(baseTime.Add(1500 * time.Millisecond))
	require.NoError(t, err)
	assert.Equal(t, baseTime.Add(time.Second), set.created, "names have whole-second UTC times")
	for _, kind := range setFileKinds {
		parsed, parsedKind, ok := names.parse(names.file(set, kind))
		require.True(t, ok, kind.suffix())
		assert.Equal(t, set, parsed)
		assert.Equal(t, kind, parsedKind)
	}

	others, err := newNamer(otherProvider)
	require.NoError(t, err)
	for _, foreign := range []string{
		others.file(set, fileKindManifest),
		names.file(set, fileKindManifest) + ".bak",
		strings.ToUpper(names.file(set, fileKindPlacements)),
		"fred-snapshot-" + placementstore.ProviderUUID + "-20261001T120000Z-0123456g.payloads.db",
		"placements.db",
	} {
		_, _, ok := names.parse(foreign)
		assert.False(t, ok, foreign)
	}
	assert.True(t, names.isTemp(names.tempPrefix()+strings.Repeat("a", 32)))
	assert.False(t, names.isTemp(others.tempPrefix()+strings.Repeat("a", 32)))
	assert.False(t, names.isTemp(names.tempPrefix()+"short"))

	_, err = newNamer(strings.ToUpper(placementstore.ProviderUUID))
	assert.Error(t, err)
	later := setName{created: set.created, id: "ffffffff"}
	assert.True(t, later.newer(setName{created: set.created, id: "00000000"}))
	assert.True(t, setName{created: set.created.Add(time.Second), id: "00000000"}.newer(later))
}

func TestOpenDirectoryRefusesAnUnsafeDirectory(t *testing.T) {
	f := newFixture(t)
	open := func(path string) error {
		snapshots, err := OpenDirectory(path, placementstore.ProviderUUID, f.live)
		if err == nil {
			require.NoError(t, snapshots.Close())
		}
		return err
	}
	for _, mode := range []os.FileMode{0o770, 0o702, 0o777} {
		dir := filepath.Join(t.TempDir(), "snapshots")
		require.NoError(t, os.Mkdir(dir, 0o700))
		require.NoError(t, os.Chmod(dir, mode))
		assert.ErrorContains(t, open(dir), "not writable by group or others", mode.String())
	}
	assert.ErrorContains(t, open(filepath.Dir(f.live.Placements)), "live database's directory")

	link := filepath.Join(t.TempDir(), "link")
	require.NoError(t, os.Symlink(f.dir, link))
	assert.Error(t, open(link), "a symlinked directory is refused")
	assert.Error(t, open("relative/snapshots"))

	_, err := OpenDirectory(f.dir, strings.ToUpper(placementstore.ProviderUUID), f.live)
	assert.Error(t, err)
	missing := f.live
	missing.Payloads = filepath.Join(t.TempDir(), "absent.db")
	_, err = OpenDirectory(f.dir, placementstore.ProviderUUID, missing)
	assert.Error(t, err)
}

func TestPublishWritesAVerifiedPrivateSet(t *testing.T) {
	f := newFixture(t)
	set := f.publishAt(t, baseTime)

	assert.ElementsMatch(t, f.setFiles(set), f.entries(t), "no staged file is left behind")
	for _, name := range f.setFiles(set) {
		info, err := os.Lstat(filepath.Join(f.dir, name))
		require.NoError(t, err)
		assert.True(t, f.snapshots.private(info), "%s is a private single-link file", name)
	}
	raw, err := os.ReadFile(filepath.Join(f.dir, f.snapshots.names.file(set, fileKindManifest)))
	require.NoError(t, err)
	decoded, err := f.snapshots.names.decodeManifest(set, raw)
	require.NoError(t, err)
	for kind, described := range map[fileKind]manifestFile{
		fileKindPlacements: decoded.Placements,
		fileKindPayloads:   decoded.Payloads,
	} {
		path := filepath.Join(f.dir, described.Name)
		assert.Equal(t, f.snapshots.names.file(set, kind), described.Name)
		assert.Equal(t, described.SHA256, fileSHA256(t, path))
		info, err := os.Stat(path)
		require.NoError(t, err)
		assert.Equal(t, described.Size, info.Size())
	}

	db, err := bolt.Open(filepath.Join(f.dir, decoded.Payloads.Name), 0o600,
		&bolt.Options{ReadOnly: true, Timeout: time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("payloads"))
		require.NotNil(t, bucket)
		assert.NotNil(t, bucket.Get([]byte(storedLease)), "the payload copy holds the stored payload")
		return nil
	}))

	complete, doubt := f.snapshots.classify(set)
	assert.True(t, complete)
	assert.Equal(t, pruneFailureInvalid, doubt)
}

func TestPublishNeverReplacesAnExistingFileAndLeavesNoManifest(t *testing.T) {
	f := newFixture(t)
	set, err := newSetName(baseTime)
	require.NoError(t, err)
	occupied := filepath.Join(f.dir, f.snapshots.names.file(set, fileKindPayloads))
	require.NoError(t, os.WriteFile(occupied, []byte("operator data"), 0o600))

	err = f.snapshots.publishAs(t.Context(), f.capture(t, time.Minute), set)
	require.ErrorContains(t, err, "already exists")
	kept, err := os.ReadFile(occupied)
	require.NoError(t, err)
	assert.Equal(t, "operator data", string(kept))
	assert.Equal(t, []string{filepath.Base(occupied)}, f.entries(t),
		"the attempt removed its own published placements file and its staged files")
}

func TestAnAttemptWhoseCopyFailsLeavesNothingBehind(t *testing.T) {
	f := newFixture(t)
	cut := f.capture(t, 20*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	_, err := f.snapshots.publish(t.Context(), cut, baseTime)
	require.ErrorContains(t, err, "already used or expired")
	assert.Empty(t, f.entries(t))
}

func TestVerifyCopyAcceptsOnlyTheStreamedBytesInAPrivateFile(t *testing.T) {
	f := newFixture(t)
	var copied bytes.Buffer
	receipt, err := f.capture(t, time.Minute).Stream(t.Context(), &copied, io.Discard)
	require.NoError(t, err)
	want := receipt.Placements()
	stage := func(name string, content []byte, mode os.FileMode) string {
		path := filepath.Join(f.dir, name)
		require.NoError(t, os.WriteFile(path, content, 0o600))
		require.NoError(t, os.Chmod(path, mode))
		return name
	}

	good := stage("good", copied.Bytes(), 0o600)
	_, err = f.snapshots.verifyCopy(t.Context(), good, want)
	require.NoError(t, err)

	require.NoError(t, os.Link(filepath.Join(f.dir, good), filepath.Join(f.dir, "second-link")))
	_, err = f.snapshots.verifyCopy(t.Context(), good, want)
	assert.ErrorContains(t, err, "not a private regular file", "a second link is refused")

	_, err = f.snapshots.verifyCopy(t.Context(), stage("readable", copied.Bytes(), 0o644), want)
	assert.ErrorContains(t, err, "not a private regular file")

	_, err = f.snapshots.verifyCopy(t.Context(), stage("short", copied.Bytes()[:copied.Len()-1], 0o600), want)
	assert.ErrorContains(t, err, "the stream copied")

	flipped := bytes.Clone(copied.Bytes())
	flipped[len(flipped)-1] ^= 0xff
	_, err = f.snapshots.verifyCopy(t.Context(), stage("flipped", flipped, 0o600), want)
	assert.ErrorContains(t, err, "does not match the streamed digest")

	garbage := bytes.Repeat([]byte{0xab}, int(want.Size))
	_, err = f.snapshots.verifyCopy(t.Context(), stage("garbage", garbage, 0o600),
		placement.CutCopy{Size: want.Size, SHA256: sha256.Sum256(garbage)})
	assert.Error(t, err, "bytes matching their digest must still be a consistent bbolt database")
}

func TestPruneKeepsTheNewestSetsAndTheSetItJustPublished(t *testing.T) {
	f := newFixture(t)
	var sets []setName
	for i := range 5 {
		sets = append(sets, f.publishAt(t, baseTime.Add(time.Duration(i)*time.Hour)))
	}
	report := f.snapshots.prune(sets[0], 2, pruneTime)
	assert.Empty(t, report.failures)
	assert.Equal(t, 6, report.removed)
	var want []string
	for _, set := range []setName{sets[0], sets[3], sets[4]} {
		want = append(want, f.setFiles(set)...)
	}
	assert.ElementsMatch(t, want, f.entries(t))

	newest, ok := f.snapshots.newestComplete()
	require.True(t, ok)
	assert.Equal(t, sets[4], newest)
}

func TestPruneTouchesOnlyWhatItCanProveIsItsOwn(t *testing.T) {
	f := newFixture(t)
	old := f.publishAt(t, baseTime)
	staleIncomplete := f.publishAt(t, baseTime.Add(time.Hour))
	newest := f.publishAt(t, baseTime.Add(2*time.Hour))
	freshIncomplete := f.publishAt(t, baseTime.Add(3*time.Hour))
	path := func(name string) string { return filepath.Join(f.dir, name) }
	for _, set := range []setName{staleIncomplete, freshIncomplete} {
		require.NoError(t, os.Remove(path(f.snapshots.names.file(set, fileKindManifest))))
	}

	others, err := newNamer(otherProvider)
	require.NoError(t, err)
	foreign := []string{others.file(old, fileKindManifest), "notes.txt", others.tempPrefix() + strings.Repeat("0", 32)}
	for _, name := range foreign {
		require.NoError(t, os.WriteFile(path(name), []byte("x"), 0o600))
	}
	ownTemp := f.snapshots.names.tempPrefix() + strings.Repeat("1", 32)
	require.NoError(t, os.WriteFile(path(ownTemp), []byte("crashed attempt"), 0o600))

	// Two old incomplete sets whose placements names are a symlink and a hard
	// link to a live database. Neither name may be unlinked, and the set's
	// payloads file outlives the refusal.
	symlinkedSet, err := newSetName(baseTime.Add(-time.Hour))
	require.NoError(t, err)
	symlinked := f.snapshots.names.file(symlinkedSet, fileKindPlacements)
	require.NoError(t, os.Symlink(f.live.Payloads, path(symlinked)))
	hardLinkedSet, err := newSetName(baseTime.Add(-2 * time.Hour))
	require.NoError(t, err)
	hardLinked := f.snapshots.names.file(hardLinkedSet, fileKindPlacements)
	require.NoError(t, os.Link(f.live.Placements, path(hardLinked)))
	behindRefusal := f.snapshots.names.file(hardLinkedSet, fileKindPayloads)
	require.NoError(t, os.WriteFile(path(behindRefusal), []byte("x"), 0o600))

	leftovers := f.snapshots.removeLeftoverStaging()
	assert.Equal(t, 1, leftovers.removed)
	report := f.snapshots.prune(setName{}, 1, pruneTime)
	assert.Equal(t, map[pruneFailure]int{pruneFailureLiveDatabase: 1, pruneFailureNotOwned: 1}, report.failures)
	entries := f.entries(t)
	for _, name := range foreign {
		assert.Contains(t, entries, name, "foreign files are never touched")
	}
	assert.NotContains(t, entries, ownTemp, "a crashed attempt's staged file is removed")
	for _, name := range f.setFiles(old) {
		assert.NotContains(t, entries, name, "an older complete set beyond retention is removed")
	}
	for _, name := range f.setFiles(staleIncomplete)[1:] {
		assert.NotContains(t, entries, name, "an incomplete set older than the newest complete one is removed")
	}
	for _, name := range append(f.setFiles(newest), f.setFiles(freshIncomplete)[1:]...) {
		assert.Contains(t, entries, name)
	}
	assert.Contains(t, entries, symlinked)
	assert.Contains(t, entries, hardLinked)
	assert.Contains(t, entries, behindRefusal, "a refused data file stops the rest of its set's deletion")

	report = f.snapshots.prune(setName{}, 1, pruneTime)
	assert.Equal(t, map[pruneFailure]int{pruneFailureLiveDatabase: 1, pruneFailureNotOwned: 1}, report.failures)
	for _, live := range []string{f.live.Placements, f.live.Payloads} {
		info, err := os.Lstat(live)
		require.NoError(t, err)
		assert.True(t, info.Mode().IsRegular())
	}
}

func TestPruneKeepsASetWhoseManifestItCannotRead(t *testing.T) {
	f := newFixture(t)
	old := f.publishAt(t, baseTime)
	f.publishAt(t, baseTime.Add(time.Hour))
	manifest := filepath.Join(f.dir, f.snapshots.names.file(old, fileKindManifest))
	require.NoError(t, os.Chmod(manifest, 0o000))
	t.Cleanup(func() { _ = os.Chmod(manifest, 0o600) })
	if raw, err := os.ReadFile(manifest); err == nil && len(raw) > 0 {
		t.Skip("running with privileges that read a mode-0000 file")
	}

	complete, doubt := f.snapshots.classify(old)
	assert.False(t, complete)
	assert.Equal(t, pruneFailureInspect, doubt)
	report := f.snapshots.prune(setName{}, 1, pruneTime)
	assert.Equal(t, map[pruneFailure]int{pruneFailureInspect: 1}, report.failures)
	for _, name := range f.setFiles(old) {
		assert.Contains(t, f.entries(t), name, "doubt keeps the whole set")
	}
}

func TestRemovingASetStopsWhenItsManifestCannotBeRemoved(t *testing.T) {
	f := newFixture(t)
	set := f.publishAt(t, baseTime)
	manifest := filepath.Join(f.dir, f.snapshots.names.file(set, fileKindManifest))
	require.NoError(t, os.Remove(manifest))
	require.NoError(t, os.Mkdir(manifest, 0o700))

	var report pruneReport
	f.snapshots.removeSet(set, &report)
	assert.Equal(t, 1, report.failures[pruneFailureNotOwned])
	assert.ElementsMatch(t, f.setFiles(set), f.entries(t), "data outlives a manifest that is still there")
}

func TestClassifyNeverTrustsAMalformedOrSpecialManifest(t *testing.T) {
	f := newFixture(t)
	set := f.publishAt(t, baseTime)
	manifest := filepath.Join(f.dir, f.snapshots.names.file(set, fileKindManifest))

	require.NoError(t, os.WriteFile(manifest, []byte(`{"schema":"other"}`), 0o600))
	complete, doubt := f.snapshots.classify(set)
	assert.False(t, complete, "a malformed manifest makes the set incomplete")
	assert.Equal(t, pruneFailureInvalid, doubt)

	require.NoError(t, os.Remove(manifest))
	require.NoError(t, syscall.Mkfifo(manifest, 0o600))
	complete, doubt = f.snapshots.classify(set)
	assert.False(t, complete, "a FIFO is never read as a manifest")
	assert.Equal(t, pruneFailureInvalid, doubt)
}

func TestASetWhoseDataDisagreesWithItsManifestIsIncomplete(t *testing.T) {
	f := newFixture(t)
	set := f.publishAt(t, baseTime)
	payloads := filepath.Join(f.dir, f.snapshots.names.file(set, fileKindPayloads))
	info, err := os.Stat(payloads)
	require.NoError(t, err)
	require.NoError(t, os.Truncate(payloads, info.Size()-1))
	complete, doubt := f.snapshots.classify(set)
	assert.False(t, complete, "a data file shorter than its manifest says")
	assert.Equal(t, pruneFailureInvalid, doubt)

	require.NoError(t, os.Remove(payloads))
	complete, doubt = f.snapshots.classify(set)
	assert.False(t, complete, "a missing data file")
	assert.Equal(t, pruneFailureInvalid, doubt)
}

func TestManifestDecodingIsStrict(t *testing.T) {
	f := newFixture(t)
	set := f.publishAt(t, baseTime)
	raw, err := os.ReadFile(filepath.Join(f.dir, f.snapshots.names.file(set, fileKindManifest)))
	require.NoError(t, err)
	_, err = f.snapshots.names.decodeManifest(set, raw)
	require.NoError(t, err)

	for name, mutate := range map[string]func(string) string{
		"unknown field":  func(s string) string { return strings.Replace(s, `"schema"`, `"extra": 1, "schema"`, 1) },
		"trailing value": func(s string) string { return s + "{}" },
		"other provider": func(s string) string { return strings.ReplaceAll(s, placementstore.ProviderUUID, otherProvider) },
		"upper digest":   func(s string) string { return strings.Replace(s, `"sha256": "`, `"sha256": "A`, 1) },
		"other schema":   func(s string) string { return strings.Replace(s, manifestSchema, "fred-placement-snapshot/v2", 1) },
		"other creation time": func(s string) string {
			return strings.Replace(s, set.created.Format(time.RFC3339), set.created.Add(time.Second).Format(time.RFC3339), 1)
		},
		"empty data file": func(s string) string { return strings.Replace(s, `"size": `, `"size": -`, 1) },
	} {
		_, err := f.snapshots.names.decodeManifest(set, []byte(mutate(string(raw))))
		assert.Error(t, err, name)
	}
	other := setName{created: set.created.Add(time.Second), id: set.id}
	_, err = f.snapshots.names.decodeManifest(other, raw)
	assert.Error(t, err, "a manifest describes only the set it is named for")
}

func TestFirstSnapshotWaitsOneIntervalSinceTheNewestSet(t *testing.T) {
	interval := time.Hour
	now := baseTime
	assert.Equal(t, 40*time.Minute, firstSnapshotDelay(now, now.Add(-20*time.Minute), interval))
	assert.Equal(t, startupDelay, firstSnapshotDelay(now, now.Add(-3*time.Hour), interval))
	assert.Equal(t, startupDelay, firstSnapshotDelay(now, now.Add(-interval+time.Second), interval))
	assert.Equal(t, interval, firstSnapshotDelay(now, now.Add(24*time.Hour), interval),
		"a set from the future cannot postpone snapshots past one interval")
	assert.Equal(t, startupDelay, firstSnapshotDelay(now, now, 10*time.Second))
}

func TestFreeSpaceCheckRequiresTwiceTheCopyAndAReserve(t *testing.T) {
	assert.Equal(t, uint64(2_000+spaceReserve), requiredBytes(1_000))
	assert.True(t, hasRoom(2_000+spaceReserve, 1_000))
	assert.False(t, hasRoom(2_000+spaceReserve-1, 1_000))
	assert.Equal(t, uint64(spaceReserve), requiredBytes(-5))
}

func TestEachAttemptCountsExactlyOneOutcome(t *testing.T) {
	f := newFixture(t)
	settings, err := NewSettings(time.Hour, 3)
	require.NoError(t, err)
	service, err := NewService(f.placements, f.payloads, f.snapshots, settings)
	require.NoError(t, err)
	counter := func(result outcome) float64 {
		return testutil.ToFloat64(metrics.PlacementSnapshotsTotal.WithLabelValues(result.label()))
	}
	before := map[outcome]float64{}
	for _, result := range outcomes {
		before[result] = counter(result)
	}

	result, report := service.snapshotOnce(t.Context(), time.Now())
	require.Equal(t, outcomeSuccess, result)
	record(result, report)
	assert.Len(t, f.entries(t), 3)

	require.NoError(t, f.payloads.Close())
	result, report = service.snapshotOnce(t.Context(), time.Now())
	assert.Equal(t, outcomeError, result)
	record(result, report)

	assert.InDelta(t, before[outcomeSuccess]+1, counter(outcomeSuccess), 0)
	assert.InDelta(t, before[outcomeError]+1, counter(outcomeError), 0)
	assert.InDelta(t, before[outcomeInsufficientSpace], counter(outcomeInsufficientSpace), 0)
}

func TestRunStopsWhenItsContextEnds(t *testing.T) {
	f := newFixture(t)
	settings, err := NewSettings(time.Hour, 1)
	require.NoError(t, err)
	service, err := NewService(f.placements, f.payloads, f.snapshots, settings)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after its context ended")
	}
}

func TestSettingsAndServiceRefuseInvalidInputs(t *testing.T) {
	_, err := NewSettings(0, 1)
	assert.Error(t, err)
	_, err = NewSettings(time.Hour, 0)
	assert.Error(t, err)
	f := newFixture(t)
	settings, err := NewSettings(time.Hour, 1)
	require.NoError(t, err)
	_, err = NewService(nil, f.payloads, f.snapshots, settings)
	assert.Error(t, err)
	_, err = NewService(f.placements, f.payloads, &Directory{}, settings)
	assert.Error(t, err)
	_, err = NewService(f.placements, f.payloads, f.snapshots, Settings{})
	assert.Error(t, err)
}

func TestOutcomesAndPruneFailuresAreClosed(t *testing.T) {
	seen := map[string]bool{}
	for _, result := range outcomes {
		label := result.label()
		assert.NotEmpty(t, label)
		assert.False(t, seen[label])
		seen[label] = true
	}
	assert.Empty(t, outcomeInvalid.label())
	assert.Empty(t, outcome(99).label())
	for _, failure := range pruneFailures {
		label := failure.label()
		assert.NotEmpty(t, label)
		assert.False(t, seen[label])
		seen[label] = true
	}
	assert.Empty(t, pruneFailureInvalid.label())
	assert.Empty(t, pruneFailure(99).label())
	assert.Empty(t, fileKindInvalid.suffix())
	assert.True(t, slices.Contains(setFileKinds[:], fileKindManifest))
	assert.Equal(t, fileKindManifest, setFileKinds[0], "a set's manifest is deleted first")
}

// TestOnlyTheirConstructorsMintSetNamesAndVerifiedSets keeps the minting
// claims in this package's comments true: a non-empty literal of either type
// appears only in its constructor.
func TestOnlyTheirConstructorsMintSetNamesAndVerifiedSets(t *testing.T) {
	allowed := map[string]map[string]bool{
		"setName":     {"newSetName": true, "parse": true},
		"verifiedSet": {"verifyStaged": true},
	}
	for _, violation := range nonEmptyLiteralsOutside(t, ".", allowed) {
		t.Error(violation)
	}
}

func TestTheSnapshotPackageNeverWritesThroughAPath(t *testing.T) {
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return walkErr
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, call := range []string{"os.Remove(", "os.RemoveAll(", "os.Rename(", "os.OpenFile(", "os.Create(", "os.WriteFile("} {
			assert.NotContains(t, string(raw), call, "%s must operate through the retained directory", path)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestPruneKeepsSetsDatedAheadOfTheClockWithoutLettingThemDisplaceCurrentOnes(t *testing.T) {
	f := newFixture(t)
	older := f.publishAt(t, baseTime)
	current := f.publishAt(t, baseTime.Add(time.Hour))
	ahead := f.publishAt(t, baseTime.Add(48*time.Hour))
	now := baseTime.Add(2 * time.Hour)

	report := f.snapshots.prune(setName{}, 1, now)
	assert.Empty(t, report.failures)
	entries := f.entries(t)
	for _, name := range append(f.setFiles(current), f.setFiles(ahead)...) {
		assert.Contains(t, entries, name, "the current set and the set dated ahead are both kept")
	}
	for _, name := range f.setFiles(older) {
		assert.NotContains(t, entries, name)
	}
}

func TestEachAttemptFirstRemovesLeftoverStaging(t *testing.T) {
	f := newFixture(t)
	settings, err := NewSettings(time.Hour, 3)
	require.NoError(t, err)
	service, err := NewService(f.placements, f.payloads, f.snapshots, settings)
	require.NoError(t, err)
	own := f.snapshots.names.tempPrefix() + strings.Repeat("2", 32)
	others, err := newNamer(otherProvider)
	require.NoError(t, err)
	foreign := others.tempPrefix() + strings.Repeat("3", 32)
	for _, name := range []string{own, foreign} {
		require.NoError(t, os.WriteFile(filepath.Join(f.dir, name), []byte("crashed attempt"), 0o600))
	}

	result, report := service.snapshotOnce(t.Context(), pruneTime)
	require.Equal(t, outcomeSuccess, result)
	assert.Equal(t, 1, report.removed)
	entries := f.entries(t)
	assert.NotContains(t, entries, own)
	assert.Contains(t, entries, foreign, "another provider's staged file is never touched")
}

func TestACanceledAttemptLeavesNothingBehind(t *testing.T) {
	f := newFixture(t)
	cut := f.capture(t, time.Minute)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := f.snapshots.publish(ctx, cut, baseTime)
	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, f.entries(t))
}

func TestAbandonKeepsOlderFilesWhenAPublishedFileCannotBeRemoved(t *testing.T) {
	f := newFixture(t)
	attempt := &publication{snapshots: f.snapshots, open: make(map[string]*os.File)}
	stage := func(name string) stagedFile {
		path := filepath.Join(f.dir, name)
		require.NoError(t, os.WriteFile(path, []byte(name), 0o600))
		info, err := os.Lstat(path)
		require.NoError(t, err)
		return stagedFile{temp: name, info: info}
	}
	data := stage("data")
	manifest := stage("manifest")
	attempt.published = []stagedFile{data, manifest}
	// The manifest's name now holds another inode, so abandon must not remove
	// it, and must then keep the data it describes.
	require.NoError(t, os.Remove(filepath.Join(f.dir, "manifest")))
	require.NoError(t, os.WriteFile(filepath.Join(f.dir, "manifest"), []byte("replaced"), 0o600))

	assert.ErrorContains(t, attempt.abandon(), "was replaced")
	assert.ElementsMatch(t, []string{"data", "manifest"}, f.entries(t))
}

func TestOpenDirectoryResolvesASymlinkedLiveDatabaseDirectory(t *testing.T) {
	real := t.TempDir()
	link := filepath.Join(t.TempDir(), "fred")
	require.NoError(t, os.Symlink(real, link))
	for _, name := range []string{"placements.db", "payloads.db"} {
		require.NoError(t, os.WriteFile(filepath.Join(real, name), nil, 0o600))
	}
	live := LiveDatabases{
		Placements: filepath.Join(link, "placements.db"),
		Payloads:   filepath.Join(link, "payloads.db"),
	}
	dir := filepath.Join(t.TempDir(), "snapshots")
	require.NoError(t, os.Mkdir(dir, 0o700))
	snapshots, err := OpenDirectory(dir, placementstore.ProviderUUID, live)
	require.NoError(t, err, "a symlinked live database directory is followed")
	require.NoError(t, snapshots.Close())

	_, err = OpenDirectory(real, placementstore.ProviderUUID, live)
	assert.ErrorContains(t, err, "live database's directory",
		"the directory behind the symlink is still the live database's directory")
}
