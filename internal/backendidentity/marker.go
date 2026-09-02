package backendidentity

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unicode"
	"unicode/utf8"
)

const (
	// MarkerFilename is the conventional name for a marker stored directly in
	// a backend data root. Callers whose substrate is represented by another
	// file may instead choose a marker path adjacent to that file.
	MarkerFilename = ".fred-backend-storage-identity.json"

	markerSchemaVersion = 1
	maxMarkerBytes      = 64 << 10
	// Binding values come from external substrates (for example Docker's
	// SystemID). Bound them independently of the complete JSON document so an
	// attacker-controlled or corrupt identifier cannot force an oversized
	// allocation. encodeMarkerRecord applies the final, escape-aware document
	// bound before any directory entry is published.
	maxMarkerBindingValueBytes = 16 << 10
	maxMarkerPublicationLinks  = 1
)

var (
	// ErrInvalidMarker reports a marker whose file type or JSON representation
	// cannot be trusted as a storage identity.
	ErrInvalidMarker = errors.New("invalid backend storage identity marker")

	// ErrMarkerBindingMismatch reports a valid marker that belongs to a
	// different backend name or durable substrate.
	ErrMarkerBindingMismatch = errors.New("backend storage identity marker binding mismatch")

	// ErrInvalidMarkerBinding reports invalid binding arguments supplied while
	// loading or creating a marker.
	ErrInvalidMarkerBinding = errors.New("backend storage identity marker requires a path, backend name, and substrate ID")

	errMarkerAbsent = errors.New("backend storage identity marker absent")
)

type markerRecord struct {
	Schema                int    `json:"schema"`
	StorageID             string `json:"storage_id"`
	BackendName           string `json:"backend_name"`
	SubstrateID           string `json:"substrate_id"`
	State                 string `json:"state,omitempty"`
	InitializationProfile string `json:"initialization_profile,omitempty"`
}

const markerStatePending = "pending"

// MarkerPairStoreHooks joins identity-bound control-store publication to the
// marker pair's existing crash protocol. Prepare and Check receive an opaque
// PendingStorage only after the pending anchor is durable, so an arbitrary UUID
// cannot authorize store creation or binding. Verify is called only after both
// markers are committed and must be read-only: a committed lineage may never
// repair or replace missing state.
//
// Keeping the two capabilities separate prevents an ordinary restart (or an
// explicit initializer rerun after success) from accidentally turning durable
// state loss into a fresh empty journal.
type MarkerPairStoreHooks struct {
	Profile InitializationProfile
	Prepare func(PendingStorage, InitializationProfile) error
	Check   func(PendingStorage) error
	Verify  func(VerifiedStorage) error
}

// InitializationProfile records whether a pending seal may create genuinely
// fresh authoritative stores or must adopt a complete pre-existing v0.13 set.
// It lives only on the pending anchor and makes crash recovery independent of
// the partially-created file set visible after the crash.
type InitializationProfile string

const (
	InitializationProfileFresh    InitializationProfile = "fresh"
	InitializationProfileExisting InitializationProfile = "existing"
)

func (profile InitializationProfile) valid() bool {
	return profile == InitializationProfileFresh || profile == InitializationProfileExisting
}

// LoadVerifiedMarkerPair is the capability-returning form used by production
// constructors. Unlike a bare ID, the result cannot be created by parsing a
// caller-supplied UUID.
func LoadVerifiedMarkerPair(
	primaryPath, anchorPath, backendName, substrateID string,
) (VerifiedStorage, error) {
	id, err := LoadMarkerPair(primaryPath, anchorPath, backendName, substrateID)
	if err != nil {
		return VerifiedStorage{}, err
	}
	return VerifiedStorage{id: id}, nil
}

// LoadMarkerPair loads a previously sealed marker pair. It never creates,
// repairs, or finalizes either member. Requiring both committed copies is what
// makes ordinary process startup distinguish a new installation from lost or
// unmounted backend state.
func LoadMarkerPair(
	primaryPath, anchorPath, backendName, substrateID string,
) (ID, error) {
	if primaryPath == anchorPath || primaryPath == "" || anchorPath == "" {
		return ID{}, fmt.Errorf("%w: marker pair paths must be distinct", ErrInvalidMarkerBinding)
	}
	primaryID, primaryState, primaryErr := inspectMarkerStatePath(
		primaryPath, backendName, substrateID,
	)
	anchorID, anchorState, anchorErr := inspectMarkerStatePath(
		anchorPath, backendName, substrateID,
	)
	if primaryErr != nil {
		if errors.Is(primaryErr, errMarkerAbsent) {
			return ID{}, fmt.Errorf("%w: primary marker is missing; run explicit storage identity initialization", ErrInvalidMarker)
		}
		return ID{}, primaryErr
	}
	if anchorErr != nil {
		if errors.Is(anchorErr, errMarkerAbsent) {
			return ID{}, fmt.Errorf("%w: identity anchor is missing; run explicit storage identity initialization", ErrInvalidMarker)
		}
		return ID{}, anchorErr
	}
	if primaryState != "" || anchorState != "" {
		return ID{}, fmt.Errorf("%w: uncommitted marker pair requires explicit storage identity initialization", ErrInvalidMarker)
	}
	if primaryID != anchorID {
		return ID{}, fmt.Errorf("%w: marker pair identities differ (%s != %s)",
			ErrMarkerBindingMismatch, primaryID, anchorID)
	}
	if err := verifyDistinctMarkerFiles(primaryPath, anchorPath); err != nil {
		return ID{}, err
	}
	return primaryID, nil
}

// VerifyMarkerPair proves that both durable anchors still name the expected
// backend, substrate, and storage UUID. It never creates or repairs a file.
func VerifyMarkerPair(
	primaryPath, anchorPath, backendName, substrateID string, expected ID,
) error {
	if primaryPath == anchorPath || !expected.Valid() {
		return ErrInvalidMarkerBinding
	}
	primaryID, err := inspectMarkerPath(primaryPath, backendName, substrateID)
	if err != nil {
		if errors.Is(err, errMarkerAbsent) {
			return fmt.Errorf("%w: primary marker is missing", ErrInvalidMarker)
		}
		return err
	}
	anchorID, err := inspectMarkerPath(anchorPath, backendName, substrateID)
	if err != nil {
		if errors.Is(err, errMarkerAbsent) {
			return fmt.Errorf("%w: identity anchor is missing", ErrInvalidMarker)
		}
		return err
	}
	if primaryID != expected || anchorID != expected {
		return fmt.Errorf("%w: marker pair is (%s,%s), expected %s",
			ErrMarkerBindingMismatch, primaryID, anchorID, expected)
	}
	if err := verifyDistinctMarkerFiles(primaryPath, anchorPath); err != nil {
		return err
	}
	return nil
}

func verifyDistinctMarkerFiles(primaryPath, anchorPath string) error {
	primaryInfo, err := os.Lstat(primaryPath)
	if err != nil {
		return fmt.Errorf("stat primary backend storage identity marker: %w", err)
	}
	anchorInfo, err := os.Lstat(anchorPath)
	if err != nil {
		return fmt.Errorf("stat backend storage identity anchor: %w", err)
	}
	if err := validateMarkerFileMode(filepath.Base(primaryPath), primaryInfo.Mode()); err != nil {
		return err
	}
	if err := validateMarkerFileMode(filepath.Base(anchorPath), anchorInfo.Mode()); err != nil {
		return err
	}
	if os.SameFile(primaryInfo, anchorInfo) {
		return fmt.Errorf("%w: marker pair paths refer to the same file", ErrInvalidMarker)
	}
	for path, info := range map[string]fs.FileInfo{
		primaryPath: primaryInfo,
		anchorPath:  anchorInfo,
	} {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Nlink != 1 {
			return fmt.Errorf("%w: marker %q must have exactly one hard link", ErrInvalidMarker, path)
		}
	}
	return nil
}

func inspectMarkerPath(markerPath, backendName, substrateID string) (result ID, resultErr error) {
	id, state, err := inspectMarkerStatePath(markerPath, backendName, substrateID)
	if err != nil {
		return ID{}, err
	}
	if state != "" {
		return ID{}, fmt.Errorf("%w: marker is not committed", ErrInvalidMarker)
	}
	return id, nil
}

func inspectMarkerStatePath(
	markerPath, backendName, substrateID string,
) (result ID, state string, resultErr error) {
	if markerPath == "" || filepath.Clean(markerPath) != markerPath ||
		!validMarkerBindingValue(backendName) || !validMarkerBindingValue(substrateID) {
		return ID{}, "", ErrInvalidMarkerBinding
	}
	root, err := os.OpenRoot(filepath.Dir(markerPath))
	if err != nil {
		return ID{}, "", fmt.Errorf("open backend storage identity marker directory: %w", err)
	}
	defer func() {
		if err := root.Close(); resultErr == nil && err != nil {
			result = ID{}
			state = ""
			resultErr = fmt.Errorf("close backend storage identity marker directory: %w", err)
		}
	}()
	return loadMarkerState(root, filepath.Base(markerPath), backendName, substrateID)
}

func loadMarkerState(
	root *os.Root, markerName, backendName, substrateID string,
) (ID, string, error) {
	data, err := readRegularFile(root, markerName)
	if err != nil {
		return ID{}, "", err
	}
	record, err := decodeMarker(data)
	if err != nil {
		return ID{}, "", err
	}
	return validateMarkerRecord(record, backendName, substrateID)
}

func readRegularFile(root *os.Root, markerName string) ([]byte, error) {
	pathInfo, err := root.Lstat(markerName)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, errMarkerAbsent
		}
		return nil, err
	}
	if err := validateMarkerFileMode(markerName, pathInfo.Mode()); err != nil {
		return nil, err
	}

	file, err := root.Open(markerName)
	if err != nil {
		return nil, fmt.Errorf("%w: marker changed while opening: %w", ErrInvalidMarker, err)
	}
	openedInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("stat open backend storage identity marker: %w", err)
	}
	if validateMarkerFileMode(markerName, openedInfo.Mode()) != nil ||
		!os.SameFile(pathInfo, openedInfo) {
		_ = file.Close()
		return nil, fmt.Errorf("%w: marker changed while opening", ErrInvalidMarker)
	}

	data, readErr := io.ReadAll(io.LimitReader(file, maxMarkerBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read backend storage identity marker: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close backend storage identity marker: %w", closeErr)
	}
	if len(data) > maxMarkerBytes {
		return nil, fmt.Errorf("%w: marker exceeds %d bytes", ErrInvalidMarker, maxMarkerBytes)
	}

	currentInfo, err := root.Lstat(markerName)
	if err != nil {
		return nil, fmt.Errorf("%w: marker changed while reading: %w", ErrInvalidMarker, err)
	}
	if validateMarkerFileMode(markerName, currentInfo.Mode()) != nil ||
		!os.SameFile(openedInfo, currentInfo) {
		return nil, fmt.Errorf("%w: marker changed while reading", ErrInvalidMarker)
	}
	return data, nil
}

func validMarkerBindingValue(value string) bool {
	if len(value) > maxMarkerBindingValueBytes || !utf8.ValidString(value) ||
		strings.TrimSpace(value) == "" {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func encodeMarkerRecord(record markerRecord) ([]byte, error) {
	data, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("encode backend storage identity marker: %w", err)
	}
	if len(data) > maxMarkerBytes {
		return nil, fmt.Errorf(
			"%w: encoded backend storage identity marker exceeds %d bytes",
			ErrInvalidMarkerBinding,
			maxMarkerBytes,
		)
	}
	return data, nil
}

func validateMarkerFileMode(markerName string, mode fs.FileMode) error {
	if !mode.IsRegular() {
		return fmt.Errorf("%w: %q is not a regular file", ErrInvalidMarker, markerName)
	}
	if mode.Perm()&0o022 != 0 {
		return fmt.Errorf("%w: %q is group- or world-writable", ErrInvalidMarker, markerName)
	}
	return nil
}

func decodeMarker(data []byte) (markerRecord, error) {
	if !utf8.Valid(data) {
		return markerRecord{}, fmt.Errorf("%w: JSON is not valid UTF-8", ErrInvalidMarker)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	opening, err := decoder.Token()
	if err != nil {
		return markerRecord{}, fmt.Errorf("%w: decode JSON: %w", ErrInvalidMarker, err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return markerRecord{}, fmt.Errorf("%w: JSON root must be an object", ErrInvalidMarker)
	}

	fields := make(map[string]json.RawMessage, 6)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return markerRecord{}, fmt.Errorf("%w: decode field name: %w", ErrInvalidMarker, err)
		}
		key, ok := keyToken.(string)
		if !ok {
			return markerRecord{}, fmt.Errorf("%w: non-string field name", ErrInvalidMarker)
		}
		if _, duplicate := fields[key]; duplicate {
			return markerRecord{}, fmt.Errorf("%w: duplicate field %q", ErrInvalidMarker, key)
		}
		switch key {
		case "schema", "storage_id", "backend_name", "substrate_id", "state", "initialization_profile":
		default:
			return markerRecord{}, fmt.Errorf("%w: unknown field %q", ErrInvalidMarker, key)
		}

		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return markerRecord{}, fmt.Errorf("%w: decode field %q: %w", ErrInvalidMarker, key, err)
		}
		fields[key] = raw
	}
	closing, err := decoder.Token()
	if err != nil {
		return markerRecord{}, fmt.Errorf("%w: close JSON object: %w", ErrInvalidMarker, err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return markerRecord{}, fmt.Errorf("%w: malformed JSON object", ErrInvalidMarker)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return markerRecord{}, fmt.Errorf("%w: trailing JSON value", ErrInvalidMarker)
		}
		return markerRecord{}, fmt.Errorf("%w: trailing data: %w", ErrInvalidMarker, err)
	}

	for _, required := range []string{"schema", "storage_id", "backend_name", "substrate_id"} {
		if _, present := fields[required]; !present {
			return markerRecord{}, fmt.Errorf("%w: missing field %q", ErrInvalidMarker, required)
		}
	}

	var record markerRecord
	if err := decodeJSONInteger(fields["schema"], &record.Schema); err != nil {
		return markerRecord{}, fmt.Errorf("%w: schema: %w", ErrInvalidMarker, err)
	}
	if err := decodeJSONString(fields["storage_id"], &record.StorageID); err != nil {
		return markerRecord{}, fmt.Errorf("%w: storage_id: %w", ErrInvalidMarker, err)
	}
	if err := decodeJSONString(fields["backend_name"], &record.BackendName); err != nil {
		return markerRecord{}, fmt.Errorf("%w: backend_name: %w", ErrInvalidMarker, err)
	}
	if err := decodeJSONString(fields["substrate_id"], &record.SubstrateID); err != nil {
		return markerRecord{}, fmt.Errorf("%w: substrate_id: %w", ErrInvalidMarker, err)
	}
	if raw, present := fields["state"]; present {
		if err := decodeJSONString(raw, &record.State); err != nil {
			return markerRecord{}, fmt.Errorf("%w: state: %w", ErrInvalidMarker, err)
		}
	}
	if raw, present := fields["initialization_profile"]; present {
		if err := decodeJSONString(raw, &record.InitializationProfile); err != nil {
			return markerRecord{}, fmt.Errorf("%w: initialization_profile: %w", ErrInvalidMarker, err)
		}
	}
	return record, nil
}

func decodeJSONInteger(raw json.RawMessage, destination *int) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return errors.New("must be an integer")
	}
	if err := json.Unmarshal(trimmed, destination); err != nil {
		return errors.New("must be an integer")
	}
	return nil
}

func decodeJSONString(raw json.RawMessage, destination *string) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '"' {
		return errors.New("must be a string")
	}
	if err := json.Unmarshal(trimmed, destination); err != nil {
		return errors.New("must be a string")
	}
	return nil
}
