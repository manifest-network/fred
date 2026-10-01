package placementsnapshot

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/manifest-network/fred/internal/provisioner/placement"
)

const (
	manifestSchema   = "fred-placement-snapshot/v1"
	maxManifestBytes = 4096
)

// manifest is the last file of a set: it names the set's data files with the
// size and SHA-256 an operator verifies before restoring them.
type manifest struct {
	Schema       string       `json:"schema"`
	ProviderUUID string       `json:"provider_uuid"`
	CreatedAt    string       `json:"created_at"`
	Placements   manifestFile `json:"placements"`
	Payloads     manifestFile `json:"payloads"`
}

type manifestFile struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// newManifest describes a verified set. Taking a verifiedSet means a manifest
// can only ever describe bytes a stream copied and a re-read confirmed.
func (names namer) newManifest(set setName, verified verifiedSet) manifest {
	describe := func(kind fileKind, copied placement.CutCopy) manifestFile {
		return manifestFile{
			Name:   names.file(set, kind),
			Size:   copied.Size,
			SHA256: hex.EncodeToString(copied.SHA256[:]),
		}
	}
	return manifest{
		Schema:       manifestSchema,
		ProviderUUID: verified.receipt.ProviderUUID(),
		CreatedAt:    set.created.Format(time.RFC3339),
		Placements:   describe(fileKindPlacements, verified.receipt.Placements()),
		Payloads:     describe(fileKindPayloads, verified.receipt.Payloads()),
	}
}

func (m manifest) encode() ([]byte, error) {
	encoded, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	encoded = append(encoded, '\n')
	if len(encoded) > maxManifestBytes {
		return nil, fmt.Errorf("snapshot manifest is %d bytes, over the %d byte cap", len(encoded), maxManifestBytes)
	}
	return encoded, nil
}

// decodeManifest strictly decodes set's manifest: one JSON object, no unknown
// fields, and every field exactly what publication would have written for set.
func (names namer) decodeManifest(set setName, raw []byte) (manifest, error) {
	if len(raw) > maxManifestBytes {
		return manifest{}, errors.New("snapshot manifest exceeds its size cap")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var decoded manifest
	if err := decoder.Decode(&decoded); err != nil {
		return manifest{}, fmt.Errorf("decode snapshot manifest: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return manifest{}, errors.New("snapshot manifest has trailing data")
	}
	switch {
	case decoded.Schema != manifestSchema:
		return manifest{}, fmt.Errorf("snapshot manifest schema %q is not %q", decoded.Schema, manifestSchema)
	case decoded.ProviderUUID != names.providerUUID:
		return manifest{}, errors.New("snapshot manifest names another provider")
	case decoded.CreatedAt != set.created.Format(time.RFC3339):
		return manifest{}, errors.New("snapshot manifest creation time does not match its name")
	}
	for kind, file := range map[fileKind]manifestFile{
		fileKindPlacements: decoded.Placements,
		fileKindPayloads:   decoded.Payloads,
	} {
		digest, err := hex.DecodeString(file.SHA256)
		switch {
		case file.Name != names.file(set, kind):
			return manifest{}, fmt.Errorf("snapshot manifest names %q for its %s file", file.Name, kind.suffix())
		case file.Size <= 0:
			return manifest{}, fmt.Errorf("snapshot manifest %s size is not positive", kind.suffix())
		case err != nil || len(digest) != 32 || hex.EncodeToString(digest) != file.SHA256:
			return manifest{}, fmt.Errorf("snapshot manifest %s digest is not lowercase SHA-256 hex", kind.suffix())
		}
	}
	return decoded, nil
}

// size is the manifest's recorded size of one data file.
func (m manifest) size(kind fileKind) int64 {
	switch kind {
	case fileKindPlacements:
		return m.Placements.Size
	case fileKindPayloads:
		return m.Payloads.Size
	default:
		return -1
	}
}
