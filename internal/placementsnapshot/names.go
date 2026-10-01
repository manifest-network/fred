package placementsnapshot

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
)

const (
	setFilePrefix  = "fred-snapshot-"
	tempFilePrefix = ".fred-snapshot-tmp-"
	nameTimeLayout = "20060102T150405Z"
)

// fileKind is one file of a snapshot set. The zero value is invalid.
type fileKind uint8

const (
	fileKindInvalid fileKind = iota
	fileKindPlacements
	fileKindPayloads
	fileKindManifest
)

// setFileKinds lists a set's files in deletion order: the manifest goes first,
// so a set never looks complete while its data is being removed.
var setFileKinds = [...]fileKind{fileKindManifest, fileKindPlacements, fileKindPayloads}

func (kind fileKind) suffix() string {
	switch kind {
	case fileKindPlacements:
		return "placements.db"
	case fileKindPayloads:
		return "payloads.db"
	case fileKindManifest:
		return "manifest.json"
	default:
		return ""
	}
}

func fileKindOf(suffix string) fileKind {
	for _, kind := range setFileKinds {
		if kind.suffix() == suffix {
			return kind
		}
	}
	return fileKindInvalid
}

// setName identifies one snapshot set. Only newSetName and namer.parse mint
// one; the zero value is invalid.
type setName struct {
	created time.Time
	id      string
}

func newSetName(at time.Time) (setName, error) {
	var random [4]byte
	if _, err := rand.Read(random[:]); err != nil {
		return setName{}, fmt.Errorf("generate snapshot set id: %w", err)
	}
	return setName{
		created: at.UTC().Truncate(time.Second),
		id:      hex.EncodeToString(random[:]),
	}, nil
}

func (name setName) valid() bool { return name.id != "" }

// newer orders sets by creation time, then by id.
func (name setName) newer(other setName) bool {
	if !name.created.Equal(other.created) {
		return name.created.After(other.created)
	}
	return name.id > other.id
}

// namer renders and recognizes one provider's snapshot file names. Nothing
// outside its two patterns is ever touched in the snapshot directory.
type namer struct {
	providerUUID string
	setFile      *regexp.Regexp
	tempFile     *regexp.Regexp
}

func newNamer(providerUUID string) (namer, error) {
	parsed, err := uuid.Parse(providerUUID)
	if err != nil || parsed.String() != providerUUID {
		return namer{}, errors.New("snapshot provider UUID must be a canonical lowercase UUID")
	}
	quoted := regexp.QuoteMeta(providerUUID)
	return namer{
		providerUUID: providerUUID,
		setFile: regexp.MustCompile(`^` + regexp.QuoteMeta(setFilePrefix) + quoted +
			`-([0-9]{8}T[0-9]{6}Z)-([0-9a-f]{8})\.(placements\.db|payloads\.db|manifest\.json)$`),
		tempFile: regexp.MustCompile(`^` + regexp.QuoteMeta(tempFilePrefix) + quoted + `-[0-9a-f]{32}$`),
	}, nil
}

func (names namer) file(set setName, kind fileKind) string {
	return setFilePrefix + names.providerUUID + "-" + set.created.Format(nameTimeLayout) +
		"-" + set.id + "." + kind.suffix()
}

// tempPrefix is the CreateTemp prefix of this provider's staged files; the
// random suffix CreateTemp appends completes the tempFile pattern.
func (names namer) tempPrefix() string {
	return tempFilePrefix + names.providerUUID + "-"
}

// parse returns the set and kind of one of this provider's set files.
func (names namer) parse(entry string) (setName, fileKind, bool) {
	match := names.setFile.FindStringSubmatch(entry)
	if match == nil {
		return setName{}, fileKindInvalid, false
	}
	created, err := time.Parse(nameTimeLayout, match[1])
	if err != nil {
		return setName{}, fileKindInvalid, false
	}
	kind := fileKindOf(match[3])
	if kind == fileKindInvalid {
		return setName{}, fileKindInvalid, false
	}
	return setName{created: created.UTC(), id: match[2]}, kind, true
}

func (names namer) isTemp(entry string) bool {
	return names.tempFile.MatchString(entry)
}
