package imageexec

import (
	"errors"
	"slices"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

var (
	ErrUnavailable    = errors.New("image execution capability is unavailable")
	ErrInvalidImage   = errors.New("image was not admitted")
	ErrForeignImage   = errors.New("image belongs to another admitter")
	ErrInvalidProject = errors.New("compose project was not prepared")
	ErrForeignProject = errors.New("compose project belongs to another admitter")
)

// issuer binds admission and execution to the source captured at construction.
// It is deliberately nonzero-sized so distinct issuers have distinct addresses.
type issuer struct {
	source Source
}

type imageRecord struct {
	issuer    *issuer
	reference string
	id        string
	platform  ocispec.Platform
	user      string
	volumes   []string
}

// Image is an admitted, independently addressable immutable image. Only Admit
// can mint a usable Image; its zero value grants no execution capability.
type Image struct {
	record *imageRecord
}

// ID returns the immutable execution ID, or an empty string for a zero Image.
func (i Image) ID() string {
	if i.record == nil {
		return ""
	}
	return i.record.id
}

// Reference returns the original reference supplied to Admit.
func (i Image) Reference() string {
	if i.record == nil {
		return ""
	}
	return i.record.reference
}

// Platform returns a copy of the admitted image's runnable platform.
func (i Image) Platform() ocispec.Platform {
	if i.record == nil {
		return ocispec.Platform{}
	}
	return clonePlatform(i.record.platform)
}

// User returns the image's USER directive.
func (i Image) User() string {
	if i.record == nil {
		return ""
	}
	return i.record.user
}

// Volumes returns a sorted copy of the image's VOLUME declarations.
func (i Image) Volumes() []string {
	if i.record == nil {
		return nil
	}
	return slices.Clone(i.record.volumes)
}

func (i Image) requireIssuer(owner *issuer) error {
	if i.record == nil {
		return ErrInvalidImage
	}
	if i.record.issuer != owner {
		return ErrForeignImage
	}
	return nil
}

func clonePlatform(p ocispec.Platform) ocispec.Platform {
	p.OSFeatures = slices.Clone(p.OSFeatures)
	return p
}
