package imagefetch

import (
	"archive/tar"
	"errors"
	"io"
)

// A normalized tar header hides PAX/GNU extension records. Docker's tar-split
// reader retains their complete raw span in memory during Next. Bound that
// span separately from normalized metadata: a 128 KiB extension envelope plus
// four blocks for the file header, padding and terminator framing. This is an
// additional format limit; bounded normalized fields alone do not imply it.
const maxRawHeaderBytes = 2*maxHeaderBytes + 4*512

// layerArchive owns all transitions between payload and header parsing. Its
// parser and reader never escape; callers cannot reset the header allowance or
// accidentally count an undrained file as metadata. Copies share one cursor.
type layerArchive struct{ state *layerArchiveState }

type layerArchiveState struct {
	input   headerSpanReader
	parser  *tar.Reader
	payload int64
	err     error
}

func newLayerArchive(input io.Reader) layerArchive {
	state := &layerArchiveState{input: headerSpanReader{source: input}}
	state.parser = tar.NewReader(&state.input)
	return layerArchive{state: state}
}

func (a layerArchive) next() (*tar.Header, error) {
	s := a.state
	if s == nil {
		return nil, errors.New("image layer archive is unavailable")
	}
	if s.err != nil {
		return nil, s.err
	}
	// Only payload reads receive the unmetered phase. The enclosing decoded
	// byte budget still bounds them; tar.Reader restricts them to this entry.
	s.input.header = false
	n, err := io.Copy(io.Discard, s.parser)
	s.payload += n
	if err != nil {
		s.err = err
		return nil, err
	}
	s.input.header, s.input.remaining = true, maxRawHeaderBytes
	header, err := s.parser.Next()
	s.err = err
	return header, err
}

func (a layerArchive) payloadBytes() int64 { return a.state.payload }

type headerSpanReader struct {
	source    io.Reader
	header    bool
	remaining int
}

func (r *headerSpanReader) Read(p []byte) (int, error) {
	if !r.header {
		return r.source.Read(p)
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r.remaining == 0 {
		return 0, errors.New("image layer raw header exceeds metadata budget")
	}
	if len(p) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.source.Read(p)
	r.remaining -= n
	return n, err
}
