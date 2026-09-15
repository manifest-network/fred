package backend

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"
)

const (
	// MaxLogContentBytes is the aggregate content budget for one log retrieval,
	// shared by live container reads and retained failed-attempt diagnostics.
	MaxLogContentBytes = 32 << 20

	// Failed-attempt records bound both entry count and key size before storage.
	MaxFailureDiagnosticEntries  = 4096
	MaxFailureDiagnosticKeyBytes = 256

	// AggregateLogLimitMessage also bounds the per-entry placeholder overhead
	// left by the live API after it exhausts the content budget.
	AggregateLogLimitMessage = "[log truncated: aggregate log size limit reached]"

	// A response may merge the current runtime and one failed-attempt record.
	// Live keys are DNS service labels (63 bytes), '/' and an instance number;
	// the larger diagnostic key bound covers both, including the failed/ prefix.
	maxProjectedLogEntries  = MaxOperationQuantity + MaxFailureDiagnosticEntries
	maxProjectedLogKeyBytes = MaxFailureDiagnosticKeyBytes + len("failed/")

	// JSON can expand each input byte to six bytes (control characters, HTML
	// escaping or invalid UTF-8). Include keys, per-entry markers, object framing
	// and the encoder's trailing newline, not just the unescaped content budget.
	MaxProjectedLogsResponseBytes = 6*(MaxLogContentBytes+maxProjectedLogEntries*(maxProjectedLogKeyBytes+len("\n")+len(AggregateLogLimitMessage))) +
		6*maxProjectedLogEntries + 3
)

// LogResponse owns a bounded snapshot of the projected live and failed-attempt
// entries. Its encoder writes small string fragments: encoding/json's ordinary
// Marshal and Encoder both allocate the entire escaped document before writing.
type LogResponse struct {
	logs map[string]string
}

// NewLogResponse takes an immutable snapshot without copying string contents.
func NewLogResponse(logs map[string]string) (LogResponse, error) {
	if logs == nil {
		return LogResponse{}, nil
	}
	b := logResponseBuilder{logs: make(map[string]string, min(len(logs), maxProjectedLogEntries))}
	for key, value := range logs {
		if err := b.add(key, value); err != nil {
			return LogResponse{}, err
		}
	}
	return LogResponse{logs: b.logs}, nil
}

type logResponseBuilder struct {
	logs         map[string]string
	contentBytes int
}

func (b *logResponseBuilder) add(key, value string) error {
	if len(b.logs) >= maxProjectedLogEntries || len(key) > maxProjectedLogKeyBytes {
		return ErrResponseTooLarge
	}
	if _, exists := b.logs[key]; exists {
		return errors.New("duplicate log response key")
	}
	// JSON repairs each invalid UTF-8 source byte to U+FFFD (three bytes).
	// Charge that rune as one source byte so the wire boundary preserves the
	// Docker capture budget even for binary logs. The decoded representation
	// remains bounded by three times this charge, plus map/key overhead.
	cost := len(value) - 2*strings.Count(value, "\ufffd")
	const contentLimit = MaxLogContentBytes + maxProjectedLogEntries*(len("\n")+len(AggregateLogLimitMessage))
	if cost > contentLimit-b.contentBytes {
		return ErrResponseTooLarge
	}
	b.contentBytes += cost
	b.logs[key] = value
	return nil
}

// decodeLogResponse visits one member at a time. In particular it never keeps
// io.ReadAll's escaped document alongside the decoder and the resulting map.
// The wire cap includes whitespace, malformed values and the EOF check.
func decodeLogResponse(r io.ReadCloser, limit int64) (_ *map[string]string, err error) {
	lr := &io.LimitedReader{R: r, N: limit + 1}
	defer func() {
		if lr.N == 0 {
			err = ErrResponseTooLarge
		}
	}()
	dec := json.NewDecoder(lr)
	token, err := dec.Token()
	if err != nil {
		return nil, err
	}
	b := logResponseBuilder{}
	if token != nil {
		if token != json.Delim('{') {
			return nil, errors.New("log response must be an object")
		}
		b.logs = make(map[string]string)
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return nil, err
			}
			name, ok := key.(string)
			if !ok {
				return nil, errors.New("log response key must be a string")
			}
			value, err := dec.Token()
			if err != nil {
				return nil, err
			}
			output, ok := value.(string)
			if !ok {
				return nil, errors.New("log response value must be a string")
			}
			if err := b.add(name, output); err != nil {
				return nil, err
			}
		}
		if _, err := dec.Token(); err != nil {
			return nil, err
		}
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("trailing JSON after log response")
		}
		return nil, err
	}
	return &b.logs, nil
}

// WriteJSON writes the backend's log map. A nil map retains the JSON null shape.
func (response LogResponse) WriteJSON(w io.Writer) error {
	bw := bufio.NewWriter(w)
	if err := writeLogMap(bw, response.logs); err != nil {
		return err
	}
	return bw.Flush()
}

// WriteEnvelopeJSON writes tenant response metadata followed by the same log
// map encoder. Metadata is small and separate from tenant-controlled log text.
func (response LogResponse) WriteEnvelopeJSON(w io.Writer, metadata map[string]string) error {
	if _, exists := metadata["logs"]; exists {
		return errors.New("log envelope metadata cannot replace logs")
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	if metadata == nil {
		encoded = []byte("{}")
	}
	bw := bufio.NewWriter(w)
	if _, err := bw.Write(encoded[:len(encoded)-1]); err != nil {
		return err
	}
	if len(metadata) > 0 {
		if err := bw.WriteByte(','); err != nil {
			return err
		}
	}
	if _, err := bw.WriteString(`"logs":`); err != nil {
		return err
	}
	if err := writeLogMap(bw, response.logs); err != nil {
		return err
	}
	if _, err := bw.WriteString("}\n"); err != nil {
		return err
	}
	return bw.Flush()
}

func writeLogMap(w *bufio.Writer, logs map[string]string) error {
	if logs == nil {
		_, err := w.WriteString("null")
		return err
	}
	if err := w.WriteByte('{'); err != nil {
		return err
	}
	for i, key := range slices.Sorted(maps.Keys(logs)) {
		if i > 0 {
			if err := w.WriteByte(','); err != nil {
				return err
			}
		}
		if err := writeLogString(w, key); err != nil {
			return err
		}
		if err := w.WriteByte(':'); err != nil {
			return err
		}
		if err := writeLogString(w, logs[key]); err != nil {
			return err
		}
	}
	return w.WriteByte('}')
}

func writeLogString(w *bufio.Writer, value string) error {
	if err := w.WriteByte('"'); err != nil {
		return err
	}
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	for len(value) > 0 {
		end := min(len(value), 8<<10)
		// Keep valid UTF-8 runes together; invalid source bytes retain the
		// standard library's replacement behavior. Only framing is ours;
		// encoding/json still owns every escape and Unicode conversion.
		for end < len(value) && end > 0 && !utf8.RuneStart(value[end]) {
			end--
		}
		if end == 0 {
			end = min(len(value), 8<<10)
		}
		encoded.Reset()
		if err := encoder.Encode(value[:end]); err != nil {
			return fmt.Errorf("encode log string: %w", err)
		}
		fragment := encoded.Bytes()
		if _, err := w.Write(fragment[1 : len(fragment)-2]); err != nil {
			return err
		}
		value = value[end:]
	}
	return w.WriteByte('"')
}
