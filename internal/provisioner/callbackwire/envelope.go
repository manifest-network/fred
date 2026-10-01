package callbackwire

import (
	"bytes"
	"errors"
	"io"
)

const (
	// MaxPayloadBytes matches the callback outbox's existing record ceiling.
	// Callback payloads contain status metadata, never tenant manifests.
	MaxPayloadBytes   = 1 << 20
	maxEnvelopeTokens = 256
	maxEnvelopeDepth  = 16
)

// ReadEnvelope bounds unauthenticated work before the JSON decoder sees it.
// The scanner retains exact wire bytes and counts structure outside strings;
// ordinary JSON decoding still owns syntax and protocol interpretation.
func ReadEnvelope(body io.Reader) ([]byte, error) {
	if body == nil {
		return nil, errors.New("callback body is missing")
	}
	var output envelopeBuffer
	var chunk [1024]byte
	if _, err := io.CopyBuffer(&output, io.LimitReader(body, MaxPayloadBytes+1), chunk[:]); err != nil {
		return nil, err
	}
	return output.body.Bytes(), nil
}

type envelopeBuffer struct {
	body   bytes.Buffer
	budget envelopeBudget
}

func (b *envelopeBuffer) Write(value []byte) (int, error) {
	if err := b.budget.consume(value); err != nil {
		return 0, err
	}
	return b.body.Write(value)
}

type envelopeBudget struct {
	bytes, tokens, depth int
	quoted, escaped      bool
}

func (b *envelopeBudget) consume(value []byte) error {
	for _, current := range value {
		b.bytes++
		if b.bytes > MaxPayloadBytes {
			return errors.New("callback payload exceeds byte budget")
		}
		if b.quoted {
			switch {
			case b.escaped:
				b.escaped = false
			case current == '\\':
				b.escaped = true
			case current == '"':
				b.quoted = false
			}
			continue
		}
		switch current {
		case '"':
			b.quoted = true
			b.tokens++
		case '{', '[':
			b.depth++
			b.tokens++
			if b.depth > maxEnvelopeDepth {
				return errors.New("callback payload exceeds nesting budget")
			}
		case '}', ']':
			b.depth--
			b.tokens++
			if b.depth < 0 {
				return errors.New("callback payload has unbalanced delimiters")
			}
		case ':', ',':
			b.tokens++
		}
		if b.tokens > maxEnvelopeTokens {
			return errors.New("callback payload exceeds structural budget")
		}
	}
	return nil
}
