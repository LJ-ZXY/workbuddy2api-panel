// Package sse incrementally decodes events without retaining a whole response.
package sse

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

const MaxEventBytes = 8 << 20

var (
	ErrIncomplete = errors.New("incomplete SSE event")
	ErrLimit      = errors.New("SSE event exceeds buffer limit")
	ErrData       = errors.New("invalid SSE JSON data")
)

type Event struct {
	Data                     string
	HasData                  bool
	MetadataLines, DataLines int
	Fragmented               bool
}

// EOF does not dispatch an unterminated event.
type Decoder struct {
	line                        []byte
	data                        strings.Builder
	metadata, lines, size       int
	skipLF, started, fragmented bool
}

func (d *Decoder) Feed(p []byte, emit func(Event) error) error {
	if len(d.line) > 0 || d.lines > 0 || d.metadata > 0 {
		d.fragmented = true
	}
	for _, c := range p {
		if d.skipLF {
			d.skipLF = false
			if c == '\n' {
				continue
			}
		}
		if c == '\r' || c == '\n' {
			if err := d.lineEnd(emit); err != nil {
				return err
			}
			d.skipLF = c == '\r'
			continue
		}
		d.size++
		if d.size > MaxEventBytes {
			return ErrLimit
		}
		d.line = append(d.line, c)
	}
	return nil
}

func (d *Decoder) lineEnd(emit func(Event) error) error {
	line := string(d.line)
	d.line = d.line[:0]
	if !d.started {
		d.started = true
		line = strings.TrimPrefix(line, "\ufeff")
	}
	if line == "" {
		e := Event{Data: d.data.String(), HasData: d.lines > 0,
			MetadataLines: d.metadata, DataLines: d.lines, Fragmented: d.fragmented}
		d.data = strings.Builder{}
		d.metadata, d.lines, d.size = 0, 0, 0
		d.fragmented = false
		if e.HasData || e.MetadataLines > 0 {
			return emit(e)
		}
		return nil
	}
	field, value, _ := strings.Cut(line, ":")
	value = strings.TrimPrefix(value, " ")
	if field != "data" {
		d.metadata++
		return nil
	}
	if d.lines > 0 {
		d.data.WriteByte('\n')
	}
	d.data.WriteString(value)
	d.lines++
	return nil
}

func (d *Decoder) End() error {
	if d.lines > 0 || strings.HasPrefix(string(d.line), "data") {
		return ErrIncomplete
	}
	return nil
}

type Reader struct {
	source   io.Reader
	decoder  Decoder
	out      []byte
	pending  error
	observe  func(Event)
	failure  func()
	reported bool
}

// NewReader canonicalizes complete events before legacy line-oriented readers.
func NewReader(source io.Reader, observe func(Event), failure ...func()) *Reader {
	r := &Reader{source: source, observe: observe}
	if len(failure) > 0 {
		r.failure = failure[0]
	}
	return r
}

func (r *Reader) event(e Event) error {
	if r.observe != nil {
		r.observe(e)
	}
	if !e.HasData {
		r.out = append(r.out, ": keepalive\n\n"...)
		return nil
	}
	payload := e.Data
	if strings.TrimSpace(payload) == "[DONE]" {
		payload = "[DONE]"
	} else {
		var compact bytes.Buffer
		if json.Compact(&compact, []byte(payload)) != nil {
			return ErrData
		}
		payload = compact.String()
	}
	r.out = append(r.out, "data: "...)
	r.out = append(r.out, payload...)
	r.out = append(r.out, '\n', '\n')
	if payload == "[DONE]" {
		return io.EOF
	}
	return nil
}

func (r *Reader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(r.out) == 0 && r.pending == nil {
		var buf [32 * 1024]byte
		n, err := r.source.Read(buf[:])
		if n > 0 {
			r.pending = r.decoder.Feed(buf[:n], r.event)
		}
		if r.pending == nil && err != nil {
			if err == io.EOF {
				r.pending = r.decoder.End()
			}
			if r.pending == nil {
				r.pending = err
			}
		}
		if n == 0 && err == nil {
			return 0, nil
		}
		if !r.reported && (r.pending == ErrData || r.pending == ErrIncomplete || r.pending == ErrLimit) {
			r.reported = true
			if r.failure != nil {
				r.failure()
			}
		}
	}
	if len(r.out) > 0 {
		n := copy(p, r.out)
		r.out = r.out[n:]
		return n, nil
	}
	return 0, r.pending
}

func (r *Reader) Close() error {
	if closer, ok := r.source.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
