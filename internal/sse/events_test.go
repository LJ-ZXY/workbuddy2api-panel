package sse

import (
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestDecoderEveryByteBoundary(t *testing.T) {
	for _, nl := range []string{"\n", "\r\n", "\r"} {
		wire := "\ufeffevent: ignored" + nl + "id: ignored" + nl + ": comment" + nl +
			"data:{" + nl + "data: \"x\":\"\u6211\"}" + nl + nl + "data: [DONE]" + nl + nl
		var want []Event
		var base Decoder
		collect := func(e Event) error { e.Fragmented = false; want = append(want, e); return nil }
		if base.Feed([]byte(wire), collect) != nil || base.End() != nil {
			t.Fatal("whole event decode failed")
		}
		if len(want) != 2 || want[0].Data != "{\n\"x\":\"我\"}" || want[0].MetadataLines != 3 || want[0].DataLines != 2 {
			t.Fatal("SSE fields were not preserved")
		}
		for split := 0; split <= len(wire); split++ {
			var d Decoder
			var got []Event
			emit := func(e Event) error { e.Fragmented = false; got = append(got, e); return nil }
			if d.Feed([]byte(wire[:split]), emit) != nil || d.Feed([]byte(wire[split:]), emit) != nil || d.End() != nil {
				t.Fatal("split decoding failed")
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatal("byte boundary changed payload")
			}
		}
	}
}

func TestReaderCompactionAndIncomplete(t *testing.T) {
	wire := "event: ignored\r\ndata:{\r\ndata: \"x\": 1}\r\n\r\ndata: [DONE]\r\n\r\n"
	raw, err := io.ReadAll(NewReader(strings.NewReader(wire), nil))
	if err != nil || string(raw) != "data: {\"x\":1}\n\ndata: [DONE]\n\n" {
		t.Fatal("canonical payload changed")
	}
	for _, wire := range []string{"data: {\"x\":1}", "data: {\"x\":1}\n"} {
		raw, err := io.ReadAll(NewReader(strings.NewReader(wire), nil))
		if !errors.Is(err, ErrIncomplete) || len(raw) != 0 {
			t.Fatal("unterminated event dispatched")
		}
	}
	_, err = io.ReadAll(NewReader(strings.NewReader("data: {invalid}\n\n"), nil))
	if !errors.Is(err, ErrData) {
		t.Fatal("invalid data accepted")
	}
}

func TestDecoderLimitsAndCallbackFailure(t *testing.T) {
	var d Decoder
	err := d.Feed([]byte(strings.Repeat("x", MaxEventBytes+1)), func(Event) error { return nil })
	if !errors.Is(err, ErrLimit) {
		t.Fatal("event buffer unbounded")
	}
	var another Decoder
	err = another.Feed([]byte("data: {}\n\n"), func(Event) error { return io.ErrClosedPipe })
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal("callback error hidden")
	}
}
