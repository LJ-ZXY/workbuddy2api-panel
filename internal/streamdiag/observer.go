// Package streamdiag observes metadata only. It never participates in routing or
// protocol decisions. A nil observer is the default, allocation-free no-op.
package streamdiag

import (
	"crypto/sha256"
	"encoding/json"
	"strings"
	"time"
)

const maxTrackedItems = 4096

// EndReason and SkipReason are closed vocabularies, not upstream free text.
type EndReason uint8

const (
	NormalFinish EndReason = iota
	ExplicitOutputLimit
	OutputLimit
	EOFWithoutFinishDone
	ReadError
	ClientCancellation
	ErrorFrame
	DoneWithoutFinish
	UnknownFinish
	endReasonCount
)

var endNames = [...]string{"normal_finish", "explicit_output_limit", "output_limit", "eof_without_finish_done", "read_error", "client_cancellation", "error_frame", "done_without_finish", "unknown_finish"}

type SkipReason uint8

const (
	SegmentLimit SkipReason = iota
	UpstreamError
	ExplicitLimit
	FirstSegmentEOF
	BlindRetryUsed
	NonLengthFinish
	ToolFragment
	EmptyOutput
	ContinuationBodyFailure
	ContinuationUpstreamFailure
	skipReasonCount
)

var skipNames = [...]string{"segment_limit", "upstream_error", "explicit_output_limit", "first_segment_eof", "blind_retry_used", "non_length_finish", "tool_fragment", "empty_output", "continuation_body_failure", "continuation_upstream_failure"}

var eventNames = [...]string{
	"response.created", "response.in_progress", "response.completed", "response.incomplete", "response.failed",
	"response.output_item.added", "response.output_item.done",
	"response.content_part.added", "response.content_part.done",
	"response.output_text.delta", "response.output_text.done",
	"response.reasoning_summary_part.added", "response.reasoning_summary_part.done",
	"response.reasoning_summary_text.delta", "response.reasoning_summary_text.done",
	"response.function_call_arguments.delta", "response.function_call_arguments.done",
	"response.custom_tool_call_input.delta", "response.custom_tool_call_input.done",
}

// Metadata is the complete disk allowlist. No upstream ids, names, strings,
// headers, payloads, error objects, or model text are retained in this schema.
type Metadata struct {
	Schema                 int               `json:"schema"`
	Time                   time.Time         `json:"time"`
	RequestID              string            `json:"request_id,omitempty"`
	GatewayID              string            `json:"gateway_diagnostic_id,omitempty"`
	Path                   string            `json:"path"`
	Model                  string            `json:"model"`
	DurationMS             int64             `json:"duration_ms"`
	FirstUpstreamMS        int64             `json:"first_upstream_ms"`
	FirstResponseMS        int64             `json:"first_response_ms"`
	LastResponseMS         int64             `json:"last_response_ms"`
	UpstreamFrames         uint64            `json:"upstream_frames"`
	UpstreamStreams        uint64            `json:"upstream_streams"`
	Segments               uint64            `json:"segments"`
	DoneMarkers            uint64            `json:"upstream_done_markers"`
	EndReasons             map[string]uint64 `json:"upstream_end_reasons"`
	ContinuationAttempts   uint64            `json:"continuation_attempts"`
	ContinuationSkips      map[string]uint64 `json:"continuation_skips"`
	ResponseEvents         map[string]uint64 `json:"response_events"`
	UnknownEvents          uint64            `json:"unknown_events"`
	TerminalCompleted      uint64            `json:"terminal_completed"`
	TerminalIncomplete     uint64            `json:"terminal_incomplete"`
	TerminalFailed         uint64            `json:"terminal_failed"`
	UnknownTerminalStatus  uint64            `json:"unknown_terminal_status"`
	ItemAdded              uint64            `json:"item_added"`
	ItemDone               uint64            `json:"item_done"`
	DuplicateID            uint64            `json:"duplicate_id"`
	DuplicateAdd           uint64            `json:"duplicate_add"`
	DuplicateDone          uint64            `json:"duplicate_done"`
	DeltaAfterDone         uint64            `json:"delta_after_done"`
	DeltaWithoutActiveItem uint64            `json:"delta_without_active_item"`
	DoneWithoutAdd         uint64            `json:"done_without_add"`
	ReasoningAfterText     uint64            `json:"reasoning_after_text"`
	ItemTrackingOverflow   uint64            `json:"item_tracking_overflow"`
	ParserFailures         uint64            `json:"parser_failures"`
	ClientCanceled         bool              `json:"client_canceled"`
	DownstreamWriteFailed  bool              `json:"downstream_write_failed"`
	Dropped                uint64            `json:"dropped"`
	FilesystemFailures     uint64            `json:"filesystem_failures"`
	UpstreamShape          map[string]uint64 `json:"upstream_shape,omitempty"`
	AdapterShape           map[string]uint64 `json:"adapter_shape,omitempty"`
	ToolNameFlow           map[string]uint64 `json:"tool_name_flow,omitempty"`
	SSEFlow                map[string]uint64 `json:"sse_flow,omitempty"`
}

type itemState struct {
	index       int
	added, done bool
}

// Observer is owned by one synchronous request. Only Recorder is shared.
type Observer struct {
	rec                *Recorder
	start              time.Time
	m                  Metadata
	items              map[[32]byte]itemState
	textSeen, complete bool
	toolKeys           map[[32]byte]bool
}

func validID(s, prefix string, n int) string {
	if len(s) != len(prefix)+n || !strings.HasPrefix(s, prefix) {
		return ""
	}
	for _, c := range s[len(prefix):] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return ""
		}
	}
	return s
}

// GatewayID is inbound correlation only, never a session/stickiness/trace key.
func GatewayID(s string) string { return validID(s, "dg_", 32) }

func eligible(path, model string) bool {
	if path != "/v1/responses" {
		return false
	}
	if model == "" || len(model) > 128 || strings.TrimSpace(model) != model {
		return false
	}
	for _, ch := range model {
		if (ch < 'a' || ch > 'z') && (ch < 'A' || ch > 'Z') && (ch < '0' || ch > '9') && ch != ':' && ch != '-' && ch != '.' && ch != '_' {
			return false
		}
	}
	return true
}

func (r *Recorder) Begin(path, model, requestID, gatewayID string) *Observer {
	if r == nil || !eligible(path, model) {
		return nil
	}
	cleanModel := strings.TrimSpace(model)
	return &Observer{rec: r, start: time.Now(), items: make(map[[32]byte]itemState), m: Metadata{
		Schema: 4, Time: time.Now().UTC(), Path: "/v1/responses", Model: cleanModel,
		RequestID: validID(requestID, "req-", 16), GatewayID: GatewayID(gatewayID),
		FirstUpstreamMS: -1, FirstResponseMS: -1, LastResponseMS: -1,
		EndReasons: map[string]uint64{}, ContinuationSkips: map[string]uint64{}, ResponseEvents: map[string]uint64{},
	}}
}

func (o *Observer) protect() {
	if recover() != nil {
		o.ParserFailure()
	}
}

func (o *Observer) ParserFailure() {
	if o != nil {
		o.m.ParserFailures++
		o.rec.parserFailures.Add(1)
	}
}

func (o *Observer) StartStream() {
	if o != nil {
		o.m.UpstreamStreams++
	}
}

// Only closed stage labels and numeric counts enter the disk allowlist.
func (o *Observer) SSEEvent(stage string, hasData bool, metadata, lines int, fragmented bool) {
	if o == nil {
		return
	}
	defer o.protect()
	switch stage {
	case "input", "normalizer", "responses":
	default:
		return
	}
	if o.m.SSEFlow == nil {
		o.m.SSEFlow = map[string]uint64{}
	}
	m := o.m.SSEFlow
	m[stage+"_events"]++
	if metadata > 0 {
		m[stage+"_metadata_events"]++
		m[stage+"_metadata_lines"] += uint64(metadata)
	}
	if hasData {
		m[stage+"_data_events"]++
		if m[stage+"_data_events"] == 1 {
			m[stage+"_first_data_accepted"]++
		}
	}
	if lines > 1 {
		m[stage+"_multiline_events"]++
	}
	if fragmented {
		m[stage+"_reassembled_events"]++
	}
}

func (o *Observer) SSEFailure() {
	if o == nil {
		return
	}
	o.ParserFailure()
	if o.m.SSEFlow == nil {
		o.m.SSEFlow = map[string]uint64{}
	}
	o.m.SSEFlow["parse_failures"]++
}

func (o *Observer) ToolItemOutput() {
	if o == nil {
		return
	}
	if o.m.SSEFlow == nil {
		o.m.SSEFlow = map[string]uint64{}
	}
	o.m.SSEFlow["tool_items_output"]++
}

func (o *Observer) UpstreamFrame() {
	if o == nil {
		return
	}
	o.m.UpstreamFrames++
	if o.m.FirstUpstreamMS < 0 {
		o.m.FirstUpstreamMS = time.Since(o.start).Milliseconds()
	}
}

func (o *Observer) Segment(reason EndReason, done bool) {
	if o == nil {
		return
	}
	o.m.Segments++
	if done {
		o.m.DoneMarkers++
	}
	if reason >= endReasonCount {
		reason = UnknownFinish
	}
	o.m.EndReasons[endNames[reason]]++
}

func (o *Observer) Skip(reason SkipReason) {
	if o == nil || reason >= skipReasonCount {
		return
	}
	o.m.ContinuationSkips[skipNames[reason]]++
}

func (o *Observer) ContinuationAttempt() {
	if o != nil {
		o.m.ContinuationAttempts++
	}
}

// ClassifyEnd observes the original segment, before any synthetic terminals.
func ClassifyEnd(finish string, done, readError, canceled, errorFrame, explicitLimit bool) EndReason {
	switch {
	case canceled:
		return ClientCancellation
	case readError:
		return ReadError
	case errorFrame:
		return ErrorFrame
	case finish == "length" || finish == "max_tokens":
		if explicitLimit {
			return ExplicitOutputLimit
		}
		return OutputLimit
	case finish == "stop" || finish == "tool_calls" || finish == "function_call" || finish == "content_filter":
		return NormalFinish
	case finish != "":
		return UnknownFinish
	case done:
		return DoneWithoutFinish
	default:
		return EOFWithoutFinishDone
	}
}

// Event consumes the already-marshaled event but only visits structural fields.
// Ids are hashed in bounded transient memory for equality and never serialized.
func (o *Observer) Event(typ string, payload map[string]any, writeFailed bool) {
	if o == nil {
		return
	}
	defer o.protect()
	if writeFailed {
		o.m.DownstreamWriteFailed = true
		return
	}
	known := false
	for _, name := range eventNames {
		if typ == name {
			o.m.ResponseEvents[name]++
			known = true
			break
		}
	}
	if !known {
		o.m.UnknownEvents++
		return
	}
	ms := time.Since(o.start).Milliseconds()
	if o.m.FirstResponseMS < 0 {
		o.m.FirstResponseMS = ms
	}
	o.m.LastResponseMS = ms
	if typ == "response.completed" || typ == "response.incomplete" || typ == "response.failed" {
		resp, _ := payload["response"].(map[string]any)
		status, _ := resp["status"].(string)
		switch status {
		case "completed":
			o.m.TerminalCompleted++
		case "incomplete":
			o.m.TerminalIncomplete++
		case "failed":
			o.m.TerminalFailed++
		default:
			o.m.UnknownTerminalStatus++
		}
	}
	if typ == "response.output_text.delta" {
		o.textSeen = true
	}
	if typ == "response.reasoning_summary_text.delta" && o.textSeen {
		o.m.ReasoningAfterText++
	}
	add, done := typ == "response.output_item.added", typ == "response.output_item.done"
	delta := strings.HasSuffix(typ, ".delta")
	if !add && !done && !delta {
		return
	}
	id, _ := payload["item_id"].(string)
	if add || done {
		item, _ := payload["item"].(map[string]any)
		id, _ = item["id"].(string)
	}
	if id == "" {
		o.ParserFailure()
		return
	}
	// Bound hashing work too; protocol writers retain their original bytes.
	if len(id) > 4096 {
		o.m.ItemTrackingOverflow++
		return
	}
	key := sha256.Sum256([]byte(id))
	state, exists := o.items[key]
	if !exists && len(o.items) >= maxTrackedItems {
		o.m.ItemTrackingOverflow++
		return
	}
	index := -1
	if n, ok := payload["output_index"].(int); ok {
		index = n
	}
	if add {
		o.m.ItemAdded++
		if state.added {
			o.m.DuplicateAdd++
			if index != state.index {
				o.m.DuplicateID++
			}
		}
		state.added, state.index = true, index
	}
	if done {
		o.m.ItemDone++
		if state.done {
			o.m.DuplicateDone++
		}
		if !state.added {
			o.m.DoneWithoutAdd++
		}
		state.done = true
	}
	if delta {
		if state.done {
			o.m.DeltaAfterDone++
		}
		if !state.added || state.done {
			o.m.DeltaWithoutActiveItem++
		}
	}
	o.items[key] = state
}

func (o *Observer) Complete(canceled bool) {
	if o == nil || o.complete {
		return
	}
	defer o.protect()
	o.complete = true
	o.m.ClientCanceled = canceled
	o.m.DurationMS = time.Since(o.start).Milliseconds()
	o.m.Dropped = o.rec.dropped.Load()
	o.m.FilesystemFailures = o.rec.fsFailures.Load()
	o.rec.enqueue(o.m)
}

// encode is intentionally fixed-schema; a JSON encoding failure drops metadata.
func encode(m Metadata) ([]byte, error) { return json.Marshal(m) }
