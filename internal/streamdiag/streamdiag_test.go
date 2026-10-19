package streamdiag

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const gateway = "dg_0123456789abcdef0123456789abcdef"
const request = "req-0123456789abcdef"

func sample(r *Recorder) Metadata {
	return r.Begin("/v1/responses", "glm-5.3", request, gateway).m
}

func files(t *testing.T, dir string) []string {
	t.Helper()
	out, err := filepath.Glob(filepath.Join(dir, "sidecar-stream-*.jsonl"))
	if err != nil {
		t.Fatal("glob failed")
	}
	return out
}

func TestDefaultOffAndExactFilter(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "absent")
	t.Setenv("WB2A_STREAM_DIAGNOSTICS_DIR", dir)
	for _, flag := range []string{"", "0", "true", "yes", " 1", "1 "} {
		t.Setenv("WB2A_STREAM_DIAGNOSTICS", flag)
		r := NewFromEnv()
		if r != nil {
			t.Fatal("nonliteral flag enabled diagnostics")
		}
		r.Begin("/v1/responses", "glm-5.3", request, gateway).Complete(false)
		r.Close()
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("off touched filesystem")
	}
	t.Setenv("WB2A_STREAM_DIAGNOSTICS", "1")
	t.Setenv("WB2A_STREAM_DIAGNOSTICS_DIR", "")
	if environmentDir() != DefaultDir {
		t.Fatal("wrong default directory")
	}
	t.Setenv("WB2A_STREAM_DIAGNOSTICS_DIR", dir)
	r := NewFromEnv()
	if r == nil || r.dir != dir {
		t.Fatal("literal flag/override not enabled")
	}
	r.Close()
	local := New(t.TempDir())
	defer local.Close()
	for _, model := range []string{"glm-5.3", "cn:glm-5.3", "global:glm-5.3", "deepseek-v4.1-flash", "cn:deepseek-v4.1-flash", "gpt-6.1-sol"} {
		if local.Begin("/v1/responses", model, request, gateway) == nil {
			t.Fatal("eligible model rejected")
		}
	}
	for _, model := range []string{"glm-5.3 ", " glm-5.3", "evil model text", "model/with/slash", "model;injection", ""} {
		if local.Begin("/v1/responses", model, request, gateway) != nil {
			t.Fatal("ineligible model accepted")
		}
	}
	for _, path := range []string{"/responses", "/v1/chat/completions", "/v1/responses/"} {
		if local.Begin(path, "glm-5.3", request, gateway) != nil {
			t.Fatal("ineligible path accepted")
		}
	}
	// Messages 协议与 Responses 同权：两条路径都产出同一套诊断，便于跨协议对比。
	for _, path := range []string{"/v1/messages", "/messages"} {
		if local.Begin(path, "glm-5.3", request, gateway) == nil {
			t.Fatal("eligible messages path rejected")
		}
	}
}

func TestCorrelationValidation(t *testing.T) {
	r := newRecorder(t.TempDir())
	if sample(r).GatewayID != gateway || sample(r).RequestID != request {
		t.Fatal("valid correlation rejected")
	}
	for _, id := range []string{"", strings.ToUpper(gateway), gateway + "0", gateway[:len(gateway)-1], " " + gateway, gateway + ",other", strings.ReplaceAll(gateway, "a", "g"), "Bearer fake-secret", "dg_+0123456789abcdef0123456789abcdef"} {
		if GatewayID(id) != "" {
			t.Fatal("invalid correlation accepted")
		}
	}
	if m := r.Begin("/v1/responses", "glm-5.3", "req-123", "bad"); m.m.RequestID != "" || m.m.GatewayID != "" {
		t.Fatal("invalid ids persisted")
	}
}

func TestAllEndReasons(t *testing.T) {
	cases := []struct {
		finish                                     string
		done, readErr, canceled, errorFrame, limit bool
		want                                       EndReason
	}{
		{"stop", true, false, false, false, false, NormalFinish},
		{"tool_calls", false, false, false, false, false, NormalFinish},
		{"content_filter", true, false, false, false, false, NormalFinish},
		{"length", true, false, false, false, true, ExplicitOutputLimit},
		{"max_tokens", true, false, false, false, true, ExplicitOutputLimit},
		{"length", true, false, false, false, false, OutputLimit},
		{"", false, false, false, false, false, EOFWithoutFinishDone},
		{"stop", false, true, false, false, false, ReadError},
		{"", false, true, true, false, false, ClientCancellation},
		{"", false, false, false, true, false, ErrorFrame},
		{"", true, false, false, false, false, DoneWithoutFinish},
		{"fake-free-text", true, false, false, false, false, UnknownFinish},
	}
	r := newRecorder(t.TempDir())
	o := r.Begin("/v1/responses", "glm-5.3", request, gateway)
	for _, c := range cases {
		got := ClassifyEnd(c.finish, c.done, c.readErr, c.canceled, c.errorFrame, c.limit)
		if got != c.want {
			t.Fatalf("reason=%d want=%d", got, c.want)
		}
		o.Segment(got, c.done)
	}
	for _, name := range endNames {
		if o.m.EndReasons[name] == 0 {
			t.Fatal("reason not counted")
		}
	}
	for i := SkipReason(0); i < skipReasonCount; i++ {
		o.Skip(i)
	}
	o.Skip(255)
	o.ContinuationAttempt()
	if len(o.m.ContinuationSkips) != int(skipReasonCount) || o.m.ContinuationAttempts != 1 {
		t.Fatal("skip/attempt counters wrong")
	}
}

func TestLifecycleAndPrivacy(t *testing.T) {
	r := newRecorder(t.TempDir())
	o := r.Begin("/v1/responses", "global:glm-5.3", request, gateway)
	fake := "fake-secret-prompt-reasoning-auth-cookie-phone-nickname-ip"
	add := map[string]any{"output_index": 0, "item": map[string]any{"id": fake, "type": fake, "arguments": fake}}
	o.Event("response.output_item.added", add, false)
	add["output_index"] = 1
	o.Event("response.output_item.added", add, false)
	o.Event("response.output_item.done", add, false)
	o.Event("response.output_item.done", add, false)
	o.Event("response.output_text.delta", map[string]any{"item_id": fake, "delta": fake}, false)
	o.Event("response.reasoning_summary_text.delta", map[string]any{"item_id": fake, "delta": fake}, false)
	o.Event(fake, map[string]any{"error": fake}, false)
	o.Event("response.failed", map[string]any{"response": map[string]any{"status": fake, "error": fake}}, false)
	for _, status := range []string{"completed", "incomplete", "failed"} {
		o.Event("response."+status, map[string]any{"response": map[string]any{"status": status}}, false)
	}
	o.Event("response.output_item.done", nil, false) // malformed but no panic
	o.Event("response.created", nil, true)
	if o.m.DuplicateID != 1 || o.m.DuplicateAdd != 1 || o.m.DuplicateDone != 1 || o.m.DeltaAfterDone != 2 || o.m.ReasoningAfterText != 1 || o.m.ParserFailures != 1 || !o.m.DownstreamWriteFailed {
		t.Fatal("anomaly counts wrong")
	}
	raw, err := encode(o.m)
	if err != nil || strings.Contains(string(raw), fake) {
		t.Fatal("free text escaped metadata allowlist")
	}
	if o.m.TerminalCompleted != 1 || o.m.TerminalIncomplete != 1 || o.m.TerminalFailed != 1 || o.m.UnknownTerminalStatus != 1 || o.m.UnknownEvents != 1 {
		t.Fatal("terminal counters wrong")
	}
	if len(o.items) != 1 {
		t.Fatal("tracking changed")
	}
}

func TestItemTrackingBound(t *testing.T) {
	o := newRecorder(t.TempDir()).Begin("/v1/responses", "glm-5.3", "", "")
	for i := 0; i < maxTrackedItems+1; i++ {
		o.Event("response.output_item.added", map[string]any{"item": map[string]any{"id": fmt.Sprintf("fake-item-%d", i)}}, false)
	}
	o.Event("response.output_item.added", map[string]any{"item": map[string]any{"id": strings.Repeat("x", 4097)}}, false)
	if len(o.items) != maxTrackedItems || o.m.ItemTrackingOverflow != 2 {
		t.Fatal("tracking unbounded")
	}
}

func TestObserverPanicContainmentAndCompleteOnce(t *testing.T) {
	r := newRecorder(t.TempDir())
	o := r.Begin("/v1/responses", "glm-5.3", "", "")
	o.m.ResponseEvents = nil
	o.Event("response.created", nil, false)
	if o.m.ParserFailures != 1 {
		t.Fatal("observer panic escaped or uncounted")
	}
	o.Complete(false)
	o.Complete(false)
	if len(r.queue) != 1 {
		t.Fatal("duplicate summary")
	}
}

func TestQueueBoundNonblockingAndConcurrentClose(t *testing.T) {
	r := newRecorder(t.TempDir()) // deliberately no consumer
	m := sample(r)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 256; j++ {
				r.enqueue(m)
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("enqueue blocked")
	}
	if cap(r.queue) != 256 || len(r.queue) != 256 || r.Stats().Dropped != 4096-256 {
		t.Fatal("queue/drop bound wrong")
	}
	go r.run()
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); r.Close(); r.enqueue(m) }()
	}
	wg.Wait()
	if !r.closed.Load() {
		t.Fatal("close failed")
	}
}

func TestRotationRetentionCapPermissionsAndNamespace(t *testing.T) {
	dir := t.TempDir()
	r := newRecorder(dir)
	now := time.Now().UTC()
	r.now = func() time.Time { return now }
	r.fileMax, r.maxBytes = 2200, 5000
	old := filepath.Join(dir, "sidecar-stream-20000101-000000-000001.jsonl")
	fresh := filepath.Join(dir, "sidecar-stream-20000101-000000-000002.jsonl")
	foreign := filepath.Join(dir, "proxy-stream-old.jsonl")
	for _, p := range []string{old, fresh, foreign} {
		if os.WriteFile(p, []byte("{}\n"), 0644) != nil {
			t.Fatal("fixture write failed")
		}
	}
	if os.Chtimes(old, now.Add(-8*24*time.Hour), now.Add(-8*24*time.Hour)) != nil || os.Chtimes(fresh, now.Add(-6*24*time.Hour), now.Add(-6*24*time.Hour)) != nil {
		t.Fatal("fixture time failed")
	}
	if !r.maintain(0) {
		t.Fatal("maintain failed")
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatal("retention failed")
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Fatal("recent archive removed")
	}
	for i := 0; i < 15; i++ {
		r.write(sample(r))
		var total int64
		for _, p := range files(t, dir) {
			st, err := os.Stat(p)
			if err != nil {
				t.Fatal("stat failed")
			}
			if st.Mode().Perm() != 0600 || st.Size() > r.fileMax {
				t.Fatal("file permissions/rotation wrong")
			}
			total += st.Size()
		}
		if total > r.maxBytes {
			t.Fatal("cap exceeded between writes")
		}
	}
	if len(files(t, dir)) < 2 {
		t.Fatal("rotation missing")
	}
	before := r.path
	now = now.Add(24 * time.Hour)
	r.write(sample(r))
	if r.path == before {
		t.Fatal("daily rotation missing")
	}
	r.closeFile()
	st, err := os.Stat(dir)
	if err != nil || st.Mode().Perm() != 0700 {
		t.Fatal("directory permissions wrong")
	}
	got, err := os.ReadFile(foreign)
	if err != nil || string(got) != "{}\n" {
		t.Fatal("proxy namespace touched")
	}
	if MaxBytes != 50<<20 || retention != 7*24*time.Hour {
		t.Fatal("production limits changed")
	}
}

func TestReal50MiBCapBeforeWrite(t *testing.T) {
	r := newRecorder(t.TempDir())
	for i := 0; i < 51; i++ {
		p := filepath.Join(r.dir, fmt.Sprintf("sidecar-stream-20261013-000000-%06d.jsonl", i))
		f, err := os.Create(p)
		if err != nil {
			t.Fatal("fixture create failed")
		}
		if f.Truncate(1<<20) != nil {
			t.Fatal("fixture truncate failed")
		}
		f.Close()
	}
	r.write(sample(r))
	r.closeFile()
	var total int64
	for _, p := range files(t, r.dir) {
		st, _ := os.Stat(p)
		total += st.Size()
	}
	if total > 50<<20 || total == 0 {
		t.Fatal("production cap wrong")
	}
}

func TestJSONAndFilesystemFailureIsolation(t *testing.T) {
	r := newRecorder(t.TempDir())
	m := sample(r)
	m.Time = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	r.write(m)
	if r.Stats().ParserFailures != 1 || r.Stats().Dropped != 1 || len(files(t, r.dir)) != 0 {
		t.Fatal("JSON failure not isolated")
	}
	parent := filepath.Join(t.TempDir(), "file")
	if os.WriteFile(parent, []byte("fake"), 0600) != nil {
		t.Fatal("fixture write failed")
	}
	for _, dir := range []string{parent, filepath.Join(parent, "child")} {
		bad := New(dir)
		bad.Begin("/v1/responses", "glm-5.3", request, gateway).Complete(false)
		bad.Close()
		if bad.Stats().Dropped != 1 || bad.Stats().FilesystemFailures == 0 {
			t.Fatal("filesystem failure not isolated")
		}
	}
	symlink := filepath.Join(t.TempDir(), "link")
	if os.Symlink(r.dir, symlink) != nil {
		t.Fatal("fixture symlink failed")
	}
	bad := newRecorder(symlink)
	bad.write(sample(bad))
	if bad.Stats().Dropped != 1 {
		t.Fatal("directory symlink accepted")
	}
	ownedLink := filepath.Join(r.dir, "sidecar-stream-20261013-000000-000000.jsonl")
	if os.Symlink(parent, ownedLink) != nil {
		t.Fatal("fixture symlink failed")
	}
	r.write(sample(r))
	if r.Stats().FilesystemFailures == 0 {
		t.Fatal("archive symlink accepted")
	}
	os.Remove(ownedLink)
	r.write(sample(r))
	if r.file == nil {
		t.Fatal("writer did not recover")
	}
	r.file.Close()
	r.write(sample(r))
	if r.file != nil || r.Stats().FilesystemFailures < 2 {
		t.Fatal("write error not isolated")
	}
	// Failed encodes and filesystem errors never retain literal error strings.
	stats, _ := json.Marshal(r.Stats())
	if strings.Contains(string(stats), "fake") || strings.Contains(string(stats), parent) {
		t.Fatal("failure metadata leaked")
	}
}

// Messages 协议与 Responses 同构：同一套观察器、同一份 schema，仅 protocol/path 不同。
func TestMessagesObserverLifecycle(t *testing.T) {
	r := newRecorder(t.TempDir())
	o := r.Begin("/v1/messages", "deepseek-v4.1-flash", request, gateway)
	if o == nil || o.m.Protocol != "messages" || o.m.Path != "/v1/messages" {
		t.Fatalf("protocol/path = %q/%q", o.m.Protocol, o.m.Path)
	}
	o.UpstreamFrame()
	o.UpstreamFinish("tool_calls")
	o.UpstreamFinish("fake-free-text")
	o.Event("message_start", map[string]any{"type": "message_start"}, false)
	o.Event("content_block_start", map[string]any{
		"type": "content_block_start", "index": 0,
		"content_block": map[string]any{"type": "thinking", "thinking": "PRIVATE"},
	}, false)
	o.Event("content_block_start", map[string]any{
		"type": "content_block_start", "index": 1,
		"content_block": map[string]any{"type": "text", "text": "PRIVATE"},
	}, false)
	o.Event("content_block_start", map[string]any{
		"type": "content_block_start", "index": 2,
		"content_block": map[string]any{"type": "tool_use", "id": "PRIVATE", "name": "PRIVATE", "input": map[string]any{"secret": "PRIVATE"}},
	}, false)
	o.Event("message_delta", map[string]any{
		"type":  "message_delta",
		"delta": map[string]any{"stop_reason": "tool_use", "stop_sequence": nil},
	}, false)
	o.Event("message_stop", map[string]any{"type": "message_stop"}, false)
	o.Event("content_block_delta", map[string]any{
		"type": "content_block_delta", "index": 2,
		"delta": map[string]any{"type": "input_json_delta", "partial_json": `{"k":"PRIVATE"}`},
	}, false)
	m := o.m
	if m.UpstreamFrames != 1 || m.UpstreamFinishReasons["tool_calls"] != 1 || m.UpstreamFinishReasons["other"] != 1 {
		t.Fatalf("upstream finish reasons = %+v", m.UpstreamFinishReasons)
	}
	if m.StopReasons["tool_use"] != 1 || m.ToolUseBlocks != 1 || m.TextBlocks != 1 || m.ThinkingBlocks != 1 {
		t.Fatalf("stop/blocks = %+v %d/%d/%d", m.StopReasons, m.ToolUseBlocks, m.TextBlocks, m.ThinkingBlocks)
	}
	o.PseudoFinishNudge()
	if o.m.PseudoFinishNudges != 1 {
		t.Fatalf("pseudo finish nudges = %d want 1", o.m.PseudoFinishNudges)
	}
	if m.ResponseEvents["message_start"] != 1 || m.ResponseEvents["message_stop"] != 1 {
		t.Fatalf("anthropic events not counted: %+v", m.ResponseEvents)
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "PRIVATE") {
		t.Fatal("messages diagnostics leaked payload text")
	}
}

func TestAnthropicStopReasonVocabulary(t *testing.T) {
	cases := map[string]string{
		"end_turn": "end_turn", "max_tokens": "max_tokens", "tool_use": "tool_use",
		"refusal": "refusal", "fake-free-text": "other", "": "other",
	}
	for in, want := range cases {
		if got := anthropicStopReason(in); got != want {
			t.Fatalf("anthropicStopReason(%q)=%q want %q", in, got, want)
		}
	}
}
