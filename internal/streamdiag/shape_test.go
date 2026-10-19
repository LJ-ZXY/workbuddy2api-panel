package streamdiag

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestShapeCountsAreBoundedRedactedAndReadOnly(t *testing.T) {
	r := New(t.TempDir())
	defer r.Close()
	o := r.Begin("/v1/responses", "glm-5.3", request, gateway)
	secret := "Bearer fake-shape-secret eyJ.mock.jwt +1-555-123-4567"
	obj := map[string]any{"choices": []any{map[string]any{
		"delta": map[string]any{"content": "", "reasoning_content": secret, "refusal": secret,
			"tool_calls":    []any{map[string]any{"id": secret, "function": map[string]any{"name": secret, "arguments": secret}}},
			"function_call": map[string]any{"name": secret}, secret: secret},
		"message":       map[string]any{"content": []any{secret}, "reasoning": secret},
		"finish_reason": secret,
	}}, "Authorization": secret}
	before, _ := json.Marshal(obj)
	o.UpstreamShape(obj)
	o.AdapterShape(obj)
	after, _ := json.Marshal(obj)
	if string(before) != string(after) {
		t.Fatal("shape observer mutated input")
	}
	counts := o.m.UpstreamShape
	if counts["delta_content_empty"] != 1 || counts["delta_reasoning_content_nonempty"] != 1 ||
		counts["delta_refusal_nonempty"] != 1 || counts["delta_tool_calls"] != 1 ||
		counts["delta_function_call"] != 1 || counts["message_content_array"] != 1 || counts["finish_other"] != 1 {
		t.Fatal("shape classification mismatch")
	}
	encoded, _ := json.Marshal(o.m)
	for _, fragment := range []string{secret, "fake-shape-secret", "eyJ.mock.jwt", "555-123", "Authorization", "arguments"} {
		if strings.Contains(string(encoded), fragment) {
			t.Fatal("shape metadata leaked content")
		}
	}
	for i := 0; i < 10000; i++ {
		o.UpstreamShape(map[string]any{secret: secret, "choices": []any{map[string]any{"finish_reason": secret + string(rune(i))}}})
	}
	if len(o.m.UpstreamShape) > 80 {
		t.Fatal("unbounded diagnostic vocabulary")
	}
	var off *Observer
	off.UpstreamShape(obj)
	off.AdapterShape(obj)
}

func TestShapeVariantsAndChoiceCap(t *testing.T) {
	counts := map[string]uint64{}
	observeShape(counts, map[string]any{})
	observeShape(counts, map[string]any{"choices": []any{}})
	choices := make([]any, 100)
	for i := range choices {
		choices[i] = map[string]any{"delta": map[string]any{"content": nil, "reasoning_content": 3,
			"reasoning": map[string]any{}, "refusal": []any{}}, "message": false, "finish_reason": "stop"}
	}
	observeShape(counts, map[string]any{"choices": choices})
	if counts["choices_limit"] != 1 || counts["delta_content_null"] != 8 ||
		counts["delta_reasoning_content_other"] != 8 || counts["delta_reasoning_object"] != 8 ||
		counts["delta_refusal_array"] != 8 || counts["message_not_object"] != 8 || counts["finish_stop"] != 8 {
		t.Fatal("variant or bounded-choice observation mismatch")
	}
}
