package streamdiag

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
)

func TestToolNameMetadataBoundedAndPrivate(t *testing.T) {
	rec := New(t.TempDir())
	o := rec.Begin("/v1/responses", "glm-5.3", "req-0123456789abcdef", "")
	for index := 0; index < 200; index++ {
		o.ToolNameDecision(fmt.Sprintf("private-call-id-%d", index), false, false, false)
	}
	if len(o.toolKeys) != 128 || o.m.ToolNameFlow["tracking_overflow"] != 72 {
		t.Fatal("tool key tracking is unbounded")
	}
	o.ToolNameDecision("private-call-id-0", true, true, false)
	o.ToolNameDecision("private-call-id-0", true, true, true)
	var calls []any
	for index := 0; index < 40; index++ {
		calls = append(calls, map[string]any{"id": "private-call-id", "index": float64(index),
			"function": map[string]any{"name": "private-function-name", "arguments": "Bearer fixture-token Cookie=fixture-cookie 18512345678"}})
	}
	obj := map[string]any{"choices": []any{map[string]any{"delta": map[string]any{"tool_calls": calls}}}}
	o.UpstreamShape(obj)
	if o.m.UpstreamShape["delta_tool_item_object"] != 32 || o.m.UpstreamShape["delta_tool_observation_overflow"] != 8 {
		t.Fatal("per-frame tool inspection is unbounded")
	}
	o.Complete(false)
	rec.Close()
	raw, _ := json.Marshal(o.m)
	for _, secret := range []string{"private-call-id", "private-function-name", "fixture-token", "fixture-cookie", "18512345678"} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Fatal("private fields escaped into metadata")
		}
	}
}

func TestToolShapeTypesAndChoiceLimit(t *testing.T) {
	rec := New(t.TempDir())
	o := rec.Begin("/v1/responses", "glm-5.3", "", "")
	calls := []any{7, map[string]any{}, map[string]any{"function": 7},
		map[string]any{"function": map[string]any{}},
		map[string]any{"function": map[string]any{"name": ""}},
		map[string]any{"function": map[string]any{"name": " \t"}},
		map[string]any{"function": map[string]any{"name": 7}},
		map[string]any{"function": map[string]any{"name": "fixture"}}}
	o.UpstreamShape(map[string]any{"choices": []any{map[string]any{"delta": map[string]any{"tool_calls": calls}}}})
	for _, key := range []string{"delta_tool_item_nonobject", "delta_tool_function_nonobject", "delta_tool_name_missing",
		"delta_tool_name_empty", "delta_tool_name_blank", "delta_tool_name_nonstring", "delta_tool_name_nonempty"} {
		if o.m.UpstreamShape[key] != 1 {
			t.Fatal("fixed type bucket missing: " + key)
		}
	}
	choices := make([]any, 20)
	for index := range choices {
		choices[index] = map[string]any{"delta": map[string]any{"tool_calls": calls}}
	}
	o.AdapterShape(map[string]any{"choices": choices})
	if o.m.AdapterShape["choices_limit"] != 1 || o.m.AdapterShape["delta_tool_item_object"] > 32 {
		t.Fatal("choice/tool inspection limit missing")
	}
	o.Complete(false)
	rec.Close()
}
