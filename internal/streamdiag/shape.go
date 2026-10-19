package streamdiag

import "strings"

// Shape observations only visit fixed keys. Values and unknown keys never
// leave the request, and neither the input object nor routing is modified.
func (o *Observer) UpstreamShape(obj map[string]any) {
	if o == nil {
		return
	}
	defer o.protect()
	if o.m.UpstreamShape == nil {
		o.m.UpstreamShape = make(map[string]uint64)
	}
	observeShape(o.m.UpstreamShape, obj)
}

func (o *Observer) AdapterShape(obj map[string]any) {
	if o == nil {
		return
	}
	defer o.protect()
	if o.m.AdapterShape == nil {
		o.m.AdapterShape = make(map[string]uint64)
	}
	observeShape(o.m.AdapterShape, obj)
}

func observeShape(dst map[string]uint64, obj map[string]any) {
	dst["frames"]++
	toolBudget := 32
	choices, ok := obj["choices"].([]any)
	if !ok {
		dst["choices_not_array"]++
		return
	}
	if len(choices) == 0 {
		dst["choices_empty"]++
	}
	for index, raw := range choices {
		if index >= 8 {
			dst["choices_limit"]++
			break
		}
		choice, ok := raw.(map[string]any)
		if !ok {
			dst["choice_not_object"]++
			continue
		}
		for _, field := range []string{"delta", "message"} {
			value, present := choice[field]
			if !present {
				continue
			}
			message, ok := value.(map[string]any)
			if !ok {
				dst[field+"_not_object"]++
				continue
			}
			dst[field+"_objects"]++
			for _, key := range []string{"content", "reasoning_content", "reasoning", "refusal"} {
				value, present := message[key]
				if !present {
					continue
				}
				name := field + "_" + key
				switch value := value.(type) {
				case string:
					if len(value) == 0 {
						dst[name+"_empty"]++
					} else {
						dst[name+"_nonempty"]++
						dst[name+"_bytes"] += uint64(len(value))
					}
				case []any:
					dst[name+"_array"]++
				case map[string]any:
					dst[name+"_object"]++
				case nil:
					dst[name+"_null"]++
				default:
					dst[name+"_other"]++
				}
			}
			if calls, ok := message["tool_calls"].([]any); ok {
				dst[field+"_tool_calls"] += uint64(len(calls))
				for _, rawCall := range calls {
					if toolBudget == 0 {
						dst[field+"_tool_observation_overflow"]++
						continue
					}
					toolBudget--
					call, ok := rawCall.(map[string]any)
					if !ok {
						dst[field+"_tool_item_nonobject"]++
						continue
					}
					dst[field+"_tool_item_object"]++
					if _, exists := call["id"]; exists {
						dst[field+"_tool_id_present"]++
					} else {
						dst[field+"_tool_id_missing"]++
					}
					if _, exists := call["index"]; !exists {
						dst[field+"_tool_index_missing"]++
					} else if _, ok := call["index"].(float64); ok {
						dst[field+"_tool_index_number"]++
					} else {
						dst[field+"_tool_index_other"]++
					}
					fn, ok := call["function"].(map[string]any)
					if !ok {
						if _, exists := call["function"]; exists {
							dst[field+"_tool_function_nonobject"]++
						} else {
							dst[field+"_tool_function_missing"]++
						}
						continue
					}
					dst[field+"_tool_function_object"]++
					value, exists := fn["name"]
					if !exists {
						dst[field+"_tool_name_missing"]++
					} else if name, ok := value.(string); !ok {
						dst[field+"_tool_name_nonstring"]++
					} else if name == "" {
						dst[field+"_tool_name_empty"]++
					} else if strings.TrimSpace(name) == "" {
						dst[field+"_tool_name_blank"]++
					} else {
						dst[field+"_tool_name_nonempty"]++
					}
				}
			}
			if _, ok := message["function_call"].(map[string]any); ok {
				dst[field+"_function_call"]++
			}
		}
		if finish, ok := choice["finish_reason"].(string); ok && finish != "" {
			switch finish {
			case "stop", "length", "max_tokens", "tool_calls", "function_call", "content_filter":
				dst["finish_"+finish]++
			default:
				dst["finish_other"]++
			}
		}
	}
}
