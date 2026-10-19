package streamdiag

import "crypto/sha256"

const maxTrackedToolKeys = 128

func (o *Observer) nameCount(key string) {
	if o.m.ToolNameFlow == nil {
		o.m.ToolNameFlow = make(map[string]uint64)
	}
	o.m.ToolNameFlow[key]++
}

// Keys are transient hashes; only fixed counters reach the recorder.
func (o *Observer) ToolNameDecision(key string, valid, present, alreadySent bool) {
	if o == nil {
		return
	}
	defer o.protect()
	if alreadySent {
		if present {
			o.nameCount("repeated_name_removed")
		}
		return
	}
	if o.toolKeys == nil {
		o.toolKeys = make(map[[32]byte]bool)
	}
	hash := sha256.Sum256([]byte(key))
	_, observed := o.toolKeys[hash]
	if !observed && len(o.toolKeys) >= maxTrackedToolKeys {
		o.nameCount("tracking_overflow")
		return
	}
	if valid {
		o.nameCount("first_valid_name_forwarded")
		if observed {
			o.nameCount("late_name_forwarded")
		}
	}
	o.toolKeys[hash] = true
}

func (o *Observer) ToolNameOverflow() {
	if o != nil {
		o.nameCount("observation_overflow")
	}
}

func (o *Observer) NamelessToolDropped() {
	if o != nil {
		o.nameCount("nameless_tool_dropped")
	}
}

func (o *Observer) ToolFinishWithoutValidTool() {
	if o != nil {
		o.nameCount("tool_finish_without_valid_tool")
	}
}
