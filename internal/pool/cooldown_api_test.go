package pool

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
)

type cooldownAPIRow struct {
	Model            string     `json:"model"`
	Kind             string     `json:"kind"`
	Until            time.Time  `json:"until"`
	ResetAt          *time.Time `json:"reset_at"`
	RemainingSeconds *int64     `json:"remaining_seconds"`
}

func cooldownAPIRows(t *testing.T, p *Pool, e *entry, now time.Time) []cooldownAPIRow {
	t.Helper()
	p.mu.RLock()
	rows := p.rateLimitedModelsLocked(e, now)
	p.mu.RUnlock()
	raw, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	var decoded []cooldownAPIRow
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

func TestCooldownAPIIndependentDeadlines(t *testing.T) {
	// Fixed historical time catches an accidental time.Until/time.Now in the reader.
	now := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	e := &entry{modelCooldowns: map[string]modelCooldown{
		"model-c": {Until: now.Add(2 * time.Second), Reason: "11102 model unavailable"},
		"model-a": {Until: now.Add(time.Nanosecond)},
		"model-b": {Until: now.Add(time.Second + time.Nanosecond)},
	}}
	rows := cooldownAPIRows(t, New(""), e, now)
	if len(rows) != 3 {
		t.Fatalf("rows=%+v, want three independent models", rows)
	}
	for i, want := range []struct {
		model     string
		remaining int64
		kind      string
	}{
		{"model-a", 1, "rate_limit"},
		{"model-b", 2, "rate_limit"},
		{"model-c", 2, "model_unavailable"},
	} {
		row := rows[i]
		if row.Model != want.model || !row.Until.Equal(e.modelCooldowns[want.model].Until) || row.Kind != want.kind {
			t.Errorf("row=%+v, want independent deadline for %s (%s)", row, want.model, want.kind)
		}
		if row.RemainingSeconds == nil || *row.RemainingSeconds != want.remaining {
			t.Errorf("%s remaining_seconds=%v, want %d", row.Model, row.RemainingSeconds, want.remaining)
		}
	}
}

func TestCooldownAPIResetAt(t *testing.T) {
	now := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	until := now.Add(time.Minute)
	for _, tc := range []struct {
		name  string
		reset time.Time
	}{
		{"same", until},
		{"different", until.Add(time.Hour)},
		{"unknown", time.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &entry{modelCooldowns: map[string]modelCooldown{
				"model": {Until: until, ResetAt: tc.reset},
			}}
			rows := cooldownAPIRows(t, New(""), e, now)
			if len(rows) != 1 {
				t.Fatalf("rows=%+v, want one model", rows)
			}
			if tc.reset.IsZero() {
				if rows[0].ResetAt != nil {
					t.Errorf("unknown reset_at must be omitted, got %v", rows[0].ResetAt)
				}
			} else if rows[0].ResetAt == nil || !rows[0].ResetAt.Equal(tc.reset) {
				t.Errorf("reset_at=%v, want %v even when equal to until", rows[0].ResetAt, tc.reset)
			}
		})
	}
}

func TestCooldownAPIExcludesExpiredRecords(t *testing.T) {
	now := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	e := &entry{modelCooldowns: map[string]modelCooldown{
		"past":   {Until: now.Add(-time.Nanosecond), ResetAt: now.Add(time.Hour)},
		"equal":  {Until: now},
		"zero":   {ResetAt: now.Add(time.Hour)},
		"future": {Until: now.Add(time.Nanosecond)},
	}}
	rows := cooldownAPIRows(t, New(""), e, now)
	if len(rows) != 1 || rows[0].Model != "future" {
		t.Fatalf("rows=%+v, want only the strictly future deadline", rows)
	}
	for _, cooldowns := range []map[string]modelCooldown{
		nil,
		{},
		{"expired": {Until: now.Add(-time.Second)}},
	} {
		e.modelCooldowns = cooldowns
		if rows := cooldownAPIRows(t, New(""), e, now); rows != nil {
			t.Errorf("rows=%+v, want nil without active records", rows)
		}
	}
}

func TestCooldownAPIReadsDoNotMutateState(t *testing.T) {
	for _, dirty := range []bool{false, true} {
		name := "clean"
		if dirty {
			name = "dirty"
		}
		t.Run(name, func(t *testing.T) {
			p := New("") // No background flusher: only the reads can affect this fixture.
			p.Add(&auth.Auth{UID: "u1"})
			now := time.Now()
			e := p.byUID["u1"]
			e.modelCooldowns = map[string]modelCooldown{
				"active":  {Until: now.Add(time.Hour), ResetAt: now.Add(time.Hour), Hits: 2},
				"expired": {Until: now.Add(-time.Hour), Hits: 3, AuditOnly: true},
			}
			beforeCooldowns := make(map[string]modelCooldown, len(e.modelCooldowns))
			for model, cooldown := range e.modelCooldowns {
				beforeCooldowns[model] = cooldown
			}
			p.stateFp = filepath.Join(t.TempDir(), "state.json")
			p.dirty.Store(true)
			p.Flush()
			beforeFile, err := os.ReadFile(p.stateFp)
			if err != nil {
				t.Fatal(err)
			}
			beforeInfo, err := os.Stat(p.stateFp)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(beforeFile), "remaining_seconds") {
				t.Fatal("API countdown must not be persisted")
			}
			if !strings.Contains(string(beforeFile), "model_cooldowns") {
				t.Fatal("existing model cooldown persistence must be preserved")
			}
			p.mu.RLock()
			beforeState := p.stateOverviewLocked()
			p.mu.RUnlock()
			p.dirty.Store(dirty)
			for i := 0; i < 10; i++ {
				status, ok := p.Status("u1")
				if !ok || len(status.RateLimitedModels) != 1 || len(p.List()) != 1 {
					t.Fatal("read lost the account or active cooldown")
				}
				cooldownAPIRows(t, p, e, now)
				status.RateLimitedModels[0].Until = time.Time{}
				status.RateLimitedModels[0].Reason = "caller-owned copy"
			}
			p.mu.RLock()
			afterState := p.stateOverviewLocked()
			p.mu.RUnlock()
			if !reflect.DeepEqual(beforeState, afterState) || !reflect.DeepEqual(beforeCooldowns, e.modelCooldowns) {
				t.Fatal("reads mutated pool state or removed an expired record")
			}
			if p.dirty.Load() != dirty {
				t.Fatal("reads changed the persistence dirty flag")
			}
			afterFile, err := os.ReadFile(p.stateFp)
			if err != nil {
				t.Fatal(err)
			}
			afterInfo, err := os.Stat(p.stateFp)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(beforeFile, afterFile) || !beforeInfo.ModTime().Equal(afterInfo.ModTime()) {
				t.Fatal("reads changed the persisted state file")
			}
		})
	}
}

func TestCooldownAPIConcurrentReads(t *testing.T) {
	p := New("")
	p.Add(&auth.Auth{UID: "u1"})
	now := time.Now()
	e := p.byUID["u1"]
	e.modelCooldowns = map[string]modelCooldown{
		"active":  {Until: now.Add(time.Hour), ResetAt: now.Add(time.Hour)},
		"expired": {Until: now.Add(-time.Hour)},
	}
	p.dirty.Store(false)
	var readers sync.WaitGroup
	for i := 0; i < 8; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for j := 0; j < 25; j++ {
				status, ok := p.Status("u1")
				if !ok || len(status.RateLimitedModels) != 1 || len(p.List()) != 1 {
					t.Error("concurrent read lost active cooldown")
					return
				}
			}
		}()
	}
	readers.Wait()
	if len(e.modelCooldowns) != 2 || p.dirty.Load() {
		t.Fatal("concurrent reads mutated cooldown records or dirty flag")
	}
}
