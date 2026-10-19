package pool

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
)

func TestAuthFileEmptyRateLimitArray(t *testing.T) {
	for _, st := range []Status{{UID: "zero"}, {UID: "empty", RateLimitedModels: []RateLimitedModel{}}} {
		raw, err := json.Marshal(st)
		if err != nil || !strings.Contains(string(raw), `"rate_limited_models":[]`) {
			t.Fatal("supported empty observation missing", string(raw), err)
		}
	}
}

func TestAuthFileSetDisabledDurablePreservesEntireEntry(t *testing.T) {
	fp := filepath.Join(t.TempDir(), "state.json")
	p := New(fp)
	defer p.Close()
	a := &auth.Auth{UID: "u", AccessToken: "fixture"}
	p.Add(a)
	p.Cooldown("u", CoolSoft, time.Hour, "cooling reason")
	p.CooldownSoftForModel("u", time.Minute, time.Now().Add(time.Hour), "m", "6004")
	p.Pause("u")
	p.mu.Lock()
	e := p.byUID["u"]
	e.fails, e.retryCount, e.sessionDeadFails, e.softStreak = 3, 2, 1, 4
	e.breakerUntil = time.Now().Add(time.Hour)
	p.mu.Unlock()
	snapshot := func() string {
		p.mu.RLock()
		defer p.mu.RUnlock()
		state := p.stateOverviewLocked()
		account := state.Accounts["u"]
		account.Disabled = false
		state.Accounts["u"] = account
		e := p.byUID["u"]
		raw, err := json.Marshal(struct {
			State             stateFile
			Fails, RetryCount int
			InFlight          int64
		}{state, e.fails, e.retryCount, e.inFlight.Load()})
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	before := snapshot()
	for _, disabled := range []bool{true, false} {
		if err := p.SetDisabled("u", disabled); err != nil {
			t.Fatal(err)
		}
		if before != snapshot() {
			t.Fatal("toggle changed unrelated entry state")
		}
		raw, err := os.ReadFile(fp)
		var state stateFile
		if err != nil || json.Unmarshal(raw, &state) != nil || state.Accounts["u"].Disabled != disabled {
			t.Fatal("desired flag not durable", err)
		}
	}
	if err := p.SetDisabled("missing", true); err == nil {
		t.Fatal("missing account accepted")
	}
}

func TestAuthFileStatusAndRemovePersistenceFailure(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "state.json")
	p := New(fp)
	defer p.Close()
	a := &auth.Auth{UID: "u", AccessToken: "fixture", FilePath: filepath.Join(dir, "credential.json")}
	if err := a.SaveAtomic(); err != nil {
		t.Fatal(err)
	}
	p.Add(a)
	if err := os.Mkdir(fp+".tmp", 0700); err != nil {
		t.Fatal(err)
	}
	if err := p.SetDisabled("u", true); err == nil {
		t.Fatal("failed status persistence claimed success")
	}
	if st, _ := p.Status("u"); st.Disabled {
		t.Fatal("failed toggle leaked into visible state")
	}
	if err := p.RemoveCredential("u", a); err == nil {
		t.Fatal("failed removal persistence claimed success")
	}
	if p.AuthByUID("u") != a || a.Deleted() {
		t.Fatal("failed removal lost registration")
	}
	if _, err := os.Stat(a.FilePath); err != nil {
		t.Fatal("failed removal lost file", err)
	}
}

func TestAuthFileRemoveAcrossReloadAndStaleScan(t *testing.T) {
	p := New("")
	dir := t.TempDir()
	old := &auth.Auth{UID: "u", AccessToken: "old", FilePath: filepath.Join(dir, "credential.json")}
	if err := old.SaveAtomic(); err != nil {
		t.Fatal(err)
	}
	p.Add(old)
	reloaded := &auth.Auth{UID: "u", AccessToken: "new", FilePath: old.FilePath}
	p.SyncToDir([]*auth.Auth{reloaded})
	if err := p.RemoveCredential("u", reloaded); err != nil {
		t.Fatal(err)
	}
	if !old.Deleted() || old.SaveAtomic() == nil {
		t.Fatal("writer from earlier directory scan recreated credential")
	}
	p.SyncToDir([]*auth.Auth{old, reloaded})
	if p.AuthByUID("u") != nil {
		t.Fatal("stale scan resurrected account")
	}
	fresh := &auth.Auth{UID: "u", AccessToken: "new-login", FilePath: old.FilePath}
	if err := fresh.SaveAtomic(); err != nil {
		t.Fatal(err)
	}
	p.Add(fresh)
	if p.AuthByUID("u") != fresh || fresh.Deleted() {
		t.Fatal("explicit new login could not re-register account")
	}
	if old.SaveAtomic() == nil {
		t.Fatal("new login revived stale writer")
	}
}
