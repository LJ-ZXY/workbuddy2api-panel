package panel

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/accountmodels"
	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
	"github.com/linguo2625469/workbuddy2api-panel/internal/pool"
	"github.com/linguo2625469/workbuddy2api-panel/internal/upstream"
)

func newAccountModelPanel(t *testing.T, handler fakeRoundTripper) *Panel {
	t.Helper()
	up := upstream.New()
	up.ChatBaseCN, up.ChatBaseGlobal = "https://cn.fixture", "https://global.fixture"
	up.HTTP = &http.Client{Transport: handler}
	return New(Config{Pool: pool.New(""), Upstream: up, StateFile: filepath.Join(t.TempDir(), "state.json"), APIKey: "panel-test-key"})
}

func modelHTTPResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}

func modelRequest(p *Panel, ctx context.Context, method, path string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, nil).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer panel-test-key")
	w := httptest.NewRecorder()
	p.ServeHTTP(w, r)
	return w
}

func decodeModelSnapshot(t *testing.T, raw []byte) accountmodels.Snapshot {
	t.Helper()
	var snap accountmodels.Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		t.Fatal(err)
	}
	if snap.Models == nil {
		t.Fatal("models must be an array even when empty", string(raw))
	}
	if snap.Cached && (snap.UpdatedAt == nil || snap.UpdatedAt.IsZero()) {
		t.Fatal("cached snapshot lacks timestamp")
	}
	if !snap.Cached && (snap.UpdatedAt != nil || len(snap.Models) != 0) {
		t.Fatal("unknown snapshot carries models/timestamp")
	}
	return snap
}

func decodeModelBatch(t *testing.T, w *httptest.ResponseRecorder) accountModelBatch {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("batch code=%d body=%s", w.Code, w.Body.String())
	}
	var batch accountModelBatch
	if err := json.Unmarshal(w.Body.Bytes(), &batch); err != nil {
		t.Fatal(err)
	}
	if batch.Accounts == nil || batch.Summary.Total != len(batch.Accounts) || batch.Summary.Total != batch.Summary.Succeeded+batch.Summary.Stale+batch.Summary.Unknown+batch.Summary.Excluded {
		t.Fatal("invalid batch counts", w.Body.String())
	}
	for _, row := range batch.Accounts {
		if row.Realm != "cn" && row.Realm != "global" {
			t.Fatal("invalid realm", row)
		}
		if row.Status == "fresh" || row.Status == "stale" {
			if !row.Cached || row.UpdatedAt == nil {
				t.Fatal("missing valid cache", row)
			}
		}
		if row.Status == "unknown" && (row.Cached || row.UpdatedAt != nil || row.Models == nil || len(row.Models) != 0) {
			t.Fatal("bad unknown", row)
		}
	}
	return batch
}

func addModelAccount(p *Panel, uid, token, realm, enterprise string) *auth.Auth {
	a := &auth.Auth{UID: uid, AccessToken: token, RefreshToken: "never-use-refresh", EnterpriseID: enterprise, ExpiresAt: 1}
	_, _ = auth.BackfillRealmFor(a, realm)
	p.cfg.Pool.Add(a)
	return a
}

func TestAccountModelsGETSnapshotOnlyAndExactUIDRefresh(t *testing.T) {
	var calls atomic.Int64
	p := newAccountModelPanel(t, func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer wanted-token" || r.Header.Get("X-User-Id") != "wanted" || r.Header.Get("X-Realm") != "global" || r.Header.Get("X-Enterprise-Id") != "team" || r.URL.Host != "global.fixture" || r.Header.Get("X-Refresh-Token") != "" {
			t.Error("wrong probe credentials")
		}
		if r.URL.Path != "/v3/config" && r.URL.Path != "/v2/enterprises/personal/models" {
			t.Error("unexpected upstream route", r.URL.Path)
		}
		return modelHTTPResponse(200, `{"code":0,"data":{"models":[{"id":"dialogue","name":"Dialogue"},{"id":"image","tags":["text-to-image"]},{"id":"video","type":"video"},{"id":"unknown"}],"agents":[{"name":"cli","models":["dialogue","image"]}]}}`), nil
	})
	wanted := addModelAccount(p, "wanted", "wanted-token", "global", "team")
	addModelAccount(p, "random", "random-token", "cn", "")
	get := modelRequest(p, context.Background(), "GET", "/panel/api/accounts/wanted/models")
	if get.Code != 200 || decodeModelSnapshot(t, get.Body.Bytes()).Cached || calls.Load() != 0 {
		t.Fatal("GET probed upstream", get.Body.String())
	}
	missing := modelRequest(p, context.Background(), "GET", "/panel/api/accounts/missing/models")
	if missing.Code != 404 {
		t.Fatal("missing UID not 404")
	}
	unauth := httptest.NewRecorder()
	p.ServeHTTP(unauth, httptest.NewRequest("GET", "/panel/api/accounts/wanted/models", nil))
	if unauth.Code != 401 {
		t.Fatal("missing auth gate")
	}
	post := modelRequest(p, context.Background(), "POST", "/panel/api/accounts/wanted/models/refresh")
	if post.Code != 200 {
		t.Fatal(post.Code, post.Body.String())
	}
	snap := decodeModelSnapshot(t, post.Body.Bytes())
	if !snap.Cached || snap.Stale || len(snap.Models) != 4 || calls.Load() != 3 {
		t.Fatal("incomplete snapshot", post.Body.String())
	}
	if wanted.AccessTokenValue() != "wanted-token" || wanted.RefreshToken != "never-use-refresh" || wanted.ExpiresAt != 1 {
		t.Fatal("credentials refreshed/mutated")
	}
	// Reload from disk at startup, then corrupt disk: GET remains memory-only.
	restarted := New(p.cfg)
	path := accountmodels.PathForState(p.cfg.StateFile)
	if err := os.WriteFile(path, []byte("SECRET corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	get = modelRequest(restarted, context.Background(), "GET", "/panel/api/accounts/wanted/models")
	if !reflect.DeepEqual(snap, decodeModelSnapshot(t, get.Body.Bytes())) || calls.Load() != 3 {
		t.Fatal("GET used disk/upstream")
	}
	unknown := decodeModelSnapshot(t, modelRequest(p, context.Background(), "GET", "/panel/api/accounts/random/models").Body.Bytes())
	if unknown.Cached {
		t.Fatal("cross-account cache leak")
	}
	// Reusing a UID with another realm/enterprise does not reuse the old result.
	addModelAccount(restarted, "wanted", "different-token", "cn", "other-team")
	changed := decodeModelSnapshot(t, modelRequest(restarted, context.Background(), "GET", "/panel/api/accounts/wanted/models").Body.Bytes())
	if changed.Cached || changed.Realm != "cn" {
		t.Fatal("identity mismatch cache leak")
	}
}

func TestAccountModelsEmptySuccessFailureSnapshotAndPersistenceError(t *testing.T) {
	var fail atomic.Bool
	p := newAccountModelPanel(t, func(*http.Request) (*http.Response, error) {
		if fail.Load() {
			return modelHTTPResponse(500, `{"msg":"SECRET wanted-token"}`), nil
		}
		return modelHTTPResponse(200, `{"models":[]}`), nil
	})
	addModelAccount(p, "u", "wanted-token", "cn", "")
	first := modelRequest(p, context.Background(), "POST", "/panel/api/accounts/u/models/refresh")
	if first.Code != 200 {
		t.Fatal(first.Code, first.Body.String())
	}
	snap := decodeModelSnapshot(t, first.Body.Bytes())
	if !snap.Cached || len(snap.Models) != 0 {
		t.Fatal("empty success not valid")
	}
	fail.Store(true)
	failed := modelRequest(p, context.Background(), "POST", "/panel/api/accounts/u/models/refresh")
	var body struct {
		Detail   string                 `json:"detail"`
		Snapshot accountmodels.Snapshot `json:"snapshot"`
	}
	if failed.Code != 502 || json.Unmarshal(failed.Body.Bytes(), &body) != nil || body.Detail != "model_probe_rejected" || !body.Snapshot.Cached || !body.Snapshot.Stale || !body.Snapshot.UpdatedAt.Equal(*snap.UpdatedAt) || strings.Contains(failed.Body.String(), "SECRET") {
		t.Fatal("unsafe/invalid failure", failed.Body.String())
	}
	reloaded := New(p.cfg)
	got := decodeModelSnapshot(t, modelRequest(reloaded, context.Background(), "GET", "/panel/api/accounts/u/models").Body.Bytes())
	if !got.Stale || got.LastError == nil {
		t.Fatal("failure metadata not persisted")
	}
	fail.Store(false)
	path := accountmodels.PathForState(p.cfg.StateFile)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	failed = modelRequest(p, context.Background(), "POST", "/panel/api/accounts/u/models/refresh")
	if failed.Code != 502 || !strings.Contains(failed.Body.String(), "model_snapshot_write_failed") {
		t.Fatal("disk error not reported", failed.Body.String())
	}
	got = decodeModelSnapshot(t, modelRequest(p, context.Background(), "GET", "/panel/api/accounts/u/models").Body.Bytes())
	if !got.UpdatedAt.Equal(*snap.UpdatedAt) {
		t.Fatal("disk failure changed memory")
	}
}

func TestAccountModelsBatchMixedEligibilityAndNoPoolChanges(t *testing.T) {
	var called sync.Map
	p := newAccountModelPanel(t, func(r *http.Request) (*http.Response, error) {
		uid := r.Header.Get("X-User-Id")
		called.Store(uid, true)
		if uid == "cool" || uid == "disabled" || uid == "invalid" {
			t.Error("excluded/cooling account probed", uid)
		}
		if r.Header.Get("Authorization") != "Bearer token-"+uid {
			t.Error("wrong account token")
		}
		if uid == "stale" || uid == "unknown" {
			return modelHTTPResponse(500, "SECRET raw upstream body"), nil
		}
		if uid == "global" && (r.Header.Get("X-Realm") != "global" || r.Header.Get("X-Enterprise-Id") != "company") {
			t.Error("wrong enterprise/realm")
		}
		return modelHTTPResponse(200, `{"models":[]}`), nil
	})
	for _, uid := range []string{"fresh", "empty", "stale", "unknown", "cool", "disabled", "invalid", "global"} {
		token, realm, enterprise := "token-"+uid, "cn", ""
		if uid == "invalid" {
			token = ""
		}
		if uid == "global" {
			realm, enterprise = "global", "company"
		}
		addModelAccount(p, uid, token, realm, enterprise)
	}
	for _, uid := range []string{"stale", "cool"} {
		if _, err := p.accountModels.Commit(context.Background(), accountmodels.Identity{UID: uid, Realm: "cn"}, []accountmodels.Model{{ID: "cn:cached", Type: "chat"}}, nil); err != nil {
			t.Fatal(err)
		}
	}
	p.cfg.Pool.Cooldown("cool", pool.CoolSoft, time.Hour, "existing cooling")
	p.cfg.Pool.Disable("disabled", "already disabled")
	batch := decodeModelBatch(t, modelRequest(p, context.Background(), "POST", "/panel/api/models/refresh"))
	if batch.Summary.Total != 8 || batch.Summary.Succeeded != 3 || batch.Summary.Stale != 2 || batch.Summary.Unknown != 1 || batch.Summary.Excluded != 2 || !batch.Partial {
		t.Fatal("wrong counts", batch.Summary)
	}
	for _, st := range p.cfg.Pool.List() {
		if st.ErrTotal != 0 || st.SuccessCount != 0 || st.ConsecutiveFails != 0 || st.InFlight != 0 {
			t.Fatal("probe penalized/scheduled pool", st.UID)
		}
	}
	if _, ok := called.Load("cool"); ok {
		t.Fatal("cooling cache not retained")
	}
}

func TestAccountModelsBatchAllFailAndNoEligibleStill200(t *testing.T) {
	p := newAccountModelPanel(t, func(*http.Request) (*http.Response, error) { return modelHTTPResponse(503, "SECRET"), nil })
	addModelAccount(p, "cached", "t", "cn", "")
	addModelAccount(p, "unknown", "t", "cn", "")
	_, _ = p.accountModels.Commit(context.Background(), accountmodels.Identity{UID: "cached", Realm: "cn"}, []accountmodels.Model{{ID: "cn:old", Type: "chat"}}, nil)
	batch := decodeModelBatch(t, modelRequest(p, context.Background(), "POST", "/panel/api/models/refresh"))
	if batch.Summary.Succeeded != 0 || batch.Summary.Stale != 1 || batch.Summary.Unknown != 1 || !batch.Partial {
		t.Fatal("all-fail batch lost", batch)
	}
	p.cfg.Pool.Disable("cached", "confirmed invalid")
	p.cfg.Pool.Disable("unknown", "disabled")
	batch = decodeModelBatch(t, modelRequest(p, context.Background(), "POST", "/panel/api/models/refresh"))
	if batch.Summary.Succeeded != 0 || batch.Summary.Excluded != 2 || batch.Partial {
		t.Fatal("excluded-only round partial/counts", batch)
	}
	p.cfg.Pool.Remove("cached")
	p.cfg.Pool.Remove("unknown")
	batch = decodeModelBatch(t, modelRequest(p, context.Background(), "POST", "/panel/api/models/refresh"))
	if batch.Summary.Total != 0 || batch.Accounts == nil || batch.Partial {
		t.Fatal("empty batch", batch)
	}
}

func TestAccountModelsBatchConcurrencyAndDeadlines(t *testing.T) {
	var active, maximum, calls atomic.Int64
	ready, release := make(chan struct{}, 3), make(chan struct{})
	p := newAccountModelPanel(t, func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		n := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); n > old; old = maximum.Load() {
			if maximum.CompareAndSwap(old, n) {
				break
			}
		}
		deadline, ok := r.Context().Deadline()
		if !ok || time.Until(deadline) > 15*time.Second {
			t.Error("no 15s account deadline")
		}
		if len(ready) < 3 {
			select {
			case ready <- struct{}{}:
			default:
			}
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
		return modelHTTPResponse(200, `{"models":[]}`), nil
	})
	for i := 0; i < 7; i++ {
		addModelAccount(p, fmt.Sprint(i), "t", "cn", "")
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- modelRequest(p, context.Background(), "POST", "/panel/api/models/refresh") }()
	for i := 0; i < 3; i++ {
		select {
		case <-ready:
		case <-time.After(time.Second):
			t.Fatal("three workers not started")
		}
	}
	if maximum.Load() != 3 {
		t.Fatal("concurrency not 3", maximum.Load())
	}
	close(release)
	batch := decodeModelBatch(t, <-done)
	if batch.Summary.Succeeded != 7 || maximum.Load() > 3 || calls.Load() != 21 {
		t.Fatal("concurrency/probe counts", batch.Summary, calls.Load(), maximum.Load())
	}
	var timedCalls atomic.Int64
	p.cfg.Upstream.HTTP = &http.Client{Transport: fakeRoundTripper(func(r *http.Request) (*http.Response, error) {
		timedCalls.Add(1)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	batch = decodeModelBatch(t, modelRequest(p, ctx, "POST", "/panel/api/models/refresh"))
	if time.Since(start) > time.Second || batch.Summary.Succeeded != 0 || batch.Summary.Stale != 7 || timedCalls.Load() > 3 {
		t.Fatal("round deadline ignored", batch.Summary, timedCalls.Load())
	}
	if accountModelRoundTimeout != 60*time.Second || accountModelTimeout != 15*time.Second || accountModelConcurrency != 3 {
		t.Fatal("incorrect limits")
	}
}

func TestAccountModelsIndividualAndBatchShareUIDLock(t *testing.T) {
	var calls atomic.Int64
	entered, release := make(chan struct{}), make(chan struct{})
	p := newAccountModelPanel(t, func(r *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		}
		return modelHTTPResponse(200, `{"models":[]}`), nil
	})
	addModelAccount(p, "same", "t", "cn", "")
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- modelRequest(p, context.Background(), "POST", "/panel/api/accounts/same/models/refresh")
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	batch := decodeModelBatch(t, modelRequest(p, ctx, "POST", "/panel/api/models/refresh"))
	if calls.Load() != 1 || batch.Summary.Unknown != 1 || *batch.Accounts[0].Error != "model_probe_timeout" {
		t.Fatal("batch bypassed individual lock", batch)
	}
	close(release)
	if (<-done).Code != 200 {
		t.Fatal("individual failed")
	}
	batch = decodeModelBatch(t, modelRequest(p, context.Background(), "POST", "/panel/api/models/refresh"))
	if batch.Summary.Succeeded != 1 || calls.Load() != 6 {
		t.Fatal("lock not released")
	}
}

func TestAccountModelsCredential401ExcludedUntilTokenChangesNot403(t *testing.T) {
	var calls atomic.Int64
	p := newAccountModelPanel(t, func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		if r.Header.Get("Authorization") == "Bearer bad" {
			return modelHTTPResponse(401, "SECRET"), nil
		}
		if strings.HasSuffix(r.URL.Path, "/models") {
			return modelHTTPResponse(403, "SECRET permissions"), nil
		}
		return modelHTTPResponse(200, `{"models":[],"agents":[]}`), nil
	})
	a := addModelAccount(p, "u", "bad", "cn", "")
	first := decodeModelBatch(t, modelRequest(p, context.Background(), "POST", "/panel/api/models/refresh"))
	if first.Summary.Excluded != 1 || calls.Load() != 1 || first.Partial {
		t.Fatal("confirmed invalid handling", first)
	}
	second := decodeModelBatch(t, modelRequest(p, context.Background(), "POST", "/panel/api/models/refresh"))
	if second.Summary.Excluded != 1 || calls.Load() != 1 {
		t.Fatal("invalid credential probed again")
	}
	a.Lock()
	a.AccessToken = "good"
	a.Unlock()
	third := decodeModelBatch(t, modelRequest(p, context.Background(), "POST", "/panel/api/models/refresh"))
	if third.Summary.Succeeded != 1 || third.Partial || calls.Load() != 4 {
		t.Fatal("403 excluded valid token", third)
	}
	st, _ := p.cfg.Pool.Status("u")
	if st.Disabled || st.ErrTotal != 0 {
		t.Fatal("probe mutated pool")
	}
}

func TestAccountModelsDeletedOrReplacedInFlightNeverPublished(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(fmt.Sprint(replace), func(t *testing.T) {
			var p *Panel
			var once sync.Once
			p = newAccountModelPanel(t, func(*http.Request) (*http.Response, error) {
				once.Do(func() {
					if replace {
						addModelAccount(p, "u", "new-token", "global", "new-team")
					} else {
						p.cfg.Pool.Remove("u")
					}
				})
				return modelHTTPResponse(200, `{"models":[{"id":"old-result","type":"chat"}]}`), nil
			})
			addModelAccount(p, "u", "old-token", "cn", "old-team")
			w := modelRequest(p, context.Background(), "POST", "/panel/api/accounts/u/models/refresh")
			if w.Code != 502 {
				t.Fatal("published old identity", w.Body.String())
			}
			if p.accountModels.Snapshot(accountmodels.Identity{UID: "u", Realm: "cn", EnterpriseID: "old-team"}).Cached {
				t.Fatal("old probe persisted")
			}
			get := modelRequest(p, context.Background(), "GET", "/panel/api/accounts/u/models")
			if replace {
				if decodeModelSnapshot(t, get.Body.Bytes()).Cached {
					t.Fatal("replacement cache leaked")
				}
			} else if get.Code != 404 {
				t.Fatal("deleted UID not 404")
			}
		})
	}
}

func TestAccountModelsBatchRechecksAlreadyCompletedAccount(t *testing.T) {
	for _, change := range []string{"delete", "disable", "enterprise", "realm", "reject", "cool"} {
		t.Run(change, func(t *testing.T) {
			blocked, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			p := newAccountModelPanel(t, func(r *http.Request) (*http.Response, error) {
				if r.Header.Get("X-User-Id") == "b" {
					once.Do(func() { close(blocked) })
					select {
					case <-release:
					case <-r.Context().Done():
						return nil, r.Context().Err()
					}
				}
				return modelHTTPResponse(200, `{"models":[{"id":"only-this-account","type":"chat"}]}`), nil
			})
			addModelAccount(p, "a", "token-a", "cn", "")
			addModelAccount(p, "b", "token-b", "cn", "")
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() { done <- modelRequest(p, context.Background(), "POST", "/panel/api/models/refresh") }()
			select {
			case <-blocked:
			case <-time.After(time.Second):
				t.Fatal("second account not blocked")
			}
			id := accountmodels.Identity{UID: "a", Realm: "cn"}
			deadline := time.After(time.Second)
			for !p.accountModels.Snapshot(id).Cached {
				select {
				case <-deadline:
					t.Fatal("first account did not complete before mutation")
				case <-time.After(time.Millisecond):
				}
			}
			switch change {
			case "delete":
				p.cfg.Pool.Remove("a")
			case "disable":
				p.cfg.Pool.Disable("a", "disabled after probe completed")
			case "enterprise":
				addModelAccount(p, "a", "token-a", "cn", "changed-team")
			case "realm":
				addModelAccount(p, "a", "token-a", "global", "")
			case "reject":
				p.rejectModelCredential(id, "token-a")
			case "cool":
				p.cfg.Pool.Cooldown("a", pool.CoolSoft, time.Hour, "cooling after probe completed")
			}
			close(release)
			batch := decodeModelBatch(t, <-done)
			if batch.Summary.Succeeded != 1 || batch.Accounts[1].Status != "fresh" {
				t.Fatal("second account failed", batch.Summary)
			}
			if change == "cool" {
				if batch.Accounts[0].Status != "stale" || !batch.Accounts[0].Cached || !batch.Accounts[0].Stale || batch.Summary.Stale != 1 || !batch.Partial {
					t.Fatal("cooling snapshot not retained stale", batch.Accounts[0])
				}
			} else {
				if batch.Accounts[0].Status != "excluded" || batch.Summary.Excluded != 1 || batch.Partial {
					t.Fatal("ineligible completed result published", change, batch.Accounts[0])
				}
			}
			if change == "delete" && len(batch.Accounts[0].Models) != 0 {
				t.Fatal("deleted account model list exposed")
			}
			if change == "enterprise" && batch.Accounts[0].Cached {
				t.Fatal("enterprise identity reused cached result")
			}
		})
	}
}
