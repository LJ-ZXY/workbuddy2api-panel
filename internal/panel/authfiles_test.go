package panel

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/accountmodels"
	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
	"github.com/linguo2625469/workbuddy2api-panel/internal/pool"
)

func authFileRequest(p *Panel, method, uid, action, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "/panel/api/accounts/"+uid+"/"+action, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer panel-test-key")
	w := httptest.NewRecorder()
	p.ServeHTTP(w, r)
	return w
}

func addAuthFile(t *testing.T, p *Panel, uid string) *auth.Auth {
	t.Helper()
	a := addModelAccount(p, uid, "fixture-old-access", "cn", "")
	a.FilePath = filepath.Join(t.TempDir(), "workbuddy-"+uid+".json")
	if err := a.SaveAtomic(); err != nil {
		t.Fatal(err)
	}
	return a
}

func TestAuthFileRefreshRotationAndPreservesState(t *testing.T) {
	var calls atomic.Int64
	p := newAccountModelPanel(t, func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		if r.URL.Path != "/v2/plugin/auth/token/refresh" || r.Header.Get("X-Auth-Refresh-Source") != "plugin" || r.Header.Get("X-Refresh-Token") != "never-use-refresh" {
			t.Error("wrong credential refresh request", r.URL.Path)
		}
		return modelHTTPResponse(200, `{"code":0,"data":{"accessToken":"fixture-new-access","refreshToken":"fixture-new-refresh","expiresIn":3600}}`), nil
	})
	a := addAuthFile(t, p, "u")
	p.cfg.Pool.Cooldown("u", pool.CoolSoft, time.Hour, "account cooling")
	p.cfg.Pool.CooldownSoftForModel("u", time.Minute, time.Now().Add(time.Hour), "m", "6004 model limit")
	p.cfg.Pool.Pause("u")
	if err := p.cfg.Pool.SetDisabled("u", true); err != nil {
		t.Fatal(err)
	}
	before, _ := p.cfg.Pool.Status("u")
	w := authFileRequest(p, "POST", "u", "refresh", "")
	if w.Code != 200 || strings.Contains(w.Body.String(), "fixture-") || calls.Load() != 1 {
		t.Fatal(w.Code, w.Body.String(), calls.Load())
	}
	raw, err := os.ReadFile(a.FilePath)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := auth.Parse(raw)
	if err != nil || saved.AccessTokenValue() != "fixture-new-access" || saved.RefreshTokenValue() != "fixture-new-refresh" {
		t.Fatal("rotation not persisted", err)
	}
	after, _ := p.cfg.Pool.Status("u")
	before.CoolRemaining, after.CoolRemaining = 0, 0
	for i := range before.RateLimitedModels {
		before.RateLimitedModels[i].RemainingSeconds = 0
		after.RateLimitedModels[i].RemainingSeconds = 0
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("refresh changed routing state: before=%+v after=%+v", before, after)
	}
	if _, err := os.Stat(accountmodels.PathForState(p.cfg.StateFile)); err == nil {
		t.Fatal("refresh published a model catalog")
	}
}

func TestAuthFileRefreshErrorsAreSecretSafe(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response string
		saveFail bool
		status   int
	}{
		{"upstream_12153", `{"code":12153,"msg":"fixture-secret-access fixture-secret-refresh"}`, false, 409},
		{"upstream_generic", `{"code":11100,"msg":"fixture-secret-access generic-error"}`, false, 502},
		{"save", `{"code":0,"data":{"accessToken":"fixture-new-access","refreshToken":"fixture-new-refresh"}}`, true, 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newAccountModelPanel(t, func(*http.Request) (*http.Response, error) { return modelHTTPResponse(200, tc.response), nil })
			a := addAuthFile(t, p, "u")
			before, _ := os.ReadFile(a.FilePath)
			if tc.saveFail {
				if err := os.Mkdir(a.FilePath+".tmp", 0700); err != nil {
					t.Fatal(err)
				}
			}
			w := authFileRequest(p, "POST", "u", "refresh", "")
			if w.Code != tc.status || strings.Contains(w.Body.String(), "fixture-") {
				t.Fatal(w.Code, w.Body.String())
			}
			after, _ := os.ReadFile(a.FilePath)
			if string(after) != string(before) {
				t.Fatal("failed operation altered credential file")
			}
		})
	}
}

func TestAuthFileDownloadExactBytesAndAuthGate(t *testing.T) {
	p := newAccountModelPanel(t, func(*http.Request) (*http.Response, error) { t.Fatal("download called upstream"); return nil, nil })
	a := addAuthFile(t, p, "u")
	raw := []byte("{\n  \"custom\": \"preserve byte format\", \"account\": {\"uid\":\"u\"}\n}\n")
	if err := os.WriteFile(a.FilePath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	w := authFileRequest(p, "GET", "u", "download", "")
	if w.Code != 200 || w.Body.String() != string(raw) || w.Header().Get("Cache-Control") != "no-store" || !strings.HasPrefix(w.Header().Get("Content-Disposition"), "attachment;") {
		t.Fatal(w.Code, w.Header(), w.Body.String())
	}
	for _, path := range []string{"/panel/api/accounts/u/download", "/panel/api/accounts/u/refresh", "/panel/api/accounts/u/status", "/panel/api/accounts/u/remove"} {
		method := "POST"
		if strings.HasSuffix(path, "download") {
			method = "GET"
		}
		if strings.HasSuffix(path, "status") {
			method = "PATCH"
		}
		unauth := httptest.NewRecorder()
		p.ServeHTTP(unauth, httptest.NewRequest(method, path, strings.NewReader(`{"disabled":true}`)))
		if unauth.Code != 401 {
			t.Fatal("missing auth gate", path, unauth.Code)
		}
	}
	for _, action := range []string{"download", "refresh", "remove"} {
		method := "POST"
		if action == "download" {
			method = "GET"
		}
		if got := authFileRequest(p, method, "not-registered", action, ""); got.Code != 404 {
			t.Fatal(action, got.Code)
		}
	}
	if w := modelRequest(p, context.Background(), "GET", "/panel/api/accounts/..%2F..%2Fsecret/download"); w.Code == 200 {
		t.Fatal("path injected account accepted")
	}
}

func TestAuthFileOverviewCapabilitiesAndEmptyRateLimits(t *testing.T) {
	p := newAccountModelPanel(t, nil)
	addAuthFile(t, p, "u")
	w := modelRequest(p, context.Background(), "GET", "/panel/api/overview")
	var body struct {
		AuthFilesSupported      bool `json:"auth_files_supported"`
		ModelCooldownsSupported bool `json:"model_cooldowns_supported"`
		Accounts                []struct {
			RateLimitedModels json.RawMessage `json:"rate_limited_models"`
		} `json:"accounts"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || !body.AuthFilesSupported || !body.ModelCooldownsSupported || len(body.Accounts) != 1 || string(body.Accounts[0].RateLimitedModels) != "[]" {
		t.Fatal("missing read-only capability/array contract", w.Code, w.Body.String())
	}
}

func TestAuthFileStatusStrictDesiredState(t *testing.T) {
	p := newAccountModelPanel(t, nil)
	addAuthFile(t, p, "u")
	p.cfg.Pool.Pause("u")
	p.cfg.Pool.CooldownSoftForModel("u", time.Minute, time.Now().Add(time.Hour), "m", "6004")
	for _, body := range []string{`{}`, `{"disabled":null}`, `{"disabled":1}`, `{"disabled":"true"}`, `{"disabled":true,"revive":true}`, `{"disabled":true,"disabled":false}`, `{"disabled":true} {}`, `[]`} {
		w := authFileRequest(p, "PATCH", "u", "status", body)
		if w.Code != 400 {
			t.Fatal("invalid status accepted", body, w.Code)
		}
	}
	for _, disabled := range []bool{true, true, false, false} {
		body, _ := json.Marshal(map[string]bool{"disabled": disabled})
		w := authFileRequest(p, "PATCH", "u", "status", string(body))
		var result struct {
			OK       bool `json:"ok"`
			Disabled bool `json:"disabled"`
		}
		_ = json.Unmarshal(w.Body.Bytes(), &result)
		st, _ := p.cfg.Pool.Status("u")
		if w.Code != 200 || !result.OK || result.Disabled != disabled || st.Disabled != disabled || !st.Paused || len(st.RateLimitedModels) != 1 {
			t.Fatal("status toggle reset state", w.Code, w.Body.String(), st)
		}
	}
}

func TestAuthFileRemoveFailuresRetainAccount(t *testing.T) {
	p := newAccountModelPanel(t, nil)
	a := addAuthFile(t, p, "u")
	if err := os.Remove(a.FilePath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(a.FilePath, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(a.FilePath, "child"), []byte("blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	w := authFileRequest(p, "POST", "u", "remove", "")
	if w.Code != 500 || strings.Contains(w.Body.String(), `"ok":true`) || p.cfg.Pool.AuthByUID("u") != a || a.Deleted() {
		t.Fatal("failed unlink claimed success or lost account", w.Code, w.Body.String())
	}
}

func TestAuthFileRefreshDeleteConcurrencyAndStaleWriters(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	p := newAccountModelPanel(t, func(*http.Request) (*http.Response, error) {
		close(entered)
		<-release
		return modelHTTPResponse(200, `{"code":0,"data":{"accessToken":"fixture-rotated"}}`), nil
	})
	a := addAuthFile(t, p, "u")
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- authFileRequest(p, "POST", "u", "refresh", "") }()
	<-entered
	for _, action := range []string{"refresh", "remove", "download"} {
		method := "POST"
		if action == "download" {
			method = "GET"
		}
		if w := authFileRequest(p, method, "u", action, ""); w.Code != 409 {
			t.Fatal("concurrent operation not rejected", action, w.Code)
		}
	}
	close(release)
	if w := <-done; w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := authFileRequest(p, "POST", "u", "remove", ""); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if err := a.SaveAtomic(); err == nil {
		t.Fatal("stale writer resurrected removed credential")
	}
	if _, err := os.Stat(a.FilePath); !os.IsNotExist(err) {
		t.Fatal("credential recreated", err)
	}
}

func TestAuthFileBackgroundRefreshCompletesAfterDelete(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	p := newAccountModelPanel(t, func(*http.Request) (*http.Response, error) {
		close(entered)
		<-release
		return modelHTTPResponse(200, `{"code":0,"data":{"accessToken":"fixture-late-rotation"}}`), nil
	})
	a := addAuthFile(t, p, "u")
	done := make(chan error, 1)
	go func() {
		if err := p.cfg.Upstream.RefreshToken(a); err != nil {
			done <- err
			return
		}
		done <- a.SaveAtomic()
	}()
	<-entered
	if w := authFileRequest(p, "POST", "u", "remove", ""); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	close(release)
	if err := <-done; err == nil {
		t.Fatal("late refresh saved deleted credentials")
	}
	if _, err := os.Stat(a.FilePath); !os.IsNotExist(err) {
		t.Fatal("late refresh recreated file", err)
	}
}
