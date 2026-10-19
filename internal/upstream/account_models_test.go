package upstream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/accountmodels"
	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
)

type accountProbeTransport func(*http.Request) (*http.Response, error)

func (f accountProbeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func accountProbeResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func TestProbeAccountModelsExactCredentialsAndNoSharedCaches(t *testing.T) {
	var calls atomic.Int64
	c := New()
	c.ChatBaseCN, c.ChatBaseGlobal = "https://cn.example", "https://global.example"
	c.DeviceToken = "shared-device-fallback-must-not-win"
	c.stashGenerationModels("cn", []ModelInfo{{ID: "shared-image", GenerationKind: "image"}})
	c.efforts = map[string]map[string][]string{"cn": {"shared-chat": {"high"}}}
	c.HTTP = &http.Client{Transport: accountProbeTransport(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		realm := r.Header.Get("X-Realm")
		if r.URL.Host != realm+".example" || r.Header.Get("X-User-Id") != "same-uid" || r.Header.Get("X-Refresh-Token") != "" || r.Header.Get("X-CodeBuddy-Request") != "1" {
			t.Error("incorrect identity headers", r.URL.Host)
		}
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		enterprise := r.Header.Get("X-Enterprise-Id")
		if token != realm+"-"+enterprise {
			t.Error("mixed account credentials")
		}
		origin, language := "https://www.codebuddy.cn", "zh-CN"
		if realm == "global" {
			origin, language = "https://www.workbuddy.ai", "en-US"
		}
		if r.Header.Get("Origin") != origin || r.Header.Get("Referer") != origin+"/" || r.Header.Get("Accept-Language") != language ||
			r.Header.Get("X-Machine-ID") != deriveAccountStableID("same-uid", "machine") || r.Header.Get("X-Session-ID") != deriveAccountStableID("same-uid", "session") ||
			r.Header.Get("X-Device-Token") != realm+"-device-"+enterprise || r.Header.Get("Content-Type") != "application/json" {
			t.Error("shared/per-account authentication headers not preserved")
		}
		if enterprise != "" && r.Header.Get("X-No-Enterprise-Id") != "" {
			t.Error("global enterprise incorrectly overridden")
		}
		if enterprise == "" && r.Header.Get("X-No-Enterprise-Id") != "1" {
			t.Error("personal account enterprise declaration missing")
		}
		return accountProbeResponse(200, fmt.Sprintf(`{"code":0,"data":{"models":[{"id":"%s-%s","type":"chat"},{"id":"image","tags":["text-to-image"]},{"id":"video","type":"video"},{"id":"cn:already","type":"unrecognized"},{"id":"disabled","disabled":true}]}}`, realm, enterprise)), nil
	})}
	beforeImages, beforeEfforts := c.GenerationModels("cn"), c.effortsSnapshot("cn")
	for _, tc := range []struct{ realm, enterprise string }{{"cn", ""}, {"cn", "team-A"}, {"cn", "team-B"}, {"global", "team-A"}} {
		a := &auth.Auth{UID: "same-uid", AccessToken: tc.realm + "-" + tc.enterprise, RefreshToken: "do-not-send", EnterpriseID: tc.enterprise, DeviceToken: tc.realm + "-device-" + tc.enterprise}
		if _, err := auth.BackfillRealmFor(a, tc.realm); err != nil {
			t.Fatal(err)
		}
		models, err := c.ProbeAccountModels(context.Background(), a)
		if err != nil || len(models) != 4 {
			t.Fatalf("probe models=%v err=%v", models, err)
		}
		found := map[string]string{}
		for _, m := range models {
			found[m.ID] = m.Type
		}
		if found[tc.realm+":"+tc.realm+"-"+tc.enterprise] != "chat" || found[tc.realm+":image"] != "image" || found[tc.realm+":video"] != "video" || found["cn:already"] != "other" {
			t.Fatalf("wrong catalog: %v", models)
		}
		if a.AccessTokenValue() != tc.realm+"-"+tc.enterprise || a.RefreshToken != "do-not-send" {
			t.Fatal("credential mutation")
		}
	}
	if calls.Load() != 12 {
		t.Fatalf("unexpected probes: %d", calls.Load())
	}
	if !reflect.DeepEqual(beforeImages, c.GenerationModels("cn")) || !reflect.DeepEqual(beforeEfforts, c.effortsSnapshot("cn")) {
		t.Fatal("shared cache changed")
	}
}

func TestAccountCatalogExplicitTypesEmptyAndInvalid(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		want      []accountmodels.Model
		err       error
	}{
		{"empty", `{"code":0,"data":{"models":[]}}`, []accountmodels.Model{}, nil},
		{"all-disabled", `{"models":[{"id":"x","disabled":true},{"id":"y","enabled":false}]}`, []accountmodels.Model{}, nil},
		{"explicit-only", `{"models":[{"id":"chat-looking","maxOutputTokens":32000,"supportsToolCall":true},{"id":"img","tags":["text-to-image"]},{"id":"vid","tags":["image-to-video"]},{"id":"future","type":"future","tags":["chat"]},{"id":"cn:chat","type":"chat","name":"Display"}]}`,
			[]accountmodels.Model{{ID: "chat-looking", Type: "other"}, {ID: "img", Type: "image"}, {ID: "vid", Type: "video"}, {ID: "future", Type: "other"}, {ID: "cn:chat", DisplayName: "Display", Type: "chat"}}, nil},
		{"nested-array", `{"data":{"items":[]}}`, []accountmodels.Model{}, nil},
		{"global-prefix", `[{"id":"global:raw","display_name":"Global","type":"image"}]`, []accountmodels.Model{{ID: "global:raw", DisplayName: "Global", Type: "image"}}, nil},
		{"missing", `{"code":0,"data":{}}`, nil, accountmodels.InvalidResponse},
		{"null", `{"code":0,"data":{"models":null}}`, nil, accountmodels.InvalidResponse},
		{"non-array", `{"models":{}}`, nil, accountmodels.InvalidResponse},
		{"missing-id", `{"models":[{"type":"chat"}]}`, nil, accountmodels.InvalidResponse},
		{"bad-entry", `{"models":[null]}`, nil, accountmodels.InvalidResponse},
		{"bad-json", `{SECRET broken`, nil, accountmodels.InvalidResponse},
		{"bad-enabled", `{"models":[{"id":"x","enabled":"yes"}]}`, nil, accountmodels.InvalidResponse},
		{"rejected", `{"code":10001,"msg":"SECRET"}`, nil, accountmodels.ProbeRejected},
		{"session-dead", `{"code":12153,"msg":"SECRET"}`, nil, accountmodels.InvalidCredentials},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseAccountCatalog([]byte(tc.raw))
			if !errors.Is(err, tc.err) || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got=%+v err=%v want=%+v err=%v", got, err, tc.want, tc.err)
			}
		})
	}
}

func TestProbeAccountModelsPermission403VersusConfirmedInvalid(t *testing.T) {
	for _, tc := range []struct {
		name             string
		enterpriseStatus int
		body             string
		wantErr          error
	}{
		{"resource-permission-403", 403, `{"msg":"SECRET enterprise permission denied"}`, nil},
		{"credential-401", 401, `{"msg":"SECRET expired token"}`, accountmodels.InvalidCredentials},
		{"session-dead-12153", 200, `{"code":12153,"msg":"SECRET offline"}`, accountmodels.InvalidCredentials},
		{"session-dead-12153-non200", 400, `{"code":12153,"msg":"SECRET offline"}`, accountmodels.InvalidCredentials},
		{"403-with-explicit-session-dead-12153", 403, `{"code":12153,"msg":"SECRET offline"}`, accountmodels.InvalidCredentials},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := New()
			c.ChatBaseCN = "https://fixture.example"
			c.HTTP = &http.Client{Transport: accountProbeTransport(func(r *http.Request) (*http.Response, error) {
				if strings.HasSuffix(r.URL.Path, "/models") {
					return accountProbeResponse(tc.enterpriseStatus, tc.body), nil
				}
				return accountProbeResponse(200, `{"models":[{"id":"allowed","type":"chat"}]}`), nil
			})}
			got, err := c.ProbeAccountModels(context.Background(), &auth.Auth{UID: "u", AccessToken: "exact-token"})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err=%v want=%v", err, tc.wantErr)
			}
			if tc.wantErr == nil && (len(got) != 1 || got[0].ID != "cn:allowed") {
				t.Fatalf("403 excluded healthy account: %v", got)
			}
			if err != nil && strings.Contains(err.Error(), "SECRET") {
				t.Fatal("unsafe error")
			}
		})
	}
}

func TestProbeAccountModelsUnionFallbackEmptyAndSafeErrors(t *testing.T) {
	for _, tc := range []struct {
		name      string
		transport accountProbeTransport
		want      int
		err       error
	}{
		{"union", func(r *http.Request) (*http.Response, error) {
			id := "enterprise"
			if r.URL.Path == "/v3/config" {
				if r.Header.Get("User-Agent") == codeBuddyIDEUA {
					id = "ide"
				} else {
					id = "cli"
				}
			}
			return accountProbeResponse(200, fmt.Sprintf(`{"models":[{"id":%q,"type":"other"}]}`, id)), nil
		}, 3, nil},
		{"empty-success", func(*http.Request) (*http.Response, error) { return accountProbeResponse(200, `{"models":[]}`), nil }, 0, nil},
		{"fallback", func(r *http.Request) (*http.Response, error) {
			if r.URL.Path == "/v3/config" {
				return accountProbeResponse(404, `SECRET`), nil
			}
			return accountProbeResponse(200, `{"models":[]}`), nil
		}, 0, nil},
		{"all-fail", func(*http.Request) (*http.Response, error) { return nil, errors.New("SECRET transport token URL") }, 0, accountmodels.ProbeFailed},
		{"all-403", func(*http.Request) (*http.Response, error) { return accountProbeResponse(403, `SECRET`), nil }, 0, accountmodels.ProbeRejected},
		{"invalid-json", func(*http.Request) (*http.Response, error) { return accountProbeResponse(200, `SECRET broken`), nil }, 0, accountmodels.InvalidResponse},
		{"oversize", func(*http.Request) (*http.Response, error) {
			return accountProbeResponse(200, strings.Repeat("x", (2<<20)+1)), nil
		}, 0, accountmodels.InvalidResponse},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := New()
			c.HTTP = &http.Client{Transport: tc.transport}
			got, err := c.ProbeAccountModels(context.Background(), &auth.Auth{UID: "u", AccessToken: "t"})
			if !errors.Is(err, tc.err) || len(got) != tc.want {
				t.Fatalf("got=%v err=%v", got, err)
			}
			if err == nil && got == nil {
				t.Fatal("success missing models array")
			}
		})
	}
}

func TestProbeAccountModelsDeadlineAndNoRedirectCredentials(t *testing.T) {
	c := New()
	c.HTTP = &http.Client{Transport: accountProbeTransport(func(r *http.Request) (*http.Response, error) { <-r.Context().Done(); return nil, r.Context().Err() })}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := c.ProbeAccountModels(ctx, &auth.Auth{UID: "u", AccessToken: "t"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("deadline ignored", err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	if _, err := c.ProbeAccountModels(ctx, &auth.Auth{UID: "u", AccessToken: "t"}); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation ignored", err)
	}
	var received atomic.Int64
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { received.Add(1); w.WriteHeader(200) }))
	defer sink.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, sink.URL, 302) }))
	defer origin.Close()
	c.HTTP = origin.Client()
	c.ChatBaseCN = origin.URL
	if _, err := c.ProbeAccountModels(context.Background(), &auth.Auth{UID: "u", AccessToken: "must-not-leak"}); err != accountmodels.ProbeRejected {
		t.Fatal("redirect not rejected", err)
	}
	if received.Load() != 0 {
		t.Fatal("credentials sent to redirect")
	}
}
