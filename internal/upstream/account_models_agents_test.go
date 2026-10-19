package upstream

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
)

// Reproduces the verified schema: no model type field, craft/marketing tags,
// explicit conversational agent assignments, and image tags. No name guessing.
func accountLiveSchemaFixture(enterprise bool) []byte {
	count := 21
	if enterprise {
		count = 31
	}
	models := make([]map[string]any, 0, count+1)
	for i := 0; i < count; i++ {
		models = append(models, map[string]any{"id": fmt.Sprintf("m-%02d", i), "name": fmt.Sprintf("Display %d", i), "tags": []string{"craft"}})
	}
	models = append(models, map[string]any{"id": "image", "tags": []string{"text-to-image"}})
	ids := func(start, end int) []string {
		out := []string{}
		for i := start; i < end; i++ {
			out = append(out, fmt.Sprintf("m-%02d", i))
		}
		return out
	}
	agents := []map[string]any{}
	if enterprise {
		agents = append(agents, map[string]any{"name": "cli", "models": append(ids(10, 27), "image")})
	} else {
		for _, name := range []string{"craft", "ask", "plan"} {
			agents = append(agents, map[string]any{"name": name, "models": ids(0, 15)})
		}
		agents = append(agents, map[string]any{"name": "chat", "models": ids(15, 17)}, map[string]any{"name": "InlineChat", "models": ids(17, 19)}, map[string]any{"name": "inline-chat", "models": ids(18, 20)})
	}
	raw, _ := json.Marshal(map[string]any{"code": 0, "data": map[string]any{"models": models, "agents": agents}})
	return raw
}

func TestAccountModelsVerifiedSchema22And32WithAgents(t *testing.T) {
	for _, enterprise := range []bool{false, true} {
		models, err := parseAccountCatalog(accountLiveSchemaFixture(enterprise))
		if err != nil {
			t.Fatal(err)
		}
		counts := map[string]int{}
		for _, m := range models {
			counts[m.Type]++
		}
		if !enterprise && (len(models) != 22 || counts["chat"] != 20 || counts["other"] != 1 || counts["image"] != 1) {
			t.Fatal("v3 agent association lost", counts)
		}
		if enterprise && (len(models) != 32 || counts["chat"] != 17 || counts["other"] != 14 || counts["image"] != 1) {
			t.Fatal("enterprise agent association lost", counts)
		}
	}
	c := New()
	c.HTTP = &http.Client{Transport: accountProbeTransport(func(r *http.Request) (*http.Response, error) {
		return accountProbeResponse(200, string(accountLiveSchemaFixture(strings.HasSuffix(r.URL.Path, "/models")))), nil
	})}
	got, err := c.ProbeAccountModels(context.Background(), &auth.Auth{UID: "exact-uid", AccessToken: "fixture-token"})
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, m := range got {
		counts[m.Type]++
	}
	if len(got) != 32 || counts["chat"] != 27 || counts["other"] != 4 || counts["image"] != 1 {
		t.Fatal("merged full catalog/agent mapping", counts)
	}
}

func TestAccountModelsEnvelopeAgentsSuffixAndMediaPriority(t *testing.T) {
	raw := `{"code":0,"agents":[{"name":"InlineChat","models":["suffix","image","video","future"]},{"name":"not-conversational","models":["unknown"]},{"name":"cli","models":["global:wrong-realm"]}],"data":{"models":[{"id":"cn:suffix"},{"id":"image","type":"chat","tags":["text-to-image"]},{"id":"video","tags":["image-to-video"]},{"id":"future","type":"future"},{"id":"unknown","maxOutputTokens":32000},{"id":"cn:wrong-realm"}]}}`
	got, err := parseAccountCatalog([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"cn:suffix": "chat", "image": "image", "video": "video", "future": "other", "unknown": "other", "cn:wrong-realm": "other"}
	for _, m := range got {
		if m.Type != want[m.ID] {
			t.Fatalf("type=%s id=%s want %s", m.Type, m.ID, want[m.ID])
		}
	}
}
