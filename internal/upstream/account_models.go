package upstream

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"

	"github.com/linguo2625469/workbuddy2api-panel/internal/accountmodels"
	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
)

// ProbeAccountModels probes only the supplied account, with no refresh, pool
// selection, logging of upstream text, or reads/writes of shared catalog caches.
// Both explicit UA catalogs and the enterprise catalog contribute enabled models.
func (c *Client) ProbeAccountModels(ctx context.Context, a *auth.Auth) ([]accountmodels.Model, error) {
	a = a.ModelProbeSnapshot()
	if a == nil || a.UID == "" || a.AccessToken == "" {
		return nil, accountmodels.InvalidCredentials
	}
	if c == nil || c.HTTP == nil {
		return nil, accountmodels.ProbeFailed
	}
	realm := a.RealmStored()
	base, enterprisePath := c.ChatBaseCN, "/console/enterprises/personal/models"
	if realm == "global" {
		base, enterprisePath = c.globalChatBase(), "/v2/enterprises/personal/models"
	}
	if realm != "cn" && realm != "global" {
		return nil, accountmodels.InvalidCredentials
	}
	probes := []struct{ path, ua string }{
		{"/v3/config", codeBuddyIDEUA}, {"/v3/config", codeBuddyCLIUA}, {enterprisePath, codeBuddyCLIUA},
	}
	byID := make(map[string]accountmodels.Model)
	succeeded := false
	var lastErr error = accountmodels.ProbeFailed
	for _, probe := range probes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		models, err := c.probeAccountCatalog(ctx, a, base, probe.path, probe.ua)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			// Never convert a confirmed credential rejection into success from
			// another endpoint. Other endpoint failures permit authoritative fallback.
			if err == accountmodels.InvalidCredentials {
				return nil, err
			}
			lastErr = err
			continue
		}
		succeeded = true
		for _, model := range models {
			if !strings.HasPrefix(model.ID, "cn:") && !strings.HasPrefix(model.ID, "global:") {
				model.ID = realm + ":" + model.ID
			}
			if old, ok := byID[model.ID]; !ok {
				byID[model.ID] = model
			} else {
				if old.DisplayName == "" {
					old.DisplayName = model.DisplayName
				}
				if old.Type == "other" || ((model.Type == "image" || model.Type == "video") && old.Type == "chat") {
					old.Type = model.Type
				}
				byID[model.ID] = old
			}
		}
	}
	if !succeeded {
		return nil, lastErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]accountmodels.Model, 0, len(byID))
	for _, model := range byID {
		out = append(out, model)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (c *Client) probeAccountCatalog(ctx context.Context, a *auth.Auth, base, path, ua string) ([]accountmodels.Model, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(base, "/")+path, nil)
	if err != nil {
		return nil, accountmodels.ProbeFailed
	}
	c.CommonHeaders(req, a)
	c.injectDeviceToken(req, a)
	// Catalog headers must not apply global chat's "no enterprise" override.
	req.Header.Set("Authorization", "Bearer "+a.AccessToken)
	req.Header.Set("X-User-Id", a.UID)
	req.Header.Set("X-Realm", a.RealmStored())
	if a.EnterpriseID != "" {
		req.Header.Set("X-Enterprise-Id", a.EnterpriseID)
	} else {
		req.Header.Set("X-No-Enterprise-Id", "1")
	}
	req.Header.Set("X-Domain", v3ConfigDomain(a, base))
	req.Header.Set("X-Product", "SaaS")
	req.Header.Set("X-CodeBuddy-Request", "1")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", ua)
	// Do not forward account credentials on redirects, including same-host ones.
	client := *c.HTTP
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		var timeout net.Error
		if errors.As(err, &timeout) && timeout.Timeout() {
			return nil, accountmodels.ProbeTimeout
		}
		return nil, accountmodels.ProbeFailed
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		return nil, accountmodels.InvalidCredentials
	}
	const maxBody = 2 << 20
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return nil, accountmodels.ProbeFailed
	}
	if len(raw) > maxBody {
		return nil, accountmodels.InvalidResponse
	}
	if resp.StatusCode != http.StatusOK {
		var envelope struct {
			Code int `json:"code"`
		}
		if json.Unmarshal(raw, &envelope) == nil && envelope.Code == 12153 {
			return nil, accountmodels.InvalidCredentials
		}
		return nil, accountmodels.ProbeRejected
	}
	return parseAccountCatalog(raw)
}

// Media declarations take priority over conversational agent membership.
// Names, token limits and shared catalog knowledge are never capability evidence.
func explicitAccountType(kind string, tags []string, conversational bool) string {
	kind = strings.ToLower(strings.TrimSpace(kind))
	if kind == "image" || kind == "video" {
		return kind
	}
	for _, tag := range tags {
		switch strings.ToLower(strings.TrimSpace(tag)) {
		case "text-to-image", "image-to-image":
			return "image"
		case "text-to-video", "image-to-video":
			return "video"
		}
	}
	if kind != "" {
		return accountmodels.NormalizeType(kind)
	}
	if conversational {
		return "chat"
	}
	return "other"
}

func conversationalAgent(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "cli", "craft", "ask", "plan", "chat", "inlinechat", "inline-chat":
		return true
	}
	return false
}

func accountAgentModels(raw json.RawMessage, chatIDs map[string]bool) error {
	var agents []struct {
		Name   string            `json:"name"`
		ID     string            `json:"id"`
		Models []json.RawMessage `json:"models"`
	}
	if json.Unmarshal(raw, &agents) != nil {
		return accountmodels.InvalidResponse
	}
	for _, agent := range agents {
		name := agent.Name
		if name == "" {
			name = agent.ID
		}
		if !conversationalAgent(name) {
			continue
		}
		for _, value := range agent.Models {
			var id string
			if json.Unmarshal(value, &id) != nil {
				var model struct {
					ID      string `json:"id"`
					ModelID string `json:"modelId"`
					Model   string `json:"model"`
				}
				if json.Unmarshal(value, &model) != nil {
					return accountmodels.InvalidResponse
				}
				id = model.ID
				if id == "" {
					id = model.ModelID
				}
				if id == "" {
					id = model.Model
				}
			}
			if strings.TrimSpace(id) == "" {
				return accountmodels.InvalidResponse
			}
			chatIDs[id] = true
		}
	}
	return nil
}

func accountCatalogArray(raw json.RawMessage, depth int, chatIDs map[string]bool) (json.RawMessage, error) {
	trimmed := strings.TrimSpace(string(raw))
	if depth > 6 || trimmed == "" {
		return nil, accountmodels.InvalidResponse
	}
	if trimmed[0] == '[' {
		return raw, nil
	}
	var env map[string]json.RawMessage
	if err := json.Unmarshal(raw, &env); err != nil || env == nil {
		return nil, accountmodels.InvalidResponse
	}
	if code, ok := env["code"]; ok {
		var value int
		if json.Unmarshal(code, &value) != nil {
			return nil, accountmodels.InvalidResponse
		}
		if value != 0 {
			if value == 12153 {
				return nil, accountmodels.InvalidCredentials
			}
			return nil, accountmodels.ProbeRejected
		}
	}
	if agents, ok := env["agents"]; ok {
		if err := accountAgentModels(agents, chatIDs); err != nil {
			return nil, err
		}
	}
	for _, key := range []string{"models", "items", "list", "data", "result"} {
		if value, ok := env[key]; ok {
			return accountCatalogArray(value, depth+1, chatIDs)
		}
	}
	return nil, accountmodels.InvalidResponse
}

func parseAccountCatalog(raw []byte) ([]accountmodels.Model, error) {
	if !json.Valid(raw) {
		return nil, accountmodels.InvalidResponse
	}
	chatIDs := make(map[string]bool)
	array, err := accountCatalogArray(raw, 0, chatIDs)
	if err != nil {
		return nil, err
	}
	var entries []json.RawMessage
	if json.Unmarshal(array, &entries) != nil || entries == nil {
		return nil, accountmodels.InvalidResponse
	}
	out := make([]accountmodels.Model, 0, len(entries))
	for _, entry := range entries {
		var m struct {
			ID          string   `json:"id"`
			ModelID     string   `json:"modelId"`
			Model       string   `json:"model"`
			DisplayName string   `json:"display_name"`
			Name        string   `json:"name"`
			Type        string   `json:"type"`
			Tags        []string `json:"tags"`
			Disabled    bool     `json:"disabled"`
			Enabled     *bool    `json:"enabled"`
		}
		if json.Unmarshal(entry, &m) != nil || strings.TrimSpace(string(entry)) == "null" {
			return nil, accountmodels.InvalidResponse
		}
		if m.Disabled || (m.Enabled != nil && !*m.Enabled) {
			continue
		}
		if m.ID == "" {
			m.ID = m.ModelID
		}
		if m.ID == "" {
			m.ID = m.Model
		}
		if strings.TrimSpace(m.ID) == "" {
			return nil, accountmodels.InvalidResponse
		}
		if m.DisplayName == "" {
			m.DisplayName = m.Name
		}
		chat := chatIDs[m.ID]
		// Qualifiers belong to the public ID, while upstream agents may use the
		// same suffix without a qualifier. Different qualified realms never match.
		if strings.HasPrefix(m.ID, "cn:") {
			chat = chat || chatIDs[strings.TrimPrefix(m.ID, "cn:")]
		}
		if strings.HasPrefix(m.ID, "global:") {
			chat = chat || chatIDs[strings.TrimPrefix(m.ID, "global:")]
		}
		if !strings.HasPrefix(m.ID, "cn:") && !strings.HasPrefix(m.ID, "global:") {
			chat = chat || chatIDs["cn:"+m.ID] || chatIDs["global:"+m.ID]
		}
		out = append(out, accountmodels.Model{ID: m.ID, DisplayName: m.DisplayName, Type: explicitAccountType(m.Type, m.Tags, chat)})
	}
	return out, nil
}
