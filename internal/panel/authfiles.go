package panel

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
	"github.com/linguo2625469/workbuddy2api-panel/internal/upstream"
)

// Lock before resolving the credential so remove/refresh/download/status cannot
// operate on a stale registration while another panel operation is in flight.
func (p *Panel) lockCredential(w http.ResponseWriter, r *http.Request) (*auth.Auth, func()) {
	uid := r.PathValue("uid")
	if !p.tryLockAccount(uid) {
		writeErr(w, http.StatusConflict, "account operation in progress")
		return nil, nil
	}
	unlock := func() { p.unlockAccount(uid) }
	a := p.cfg.Pool.AuthByUID(uid)
	if a == nil || a.Deleted() {
		unlock()
		writeErr(w, http.StatusNotFound, "account not found")
		return nil, nil
	}
	return a, unlock
}

func (p *Panel) accountRefresh(w http.ResponseWriter, r *http.Request) {
	a, unlock := p.lockCredential(w, r)
	if a == nil {
		return
	}
	defer unlock()
	if p.cfg.Upstream == nil {
		writeErr(w, http.StatusNotImplemented, "token refresh unavailable")
		return
	}
	if err := p.cfg.Upstream.RefreshToken(a); err != nil {
		var ue *upstream.Error
		if (errors.As(err, &ue) && (ue.Kind == upstream.ErrSessionDead || strings.Contains(ue.Msg, "12153"))) || strings.Contains(err.Error(), "12153") {
			writeJSON(w, http.StatusConflict, map[string]any{
				"ok":    false,
				"code":  12153,
				"error": "refresh token invalid, re-login required",
			})
			return
		}
		// Upstream bodies may contain credentials. Keep all failure responses
		// deliberately generic rather than returning err.Error().
		writeErr(w, http.StatusBadGateway, "token refresh failed")
		return
	}
	if err := a.SaveAtomic(); err != nil {
		writeErr(w, http.StatusInternalServerError, "token refreshed but credential persistence failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (p *Panel) accountDownload(w http.ResponseWriter, r *http.Request) {
	a, unlock := p.lockCredential(w, r)
	if a == nil {
		return
	}
	defer unlock()
	raw, filename, err := a.CredentialBytes()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "credential file unavailable")
		return
	}
	if filename == "." || filename == "" || strings.ContainsAny(filename, "\r\n") {
		filename = "credential.json"
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": filename}))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

func (p *Panel) accountSetStatus(w http.ResponseWriter, r *http.Request) {
	var disabled *bool
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
	start, err := dec.Token()
	if err != nil || start != json.Delim('{') {
		writeErr(w, http.StatusBadRequest, "expected {disabled: boolean}")
		return
	}
	for dec.More() {
		key, err := dec.Token()
		if err != nil || key != "disabled" || disabled != nil {
			writeErr(w, http.StatusBadRequest, "expected one disabled boolean field")
			return
		}
		if err := dec.Decode(&disabled); err != nil || disabled == nil {
			writeErr(w, http.StatusBadRequest, "expected {disabled: boolean}")
			return
		}
	}
	if end, err := dec.Token(); err != nil || end != json.Delim('}') || disabled == nil {
		writeErr(w, http.StatusBadRequest, "expected {disabled: boolean}")
		return
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		writeErr(w, http.StatusBadRequest, "expected one JSON object")
		return
	}
	a, unlock := p.lockCredential(w, r)
	if a == nil {
		return
	}
	defer unlock()
	if err := p.cfg.Pool.SetDisabled(a.UID, *disabled); err != nil {
		writeErr(w, http.StatusInternalServerError, "account status persistence failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "disabled": *disabled})
}
