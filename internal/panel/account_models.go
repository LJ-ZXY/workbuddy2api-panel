package panel

import (
	"context"
	"crypto/sha256"
	"net/http"
	"sync"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/accountmodels"
	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
)

const (
	accountModelConcurrency  = 3
	accountModelTimeout      = 15 * time.Second
	accountModelRoundTimeout = 60 * time.Second
)

type accountModelResult struct {
	accountmodels.Snapshot
	Status string  `json:"status"`
	Error  *string `json:"error"`
}

type accountModelBatch struct {
	Accounts []accountModelResult `json:"accounts"`
	Summary  struct {
		Total     int `json:"total"`
		Succeeded int `json:"succeeded"`
		Stale     int `json:"stale"`
		Unknown   int `json:"unknown"`
		Excluded  int `json:"excluded"`
	} `json:"summary"`
	Partial bool `json:"partial"`
}

func modelIdentity(a *auth.Auth) accountmodels.Identity {
	return accountmodels.Identity{UID: a.UID, Realm: a.RealmStored(), EnterpriseID: a.EnterpriseID}
}

// modelAccount reads the named UID only; the clone cannot refresh or mutate the
// pool's credentials. A replaced account is rechecked before publishing results.
func (p *Panel) modelAccount(uid string) (*auth.Auth, *auth.Auth) {
	if p.cfg.Pool == nil {
		return nil, nil
	}
	a := p.cfg.Pool.AuthByUID(uid)
	return a, a.ModelProbeSnapshot()
}

func (p *Panel) accountModelSnapshot(w http.ResponseWriter, r *http.Request) {
	_, a := p.modelAccount(r.PathValue("uid"))
	if a == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"detail": string(accountmodels.AccountNotFound)})
		return
	}
	snap := p.accountModels.Snapshot(modelIdentity(a))
	if st, ok := p.cfg.Pool.Status(a.UID); ok && (st.Cooling || st.Disabled) {
		snap.Stale = true
	}
	writeJSON(w, http.StatusOK, snap)
}

func (p *Panel) accountModelRefresh(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	_, a := p.modelAccount(uid)
	if a == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"detail": string(accountmodels.AccountNotFound)})
		return
	}
	result := p.refreshAccountModels(r.Context(), uid, modelIdentity(a))
	if result.Status != "fresh" {
		writeJSON(w, http.StatusBadGateway, struct {
			Detail   string                 `json:"detail"`
			Snapshot accountmodels.Snapshot `json:"snapshot"`
		}{*result.Error, result.Snapshot})
		return
	}
	writeJSON(w, http.StatusOK, result.Snapshot)
}

func modelResult(snap accountmodels.Snapshot, err error, excluded bool) accountModelResult {
	result := accountModelResult{Snapshot: snap, Status: "fresh"}
	if err != nil {
		result.Snapshot = accountmodels.WithError(snap, err)
		result.Error = result.LastError
		if excluded {
			result.Status = "excluded"
		} else if snap.Cached {
			result.Status = "stale"
		} else {
			result.Status = "unknown"
		}
	}
	return result
}

func (p *Panel) credentialRejected(id accountmodels.Identity, token string) bool {
	p.modelRejectMu.Lock()
	defer p.modelRejectMu.Unlock()
	digest, ok := p.modelRejected[id]
	return ok && digest == sha256.Sum256([]byte(token))
}

func (p *Panel) rejectModelCredential(id accountmodels.Identity, token string) {
	p.modelRejectMu.Lock()
	defer p.modelRejectMu.Unlock()
	if p.modelRejected == nil {
		p.modelRejected = make(map[accountmodels.Identity][32]byte)
	}
	// Only an in-memory digest is retained; neither token nor digest is persisted.
	p.modelRejected[id] = sha256.Sum256([]byte(token))
}

func (p *Panel) refreshAccountModels(parent context.Context, uid string, expected accountmodels.Identity) accountModelResult {
	ctx, cancel := context.WithTimeout(parent, accountModelTimeout)
	defer cancel()
	unlock, err := p.accountModels.LockAccount(ctx, uid)
	if err != nil {
		return modelResult(p.accountModels.Snapshot(expected), err, false)
	}
	defer unlock()
	original, a := p.modelAccount(uid)
	if a == nil {
		return modelResult(accountmodels.Empty(expected), accountmodels.AccountNotFound, true)
	}
	id := modelIdentity(a)
	snap := p.accountModels.Snapshot(id)
	if id != expected {
		return modelResult(snap, accountmodels.IdentityChanged, true)
	}
	status, ok := p.cfg.Pool.Status(uid)
	if !ok {
		return modelResult(accountmodels.Empty(id), accountmodels.AccountNotFound, true)
	}
	if status.Disabled {
		return modelResult(snap, accountmodels.AccountDisabled, true)
	}
	if a.AccessToken == "" || p.credentialRejected(id, a.AccessToken) {
		return modelResult(snap, accountmodels.InvalidCredentials, true)
	}
	if status.Cooling {
		return modelResult(snap, accountmodels.AccountCooling, false)
	}
	models, probeErr := p.cfg.Upstream.ProbeAccountModels(ctx, a)
	if ctx.Err() != nil {
		probeErr = ctx.Err()
	}
	current, currentSnapshot := p.modelAccount(uid)
	if currentSnapshot == nil {
		return modelResult(accountmodels.Empty(id), accountmodels.AccountNotFound, true)
	}
	currentID := modelIdentity(currentSnapshot)
	if current != original || currentID != id {
		return modelResult(p.accountModels.Snapshot(currentID), accountmodels.IdentityChanged, true)
	}
	status, ok = p.cfg.Pool.Status(uid)
	if !ok || status.Disabled {
		return modelResult(snap, accountmodels.AccountDisabled, true)
	}
	if status.Cooling {
		return modelResult(snap, accountmodels.AccountCooling, false)
	}
	if probeErr == accountmodels.InvalidCredentials {
		p.rejectModelCredential(id, a.AccessToken)
	}
	snap, err = p.accountModels.Commit(ctx, id, models, probeErr)
	return modelResult(snap, err, probeErr == accountmodels.InvalidCredentials)
}

func (p *Panel) accountModelsRefresh(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), accountModelRoundTimeout)
	defer cancel()
	batch := accountModelBatch{Accounts: []accountModelResult{}}
	if p.cfg.Pool != nil {
		statuses := p.cfg.Pool.List()
		batch.Accounts = make([]accountModelResult, len(statuses))
		// Internal-only expected identities bind each result to the account
		// observed by its worker; enterprise IDs are not part of the API JSON.
		expected := make([]accountmodels.Identity, len(statuses))
		jobs := make(chan int)
		var workers sync.WaitGroup
		for n := 0; n < accountModelConcurrency; n++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for i := range jobs {
					st := statuses[i]
					_, a := p.modelAccount(st.UID)
					id := accountmodels.Identity{UID: st.UID, Realm: st.Realm}
					expected[i] = id
					if a == nil {
						batch.Accounts[i] = modelResult(accountmodels.Empty(id), accountmodels.AccountNotFound, true)
						continue
					}
					id = modelIdentity(a)
					expected[i] = id
					batch.Accounts[i] = p.refreshAccountModels(ctx, st.UID, id)
				}
			}()
		}
		// Dispatch remains bounded after a round timeout: cancelled account locks
		// return immediately and no further upstream work is started.
		for i := range statuses {
			jobs <- i
		}
		close(jobs)
		workers.Wait()
		// An early result may have become ineligible while a later probe was
		// running. Recheck the entire round before computing/publicizing a union.
		for i, st := range statuses {
			_, a := p.modelAccount(st.UID)
			if a == nil {
				batch.Accounts[i] = modelResult(accountmodels.Empty(expected[i]), accountmodels.AccountNotFound, true)
				continue
			}
			id := modelIdentity(a)
			if id != expected[i] {
				batch.Accounts[i] = modelResult(p.accountModels.Snapshot(id), accountmodels.IdentityChanged, true)
				continue
			}
			status, ok := p.cfg.Pool.Status(st.UID)
			if !ok {
				batch.Accounts[i] = modelResult(accountmodels.Empty(id), accountmodels.AccountNotFound, true)
			} else if status.Disabled {
				batch.Accounts[i] = modelResult(batch.Accounts[i].Snapshot, accountmodels.AccountDisabled, true)
			} else if a.AccessToken == "" || p.credentialRejected(id, a.AccessToken) {
				batch.Accounts[i] = modelResult(batch.Accounts[i].Snapshot, accountmodels.InvalidCredentials, true)
			} else if status.Cooling {
				batch.Accounts[i] = modelResult(batch.Accounts[i].Snapshot, accountmodels.AccountCooling, false)
			}
		}
	}
	batch.Summary.Total = len(batch.Accounts)
	for _, result := range batch.Accounts {
		switch result.Status {
		case "fresh":
			batch.Summary.Succeeded++
		case "stale":
			batch.Summary.Stale++
		case "unknown":
			batch.Summary.Unknown++
		case "excluded":
			batch.Summary.Excluded++
		}
	}
	batch.Partial = batch.Summary.Stale > 0 || batch.Summary.Unknown > 0
	// Even zero eligible/successful accounts return the full batch. The proxy
	// decides whether succeeded == 0 is a failed refresh round.
	writeJSON(w, http.StatusOK, batch)
}
