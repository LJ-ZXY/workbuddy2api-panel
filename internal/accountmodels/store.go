// Package accountmodels holds credential-free, account-identity-bound snapshots.
// It is independent of routing, pool state and upstream realm/media caches.
package accountmodels

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const FileName = "codebuddy-account-models.json"

type Error string

func (e Error) Error() string { return string(e) }

const (
	ProbeFailed        Error = "model_probe_failed"
	ProbeTimeout       Error = "model_probe_timeout"
	ProbeCanceled      Error = "model_probe_canceled"
	InvalidResponse    Error = "model_probe_invalid_response"
	ProbeRejected      Error = "model_probe_rejected"
	InvalidCredentials Error = "model_probe_invalid_credentials"
	WriteFailed        Error = "model_snapshot_write_failed"
	StoreUnavailable   Error = "model_snapshot_unavailable"
	AccountNotFound    Error = "account_not_found"
	AccountDisabled    Error = "account_disabled"
	AccountCooling     Error = "account_cooling"
	IdentityChanged    Error = "account_identity_changed"
)

// SafeError never includes transport URLs, upstream bodies or filesystem paths.
func SafeError(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return string(ProbeTimeout)
	}
	if errors.Is(err, context.Canceled) {
		return string(ProbeCanceled)
	}
	var safe Error
	if errors.As(err, &safe) {
		switch safe {
		case ProbeFailed, ProbeTimeout, ProbeCanceled, InvalidResponse, ProbeRejected,
			InvalidCredentials, WriteFailed, StoreUnavailable, AccountNotFound,
			AccountDisabled, AccountCooling, IdentityChanged:
			return string(safe)
		}
	}
	return string(ProbeFailed)
}

type Identity struct {
	Realm        string `json:"realm"`
	UID          string `json:"uid"`
	EnterpriseID string `json:"enterprise_id"`
}

type Model struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name,omitempty"`
	Type        string `json:"type"`
}

type Snapshot struct {
	UID       string     `json:"uid"`
	Realm     string     `json:"realm"`
	Cached    bool       `json:"cached"`
	Models    []Model    `json:"models"`
	UpdatedAt *time.Time `json:"updated_at"`
	Stale     bool       `json:"stale"`
	LastError *string    `json:"last_error"`
}

type record struct {
	Identity Identity `json:"identity"`
	Snapshot Snapshot `json:"snapshot"`
}

type diskState struct {
	Version  int      `json:"version"`
	Accounts []record `json:"accounts"`
}

type Store struct {
	path    string
	mu      sync.RWMutex
	entries map[Identity]Snapshot
	loadErr error
	// One commit lock covers individual and batch refreshes: no lost updates.
	commit  chan struct{}
	locksMu sync.Mutex
	locks   map[string]chan struct{}
	// Injectable only within this package for atomic-publication fault tests.
	write func(context.Context, string, []byte) error
}

func PathForState(stateFile string) string {
	return filepath.Join(filepath.Dir(stateFile), FileName)
}

func New(path string) *Store {
	s := &Store{path: path, entries: make(map[Identity]Snapshot), commit: make(chan struct{}, 1),
		locks: make(map[string]chan struct{}), write: writeAtomic}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return s
	}
	if err != nil || s.load(raw) != nil {
		s.entries = make(map[Identity]Snapshot)
		s.loadErr = StoreUnavailable
	}
	return s
}

func (s *Store) load(raw []byte) error {
	var disk diskState
	if err := json.Unmarshal(raw, &disk); err != nil || disk.Version != 1 || disk.Accounts == nil {
		return StoreUnavailable
	}
	for _, r := range disk.Accounts {
		id, snap := r.Identity, r.Snapshot
		if id.UID == "" || (id.Realm != "cn" && id.Realm != "global") ||
			snap.UID != id.UID || snap.Realm != id.Realm || snap.Models == nil ||
			(snap.Cached && (snap.UpdatedAt == nil || snap.UpdatedAt.IsZero())) ||
			(!snap.Cached && (snap.UpdatedAt != nil || len(snap.Models) != 0)) {
			return StoreUnavailable
		}
		if _, exists := s.entries[id]; exists {
			return StoreUnavailable
		}
		for i := range snap.Models {
			if snap.Models[i].ID == "" {
				return StoreUnavailable
			}
			snap.Models[i].Type = NormalizeType(snap.Models[i].Type)
		}
		if snap.LastError != nil {
			e := SafeError(Error(*snap.LastError))
			snap.LastError = &e
			snap.Stale = true
		}
		if !snap.Cached {
			snap.Stale = true
		}
		s.entries[id] = clone(snap)
	}
	return nil
}

func NormalizeType(t string) string {
	switch t {
	case "chat", "image", "video":
		return t
	default:
		return "other"
	}
}

func Empty(id Identity) Snapshot {
	return Snapshot{UID: id.UID, Realm: id.Realm, Models: []Model{}, Stale: true}
}

func clone(snap Snapshot) Snapshot {
	snap.Models = append([]Model{}, snap.Models...)
	if snap.UpdatedAt != nil {
		v := *snap.UpdatedAt
		snap.UpdatedAt = &v
	}
	if snap.LastError != nil {
		v := *snap.LastError
		snap.LastError = &v
	}
	return snap
}

func WithError(snap Snapshot, err error) Snapshot {
	snap = clone(snap)
	e := SafeError(err)
	snap.Stale, snap.LastError = true, &e
	return snap
}

func (s *Store) Snapshot(id Identity) Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if snap, ok := s.entries[id]; ok {
		return clone(snap)
	}
	snap := Empty(id)
	if s.loadErr != nil {
		snap = WithError(snap, s.loadErr)
	}
	return snap
}

// LockAccount shares a cancellable UID lock across individual/batch requests,
// including replacement credentials and enterprise/realm identity changes.
func (s *Store) LockAccount(ctx context.Context, uid string) (func(), error) {
	s.locksMu.Lock()
	lock := s.locks[uid]
	if lock == nil {
		lock = make(chan struct{}, 1)
		s.locks[uid] = lock
	}
	s.locksMu.Unlock()
	return acquire(ctx, lock)
}

func acquire(ctx context.Context, lock chan struct{}) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case lock <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-lock
			return nil, err
		}
		return func() { <-lock }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Commit writes a complete candidate file before making it visible to GET.
// Probe failures preserve the last successful model list and timestamp.
func (s *Store) Commit(ctx context.Context, id Identity, models []Model, probeErr error) (Snapshot, error) {
	unlock, err := acquire(ctx, s.commit)
	if err != nil {
		return WithError(s.Snapshot(id), err), err
	}
	defer unlock()
	s.mu.RLock()
	loadErr := s.loadErr
	candidate := make(map[Identity]Snapshot, len(s.entries)+1)
	for key, snap := range s.entries {
		candidate[key] = snap
	}
	s.mu.RUnlock()
	if loadErr != nil {
		return WithError(s.Snapshot(id), loadErr), loadErr
	}
	snap, ok := candidate[id]
	if !ok {
		snap = Empty(id)
	}
	if probeErr != nil {
		snap = WithError(snap, probeErr)
	} else {
		now := time.Now().UTC()
		snap = Snapshot{UID: id.UID, Realm: id.Realm, Cached: true,
			Models: append([]Model{}, models...), UpdatedAt: &now}
	}
	candidate[id] = snap
	disk := diskState{Version: 1, Accounts: make([]record, 0, len(candidate))}
	for key, value := range candidate {
		disk.Accounts = append(disk.Accounts, record{key, value})
	}
	sort.Slice(disk.Accounts, func(i, j int) bool {
		a, b := disk.Accounts[i].Identity, disk.Accounts[j].Identity
		if a.Realm != b.Realm {
			return a.Realm < b.Realm
		}
		if a.UID != b.UID {
			return a.UID < b.UID
		}
		return a.EnterpriseID < b.EnterpriseID
	})
	raw, err := json.MarshalIndent(disk, "", "  ")
	if err != nil {
		return WithError(s.Snapshot(id), WriteFailed), WriteFailed
	}
	if err := s.write(ctx, s.path, raw); err != nil {
		if ctx.Err() != nil {
			return WithError(s.Snapshot(id), ctx.Err()), ctx.Err()
		}
		return WithError(s.Snapshot(id), WriteFailed), WriteFailed
	}
	s.mu.Lock()
	s.entries = candidate
	s.mu.Unlock()
	return clone(snap), probeErr
}

func writeAtomic(ctx context.Context, path string, raw []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".account-models-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err := f.Chmod(0600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return os.Rename(name, path)
}
