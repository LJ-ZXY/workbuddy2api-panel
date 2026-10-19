package accountmodels

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStoreIdentityPersistenceEmptyAndErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), FileName)
	s := New(path)
	ids := []Identity{{"cn", "same", ""}, {"global", "same", ""}, {"cn", "same", "enterprise-A"}, {"cn", "same", "enterprise-B"}}
	for i, id := range ids {
		models := []Model{{ID: fmt.Sprintf("%s:model-%d", id.Realm, i), Type: "other"}}
		if i == 3 {
			models = nil
		}
		snap, err := s.Commit(context.Background(), id, models, nil)
		if err != nil || !snap.Cached || snap.Stale || snap.UpdatedAt == nil || snap.LastError != nil || snap.Models == nil {
			t.Fatalf("success: %+v %v", snap, err)
		}
		if snap.UpdatedAt.Location() != time.UTC {
			t.Fatal("timestamp must contain timezone")
		}
	}
	reloaded := New(path)
	for i, id := range ids {
		got := reloaded.Snapshot(id)
		if !reflect.DeepEqual(got, s.Snapshot(id)) {
			t.Fatalf("reload mismatch for %v", id)
		}
		if i != 3 && got.Models[0].ID != fmt.Sprintf("%s:model-%d", id.Realm, i) {
			t.Fatal("identity leak")
		}
	}
	before := s.Snapshot(ids[0])
	failed, err := s.Commit(context.Background(), ids[0], nil, errors.New("SECRET upstream auth token"))
	if err == nil || !failed.Stale || !failed.Cached || *failed.LastError != string(ProbeFailed) || !failed.UpdatedAt.Equal(*before.UpdatedAt) || !reflect.DeepEqual(failed.Models, before.Models) {
		t.Fatalf("failed refresh lost cached data: %+v %v", failed, err)
	}
	unknownID := Identity{"global", "unknown", ""}
	unknown, err := s.Commit(context.Background(), unknownID, nil, ProbeRejected)
	if err == nil || unknown.Cached || unknown.UpdatedAt != nil || unknown.Models == nil || len(unknown.Models) != 0 {
		t.Fatal("invalid unknown snapshot")
	}
	raw, err := os.ReadFile(path)
	if err != nil || strings.Contains(string(raw), "SECRET") || strings.Contains(string(raw), "token") {
		t.Fatal("unsafe persistence", err)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("snapshot permission", err)
	}
	got := New(path).Snapshot(ids[0])
	if !got.Stale || *got.LastError != string(ProbeFailed) {
		t.Fatal("failure not persisted")
	}
	// Returned structs, slices and pointers cannot mutate shared state.
	got.Models[0].ID = "wrong"
	*got.UpdatedAt = time.Time{}
	*got.LastError = "wrong"
	if s.Snapshot(ids[0]).Models[0].ID == "wrong" || s.Snapshot(ids[0]).UpdatedAt.IsZero() {
		t.Fatal("mutable snapshot")
	}
}

func TestStoreDiskBeforeMemoryAndSharedCommitLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), FileName)
	s := New(path)
	entered, release := make(chan struct{}), make(chan struct{})
	writer := s.write
	s.write = func(ctx context.Context, path string, raw []byte) error {
		close(entered)
		<-release
		return writer(ctx, path, raw)
	}
	id := Identity{"cn", "first", ""}
	done := make(chan error, 1)
	go func() {
		_, err := s.Commit(context.Background(), id, []Model{{ID: "cn:first", Type: "chat"}}, nil)
		done <- err
	}()
	<-entered
	if s.Snapshot(id).Cached {
		t.Fatal("memory published before disk")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := s.Commit(ctx, Identity{"cn", "blocked", ""}, nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("commit lock not cancellable", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	s.write = writer
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := Identity{"cn", fmt.Sprint(i), ""}
			if _, err := s.Commit(context.Background(), id, []Model{{ID: "cn:" + id.UID, Type: "other"}}, nil); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	reloaded := New(path)
	if len(reloaded.entries) != 25 {
		t.Fatalf("lost commits: %d", len(reloaded.entries))
	}
	for i := 0; i < 24; i++ {
		if !reloaded.Snapshot(Identity{"cn", fmt.Sprint(i), ""}).Cached {
			t.Fatal("lost persisted entry", i)
		}
	}
}

func TestStoreWriteFailuresPreserveDiskAndMemory(t *testing.T) {
	path := filepath.Join(t.TempDir(), FileName)
	s := New(path)
	id := Identity{"cn", "uid", ""}
	if _, err := s.Commit(context.Background(), id, []Model{{ID: "cn:old", Type: "chat"}}, nil); err != nil {
		t.Fatal(err)
	}
	before := s.Snapshot(id)
	raw, _ := os.ReadFile(path)
	s.write = func(context.Context, string, []byte) error { return errors.New("SECRET private filesystem") }
	got, err := s.Commit(context.Background(), id, []Model{{ID: "cn:new", Type: "chat"}}, nil)
	if err != WriteFailed || *got.LastError != string(WriteFailed) || !reflect.DeepEqual(s.Snapshot(id), before) {
		t.Fatal("memory changed on disk error", got, err)
	}
	after, _ := os.ReadFile(path)
	if string(raw) != string(after) {
		t.Fatal("disk changed on failure")
	}
	// A concrete rename failure leaves no temporary files and no publication.
	dir := t.TempDir()
	bad := filepath.Join(dir, "destination-directory")
	if err := os.Mkdir(bad, 0700); err != nil {
		t.Fatal(err)
	}
	if err := writeAtomic(context.Background(), bad, []byte("{}")); err == nil {
		t.Fatal("expected rename failure")
	}
	files, _ := filepath.Glob(filepath.Join(dir, ".account-models-*"))
	if len(files) != 0 {
		t.Fatal("temporary file leak", files)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := writeAtomic(ctx, filepath.Join(dir, "canceled.json"), []byte("{}")); !errors.Is(err, context.Canceled) {
		t.Fatal("ignored cancellation", err)
	}
}

func TestStoreLoadCorruptionFailsClosed(t *testing.T) {
	for _, raw := range []string{
		`{broken SECRET`, `null`, `{"version":1,"accounts":null}`,
		`{"version":1,"accounts":[{"identity":{"uid":"x","realm":"cn"},"snapshot":{"uid":"x","realm":"global","models":[]}}]}`,
		`{"version":1,"accounts":[{"identity":{"uid":"x","realm":"cn"},"snapshot":{"uid":"x","realm":"cn","cached":true,"models":[],"updated_at":null}}]}`,
	} {
		t.Run(raw, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), FileName)
			if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			s := New(path)
			id := Identity{"cn", "x", ""}
			got := s.Snapshot(id)
			if got.Cached || *got.LastError != string(StoreUnavailable) {
				t.Fatal("invalid load", got)
			}
			if _, err := s.Commit(context.Background(), id, nil, nil); err != StoreUnavailable {
				t.Fatal("overwrote corrupt file", err)
			}
			after, _ := os.ReadFile(path)
			if string(after) != raw {
				t.Fatal("corrupt evidence overwritten")
			}
		})
	}
}

func TestStoreAccountLocksAndPath(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), FileName))
	unlock, err := s.LockAccount(context.Background(), "same")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := s.LockAccount(ctx, "same"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("UID lock not shared", err)
	}
	other, err := s.LockAccount(context.Background(), "other")
	if err != nil {
		t.Fatal(err)
	}
	other()
	unlock()
	again, err := s.LockAccount(context.Background(), "same")
	if err != nil {
		t.Fatal(err)
	}
	again()
	for _, tc := range []struct{ state, want string }{
		{"./data/state.json", "data/" + FileName}, {"/tmp/override/state.json", "/tmp/override/" + FileName},
		{"state.json", FileName}, {"", FileName},
	} {
		if got := PathForState(tc.state); got != tc.want {
			t.Fatalf("path=%q want %q", got, tc.want)
		}
	}
}
