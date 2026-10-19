package auth

import (
	"sync"
	"testing"
)

func TestModelProbeSnapshotNoMutationOrRefreshToken(t *testing.T) {
	a := &Auth{UID: "uid", AccessToken: "probe-token", RefreshToken: "never-copy", Domain: "www.workbuddy.ai", EnterpriseID: "enterprise", DeviceToken: "per-account-device"}
	snap := a.ModelProbeSnapshot()
	if snap.UID != a.UID || snap.AccessToken != a.AccessToken || snap.EnterpriseID != a.EnterpriseID || snap.DeviceToken != "per-account-device" || snap.RealmStored() != "global" || snap.RefreshToken != "" {
		t.Fatal("incorrect probe credentials", snap.UID)
	}
	snap.AccessToken = "changed"
	if a.AccessTokenValue() != "probe-token" || a.RefreshToken != "never-copy" {
		t.Fatal("probe mutated original")
	}
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				a.Lock()
				a.AccessToken = "rotation"
				a.Domain = "www.workbuddy.ai"
				a.Unlock()
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if a.ModelProbeSnapshot().RefreshToken != "" {
					t.Error("refresh token copied")
				}
			}
		}()
	}
	wg.Wait()
}
