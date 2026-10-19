package auth

// ModelProbeSnapshot atomically captures read-only probe credentials. It never
// copies a refresh token or writes to the original Auth; realm is the identity,
// not the pool's optional CN routing escape hatch.
func (a *Auth) ModelProbeSnapshot() *Auth {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return &Auth{AccessToken: a.AccessToken, Domain: a.Domain, UID: a.UID,
		EnterpriseID: a.EnterpriseID, DeviceToken: a.DeviceToken, realm: ResolveRealm(a.realm, a.Domain)}
}
