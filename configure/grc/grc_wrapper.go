package grc

import "github.com/appootb/grc"

func NewRemoteConfigWrapper(rc *grc.RemoteConfig) *remoteConfigWrapper {
	return &remoteConfigWrapper{
		RemoteConfig: rc,
	}
}

type remoteConfigWrapper struct {
	*grc.RemoteConfig
}

// Register the configuration pointer.
func (w *remoteConfigWrapper) Register(component string, v interface{}) error {
	return w.RemoteConfig.RegisterConfig(component, v)
}
