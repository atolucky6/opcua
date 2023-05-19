package server

import (
	"context"

	"github.com/afs/server/config"
)

/*
PluginProvider is the interface to provide the plugin by using plugin id
*/
type PluginProvider interface {
	// GetPlugin returns an instance IPlugin by it ID
	GetPlugin(pluginID int16) Plugin
	// SupportPlugins returns an list of plugin that application supported
	SupportPlugins() []PluginInfo
}

/*
PluginManager is the instance, it will manage all plugin for application
  - Create an instance of PluginManager, by using NewPluginManager()
*/
type PluginManager struct {
	// ctx is the context of application,
	// it contains all of the manager instance or some information
	ctx context.Context

	// config is the common application config
	config *config.Config

	// pluginProvider is the provider of plugin
	pluginProvider PluginProvider
}

// NewPluginManager returns an PluginManager instance
func NewPluginManager() *PluginManager {
	return &PluginManager{}
}

func (p *PluginManager) SetContext(ctx context.Context) {
	p.ctx = ctx
	p.config = ctx.Value(CtxKeyConfig).(*config.Config)
	p.pluginProvider = ctx.Value(CtxKeyPluginProvider).(PluginProvider)
}

// GetPlugin return an plugin by
func (c *PluginManager) GetPlugin(id int16) Plugin {
	// find plugin in support plugins
	for _, i := range c.pluginProvider.SupportPlugins() {
		if i.Id == id {
			return c.pluginProvider.GetPlugin(id)
		}
	}
	return nil
}
