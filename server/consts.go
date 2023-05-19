package server

const (
	DefaultNameSpace uint16 = 0
	DefaultLocale    string = "en"
	PathSeparator           = "."
	PluginIDCore     int16  = 0

	// Common Properties Name and Description
	PropertyNameBrowseName  string = "BrowseName"
	PropertyNameId          string = "Id"
	PropertyNameDisplayName string = "DisplayName"
	PropertyNameDescription string = "Description"

	PropertyNameEnabled string = "_Enabled"
	PropertyDescEnabled string = "Enabled property"

	PropertyNameEntry string = "_Entry"
	PropertyDescEntry string = "Entry property"

	PropertyNameNodeType string = "_NodeType"
	PropertyDescNodeType string = "NodeType"

	PropertyNamePluginId string = "_PluginId"
	PropertyDescPluginId string = "PluginId"

	PropertyNameValue string = "_Value"
	PropertyDescValue string = "Value"

	PropertyNameInternalId string = "_InternalId"
	PropertyDescInternalId string = "InternalId"
)

type ContextKey string

var (
	// context value key
	CtxKeyProjectManager   ContextKey = "ctx_project_manager"
	CtxKeyPluginManager    ContextKey = "ctx_plugin_manager"
	CtxKeyPluginProvider   ContextKey = "ctx_plugin_provider"
	CtxKeyNamespaceManager ContextKey = "ctx_namespace_manager"
	CtxKeyConfig           ContextKey = "ctx_config"
	CtxKeyUAServer         ContextKey = "ctx_ua_server"
	CtxKeyUserRoles        string     = "ctx_user_roles"
)
