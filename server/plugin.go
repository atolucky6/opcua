package server

import (
	"fmt"
	"strings"

	"gopkg.in/guregu/null.v4"
)

var (
	// PluginFormAdd indicate the form config that plugin provide is use for add node
	PluginFormAdd FormType = "ADD"

	// PluginFormEdit indicate the form config that plugin provide is use for add node
	PluginFormEdit FormType = "EDIT"

	PluginFormUnknown FormType = ""
)

// FormType indicate the type of form config that plugin provide
type FormType string

func (t FormType) String() string {
	return string(t)
}

func ParseFormType(value interface{}) (FormType, error) {
	str := strings.ToLower(fmt.Sprintf("%s", value))
	switch str {
	case "add":
		return PluginFormAdd, nil
	case "edit":
		return PluginFormEdit, nil
	default:
		return PluginFormUnknown, ErrInvalidFormType
	}
}

// PluginInfo describe the information about the plugin
type PluginInfo struct {
	// Id is the id of plugin
	Id int16 `json:"id"`

	// DisplayName is the name of plugin that will display for client user
	DisplayName string `json:"displayName"`

	// Version is
	Version string `json:"version"`

	// Category is the define for plugin will be used for which CategoryNode like (Connectivity, DataLogger, Alarms...)
	Category int `json:"category"`

	// Description is the quick description about the plugin
	Description string `json:"description"`
}

type PluginConfig struct {
	// NodeConfigs is the map that will holding the PluginOptions for each NodeType
	NodeConfigs map[string]*NodeConfig `json:"nodeConfigs"`

	// ViewConfigs is the map that will holding some view configuration for each NodeType
	ViewConfigs map[string]interface{} `json:"viewConfigs"`
}

// GetFieldDef returns an FieldDef by the name and node type
func (cfg *PluginConfig) GetFieldDef(fieldName string, nodeType NodeType) *FieldDef {
	// get plugin options from node type
	po := cfg.NodeConfigs[nodeType.String()]
	if po == nil {
		return nil
	}

	// loop into field defs to get FieldDef if the name is matching
	for _, fd := range po.FieldDefs {
		if fd.Name == fieldName {
			return fd
		}
	}
	return nil
}

// GetNodeConfig returns an NodeConfig which will be used for provided NodeType
func (cfg *PluginConfig) GetNodeConfig(nodeType interface{}) (*NodeConfig, error) {
	// parse to NodeType instace
	nt, err := ParseNodeType(nodeType)
	if err != nil {
		return nil, err
	}

	// get options from the options map
	opt, ok := cfg.NodeConfigs[nt.String()]
	if !ok || opt == nil {
		return nil, fmt.Errorf("plugin don't have configuration for node type '%s'", nt.String())
	}

	// mapping view configuration if PluginOptions has view
	if len(opt.View) > 0 {
		if viewConfigs, ok := cfg.ViewConfigs[opt.View]; ok {
			opt.ViewConfigs = viewConfigs
		}
	}

	return opt, nil
}

// The plugin that define the behavior of the node
type Plugin interface {
	// Start will start the process of specified node if it is entry node
	Start(entryNode *ObjectNode) error
	// Stop will stop the procoess of specified node if it is entry node
	Stop(entryNode *ObjectNode) error
	// IsPluginEntry returns true if specified node is an entry of this plugin
	IsPluginEntry(node *ObjectNode) bool
	// GetPluginInfo returns the information about this plugin
	GetPluginInfo() *PluginInfo
	// GetPluginConfig returns the configuration of this plugin
	GetPluginConfig() *PluginConfig
	// ID return an id of this plugin
	GetId() int16
	// CreatePluginProps returns an plugin props for specified node
	GetPluginProps(node *ObjectNode) PluginProps
	// Validate will check the node and returns an errors if it not valid
	Validate(node *ObjectNode) map[string]error
	// CheckPropertyValue will check the property name and the value is valid for add or update the node
	CheckPropertyValue(node *ObjectNode, name string, value interface{}) (bool, interface{}, error)
	// CanAddNodeType returns true of the specified parent node can accept the child with specified node type
	CanAddNodeType(parent *ObjectNode, nodeType NodeType) bool
	// AddNode will add an child node into the parent node
	AddNode(parent *ObjectNode, child *ObjectNode) error
	// Remove will remove an child node from the parent node
	RemoveNode(parent *ObjectNode, child *ObjectNode) error
	// CheckUpdateValid check update fields is valid or not
	CheckUpdateValid(node *ObjectNode, m FieldMap) (map[string]error, FieldMap)
	// GetFormConfig returns an form configuration for specified node type and form type
	GetFormConfig(formType FormType, nodeType NodeType) ([]byte, error)
	// GetEntryState returns an current state of entry node
	GetEntryState(node *ObjectNode) *EntryState
}

// IsPropertyNameValid returns true if property name is valid for specified node type and plugin
func IsPropertyNameValid(propName string, nodeType NodeType, plugin Plugin) bool {
	return plugin.GetPluginConfig().GetFieldDef(propName, nodeType) != nil
}

/*
IsPluginProperty returns true if property name is valid for specified plugin otherwise is false
  - Property was defined in metadata.json of each plugin folder
*/
func IsPluginProperty(propName string, nodeType NodeType, plugin Plugin) bool {
	return plugin.GetPluginConfig().GetFieldDef(propName, nodeType) != nil
}

type EntryState struct {
	State     int64     `json:"state"`
	LastError string    `json:"lastError"`
	Timestamp null.Time `json:"timestamp"`
	Err       error     `json:"Error"`
}

// PluginProps store all of the needs parameters to run that node
type PluginProps interface {
	AssignNode(node *ObjectNode)
	UpdateProps()
	OnChildAdd(node *ObjectNode)
	OnChildRemove(node *ObjectNode)
}

type NodeConfig struct {
	ChildTypes  []string    `json:"childTypes"`
	FieldDefs   []*FieldDef `json:"fieldDefs"`
	View        string      `json:"view"`
	ViewConfigs interface{} `json:"viewConfigs"`
}
