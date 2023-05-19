// Copyright 2021 Converter Systems LLC. All rights reserved.

package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/Eun/go-convert"
	"github.com/afs/server/pkg/opcua/ua"
	"github.com/emirpasic/gods/lists/arraylist"
)

type NodeType int64

// ParseNodeType convert any value to NodeType
func ParseNodeType(value interface{}) (NodeType, error) {
	valueType := fmt.Sprintf("%T", value)
	if valueType == "string" {
		nts := strings.ToLower(value.(string))
		switch nts {
		case "root":
			return NodeTypeRoot, nil
		case "connectivity":
			return NodeTypeCategoryConnectivity, nil
		case "channel":
			return NodeTypeChannel, nil
		case "device":
			return NodeTypeDevice, nil
		case "group":
			return NodeTypeGroup, nil
		case "tag":
			return NodeTypeTag, nil
		case "dataloggers":
			return NodeTypeCategoryDataLogger, nil
		case "datalogger":
			return NodeTypeDataLogger, nil
		case "alarms":
			return NodeTypeCategoryAlarms, nil
		}
	} else if valueType == "server.NodeType" {
		return value.(NodeType), nil
	}

	var num int64
	err := convert.Convert(value, &num)
	if err != nil {
		return NodeType(0), ErrInvalidNodeType
	}

	nodeType := NodeType(num)
	if nodeType.String() == "Unknown" {
		return NodeType(0), ErrInvalidNodeType
	}
	return NodeType(nodeType), nil
}

// String returns the name of this node type
func (n NodeType) String() string {
	switch n {
	case NodeTypeRoot:
		return "Root"
	case NodeTypeCategoryConnectivity:
		return "Connectivity"
	case NodeTypeChannel:
		return "Channel"
	case NodeTypeDevice:
		return "Device"
	case NodeTypeGroup:
		return "Group"
	case NodeTypeTag:
		return "Tag"
	case NodeTypeCategoryDataLogger:
		return "Data Loggers"
	case NodeTypeDataLogger:
		return "Data Logger"
	case NodeTypeCategoryAlarms:
		return "Alarms"
	default:
		return "Unknown"
	}
}

// Description returns the information about this node type
func (n NodeType) Description() string {
	switch {
	case n == NodeTypeRoot:
		return "Root"
	case n == NodeTypeCategoryConnectivity:
		return "Connectivity"
	case n == NodeTypeChannel:
		return "Channel"
	case n == NodeTypeDevice:
		return "Device"
	case n == NodeTypeGroup:
		return "Group"
	case n == NodeTypeCategoryDataLogger:
		return "Data Loggers"
	case n == NodeTypeDataLogger:
		return "Data Logger"
	case n == NodeTypeCategoryAlarms:
		return "Alarms"
	default:
		return "Unknown"
	}
}

func (n NodeType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%d", n)), nil
}

// GetTypeID returns the OPC UA reference type ID ReferenceTypeIDHasTypeDefinition
func (n NodeType) GetTypeID() ua.NodeID {
	switch {
	default:
		// return ua.DataTypeIDObjectNode
		return ua.ObjectTypeIDFolderType
	}
}

// GetNodeClass returns the OPC UA NodeClass
func (n NodeType) GetNodeClass() ua.NodeClass {
	switch {
	default:
		return ua.NodeClassObject
	}
}

// IsRoot return true if NodeType is a Root
func (n NodeType) IsRoot() bool {
	return n == NodeTypeRoot
}

// IsTag returns true if NodeType is a Tag
func (n NodeType) IsTag() bool {
	return n == NodeTypeTag
}

// IsCategory returns true if NodeType is a Category
func (n NodeType) IsCategory() bool {
	switch n {
	case NodeTypeCategoryAlarms:
	case NodeTypeCategoryDataLogger:
	case NodeTypeCategoryConnectivity:
		return true
	}
	return false
}

func (n NodeType) Int() int64 {
	return int64(n)
}

const (
	NodeTypeRoot                 NodeType = 1
	NodeTypeCategoryConnectivity NodeType = 2
	NodeTypeChannel              NodeType = 4
	NodeTypeDevice               NodeType = 8
	NodeTypeGroup                NodeType = 16
	NodeTypeTag                  NodeType = 32
	NodeTypeCategoryDataLogger   NodeType = 1024
	NodeTypeDataLogger           NodeType = 2048
	NodeTypeCategoryAlarms       NodeType = 32768
)

// Node ...
type Node interface {
	GetNodeID() ua.NodeID
	GetNodeClass() ua.NodeClass
	GetBrowseName() ua.QualifiedName
	GetDisplayName() ua.LocalizedText
	GetDescription() ua.LocalizedText
	GetRolePermissions() []ua.RolePermissionType
	GetUserRolePermissions(context.Context) []ua.RolePermissionType
	GetReferences() []ua.Reference
	SetReferences([]ua.Reference)
	IsAttributeIDValid(uint32) bool
}

// NodeEx extend the node interface with some functions
type NodeEx interface {
	Node
	// SetBrowseName set BrowseName attribute of this node
	SetBrowseName(value string) error
	// SetDisplayName set DisplayName attribute of this node
	SetDisplayName(value string) error
	// SetDescription set Description attribute of this node
	SetDescription(value string) error
	// GetProperties returns all name property map of this node
	GetProperties() map[string]*VariableNode
	// GetChilds returns all child of this node
	GetChilds() *arraylist.List
	// GetNodeType returns the GetNodeType of this node
	GetNodeType() NodeType
	// IsEntry returns true if this node is an entry for plugin
	IsEntry() bool
	// GetParent returns the parent that has this node
	GetParent() *ObjectNode
	// GetPlugin returns an plugin of this node
	GetPlugin() Plugin
	// GetPluginProps returns an PluginPros that use in Plugin
	GetPluginProps() PluginProps
	// AssignPluginProps assign plugin properties for node
	AssignPluginProps()
	// GetProperty returns an property of this ObjectNode by specified property name
	GetProperty(propName string) (*VariableNode, bool)
	// MustGetProperty returns an property of this ObjectNode by specified property name
	// will panic if property not exists
	MustGetProperty(propName string) *VariableNode
	// AddProperty add a property to this node
	AddProperty(propNode *VariableNode) error
	// Dispose will release all resources of this node
	Dispose()
	// GetFullPath returns the full path to access this node
	GetFullPath() string
	// CanAddChild returns true if this node can add node with specified node type
	CanAddChild(nodeType NodeType) bool
	/// AddChild add an node into childs
	AddChild(child *ObjectNode) error
	// InsertChild insert a node into specified index
	InsertChild(index int, child *ObjectNode) error
	// MoveBefore will move the specified node before the target node
	MoveBefore(node *ObjectNode, target *ObjectNode) error
	// RemoveChild remove an specified node from childs
	RemoveChild(child *ObjectNode) error
	// Update the node via FieldMap
	Update(fields FieldMap) map[string]error
	// BeginUpdate notify this node was being update
	BeginUpdate()
	// EndUpdate notify this node was updated
	EndUpdate()
	// First retuns the first child node that match with specified predicate
	First(predicate func(node *ObjectNode) bool) *ObjectNode
	// DescendantParentFirst returns the first parent that match with specified predicate
	DescendantParentFirst(predicate func(node *ObjectNode) bool) *ObjectNode
	// GetChildByPath returns the depth child node that match with full path
	GetChildByPath(path string) *ObjectNode
	// ForEach loop through it childs
	ForEach(action func(child *ObjectNode))
	// ForEachSelf loop through childs and it self
	ForEachSelf(action func(child *ObjectNode))
	// ForEachDepth loop through childs and dept childs
	ForEachDepth(action func(child *ObjectNode))
	// ForEachSelfDepth loop through childs and it self and dept childs
	ForEachSelfDepth(action func(child *ObjectNode))
	// Validate check all of property and attribute of this node if it valid
	Validate() map[string]error
	// ValidateProperty check the value of specified property if it valid
	ValidateProperty(propName string) error
	// CheckPropertyValue will check the property name and the value is valid for add or update the node
	CheckPropertyValue(propName string, value interface{}) (bool, interface{}, error)
	// Context returns context of this node
	Context() context.Context
}

type HasNodeID interface {
	GetNodeID() ua.NodeID
	SetNodeID(id ua.NodeID)
	ReplaceNodeIDPrefix(oldPrefix, newPrefix string)
}
