package server

import (
	"context"

	"github.com/afs/server/pkg/opcua/ua"
)

// JsonPropertyType define the type of JsonPropertyNode
type JsonPropertyType int16

// IsPluginProperty returns true if the JsonPropertyType is plugin property
func (t JsonPropertyType) IsPluginProperty() bool {
	return t == PROP_TYPE_PLUGIN
}

// IsPluginProperty returns true if the JsonPropertyType is default property
func (t JsonPropertyType) IsDefaultProperty() bool {
	return t == PROP_TYPE_DEFAULT
}

const (
	// Define the property will use for plugin if this property has changed it will call PluginProps.UpdateProps()
	PROP_TYPE_PLUGIN JsonPropertyType = 1

	// Define the property will use for holding some property that might be not affected to plugin if it changed like (PluginID, NodeType, Entry...)
	PROP_TYPE_DEFAULT JsonPropertyType = 2
)

/*
	 JsonVariableNode is the instance it represent the tree structure of OPC UA VariableNode and it can be marshall to json or unmarshall back
	    - Create an instance of JsonVariableNode by using NewJsonPropertyNode()
		- JsonVariableNode will be use to save the OPC UA project
*/
type JsonVariableNode struct {
	NodeId                  ua.ExpandedNodeID       `json:"nodeId"`
	BrowseName              ua.QualifiedName        `json:"browseName"`
	DisplayName             ua.LocalizedText        `json:"displayName"`
	Description             ua.LocalizedText        `json:"description"`
	NodeClass               ua.NodeClass            `json:"nodeClass"`
	RolePermissions         []ua.RolePermissionType `json:"rolePermissions"`
	AccessRestrictions      uint16                  `json:"accessRestrictions"`
	References              []ua.Reference          `json:"references"`
	Value                   ua.DataValue            `json:"value"`
	DataType                ua.ExpandedNodeID       `json:"dataType"`
	ValueRank               int32                   `json:"valueRank"`
	ArrayDimensions         []uint32                `json:"arrayDimensions"`
	AccessLevel             byte                    `json:"accessLevel"`
	MinimumSamplingInterval float64                 `json:"minimumSamplingInterval"`
	Historizing             bool                    `json:"historizing"`
	PropType                JsonPropertyType        `json:"propertyType"`
}

// ToPropertyNode returns an equivalent VariableNode which is OPC UA variable object
func (n *JsonVariableNode) ToPropertyNode(ctx context.Context) (*VariableNode, error) {
	return NewVariableNode(
		n.NodeId.NodeID,
		n.BrowseName,
		n.DisplayName,
		n.Description,
		n.RolePermissions,
		n.References,
		n.Value,
		n.DataType.NodeID,
		n.ValueRank,
		n.ArrayDimensions,
		n.AccessLevel,
		n.MinimumSamplingInterval,
		n.Historizing,
		nil,
	), nil
}

// NewJsonVariableNode returns an JsonPropertyNode instance equivalent with provided VariableNode
func NewJsonVariableNode(n *VariableNode) *JsonVariableNode {
	jvNode := &JsonVariableNode{
		NodeId:                  ua.NewExpandedNodeID(n.NodeId),
		BrowseName:              n.BrowseName,
		DisplayName:             n.DisplayName,
		Description:             n.Description,
		NodeClass:               n.NodeClass,
		RolePermissions:         n.RolePermissions,
		AccessRestrictions:      n.AccessRestrictions,
		References:              n.References,
		Value:                   n.Value,
		DataType:                ua.NewExpandedNodeID(n.DataType),
		ValueRank:               n.ValueRank,
		ArrayDimensions:         n.ArrayDimensions,
		MinimumSamplingInterval: n.MinimumSamplingInterval,
		Historizing:             n.Historizing,
		AccessLevel:             n.AccessLevel,
		PropType:                n.propType,
	}
	return jvNode
}
