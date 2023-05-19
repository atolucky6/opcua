package server

import (
	"context"

	"github.com/afs/server/pkg/eris"
	"github.com/afs/server/pkg/opcua/ua"
)

/*
	 JsonObjectNode is the instance it represent the tree structure of OPC UA ObjectNode and it can be marshall to json or unmarshall back
	    - Create an instance of JsonObjectNode by using NewJsonNode()
		- JsonObjectNode will be use to save the OPC UA project
*/
type JsonObjectNode struct {
	NodeId             ua.ExpandedNodeID       `json:"nodeId"`
	NodeClass          ua.NodeClass            `json:"nodeClass"`
	BrowseName         ua.QualifiedName        `json:"browseName"`
	DisplayName        ua.LocalizedText        `json:"displayName"`
	Description        ua.LocalizedText        `json:"description"`
	RolePermissions    []ua.RolePermissionType `json:"rolePermissions"`
	AccessRestrictions uint16                  `json:"accessRestrictions"`
	References         []ua.Reference          `json:"references"`
	Properties         []*JsonVariableNode     `json:"properties"`
	Childs             []*JsonObjectNode       `json:"childs"`
}

// ToObjectNode returns an equivalent ObjectNode which is OPC UA base object
func (n *JsonObjectNode) ToObjectNode(ctx context.Context, parent *ObjectNode) (*ObjectNode, error) {
	properties := []*VariableNode{}

	var propNodeType *JsonVariableNode
	var propPluginID *JsonVariableNode
	var propInternalID *JsonVariableNode

	// retrive property node
	for _, jsonPropNode := range n.Properties {
		switch jsonPropNode.BrowseName.Name {
		case PropertyNamePluginId:
			propPluginID = jsonPropNode
		case PropertyNameNodeType:
			propNodeType = jsonPropNode
		case PropertyNameInternalId:
			propInternalID = jsonPropNode
		case PropertyNameValue:
			continue
		default:
			propNode, err := jsonPropNode.ToPropertyNode(ctx)
			if err != nil {
				return nil, err
			}
			properties = append(properties, propNode)
		}
	}

	// create node
	node := NewDefaultObjectNode(
		parent,
		n.BrowseName,
		n.DisplayName,
		n.Description,
		propNodeType.Value,
		propPluginID.Value,
		propInternalID.Value,
		ctx,
	)

	// assign property to node
	for _, prop := range properties {
		currentProp, ok := node.GetProperty(prop.GetBrowseName().Name)
		if ok {
			currentProp.SetValue(prop.GetValue())
		} else {
			err := node.AddProperty(prop)
			if err != nil {
				return nil, err
			}
		}
	}

	fieldErrors := node.Validate()
	if len(fieldErrors) > 0 {
		return nil, eris.Fields(fieldErrors)
	}

	node.AssignPluginProps()
	if parent != nil {
		parent.AddChild(node)
	}

	for _, jsonChild := range n.Childs {
		_, err := jsonChild.ToObjectNode(ctx, node)
		if err != nil {
			return nil, err
		}
	}

	return node, nil
}

// NewJsonObjectNode returns an JsonNode instance equivalent with provided ObjectNode
func NewJsonObjectNode(n *ObjectNode, depth bool) *JsonObjectNode {
	jsonNode := &JsonObjectNode{
		NodeId:             ua.NewExpandedNodeID(n.NodeId),
		NodeClass:          n.NodeClass,
		BrowseName:         n.BrowseName,
		DisplayName:        n.DisplayName,
		Description:        n.Description,
		RolePermissions:    n.RolePermissions,
		AccessRestrictions: n.AccessRestrictions,
		References:         n.References,
		Properties:         []*JsonVariableNode{},
		Childs:             []*JsonObjectNode{},
	}

	// create JsonPropertyNodes
	for _, propNode := range n.properties {
		jsonNode.Properties = append(jsonNode.Properties, NewJsonVariableNode(propNode))
	}

	// dig into childs if needed
	if depth {
		for _, child := range n.childs.Values() {
			jsonNode.Childs = append(jsonNode.Childs, NewJsonObjectNode(child.(*ObjectNode), depth))
		}
	}
	return jsonNode
}
