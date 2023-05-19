package server

import "github.com/iancoleman/strcase"

/*
 ApiJsonNode is the instance it represent the OPC UA ObjectNode and it can be marshall to json or unmarshall back
    - Create an instance of JsonNode by using NewJsonNode()
	- ApiJsonNode will be use to response for client
*/
type ApiJsonNode struct {
	ID          interface{}            `json:"id"`
	NodeType    int64                  `json:"nodeType"`
	PluginID    int16                  `json:"pluginID"`
	ParentID    interface{}            `json:"parentID"`
	Name        string                 `json:"name"`
	DisplayName string                 `json:"displayName"`
	Description string                 `json:"description"`
	Properties  map[string]interface{} `json:"properties"`
	Childs      []*ApiJsonNode         `json:"childs"`
}

// NewApiJsonNode returns an ApiJsonNode instance equivalent with provided ObjectNode
func NewApiJsonNode(n *ObjectNode, depth bool) *ApiJsonNode {
	apiNode := &ApiJsonNode{
		ID:          n.NodeId.String(),
		NodeType:    int64(n.nodeType),
		PluginID:    n.GetPlugin().GetId(),
		ParentID:    n.parent.NodeId.String(),
		Name:        n.BrowseName.Name,
		DisplayName: n.DisplayName.Text,
		Description: n.Description.Text,
		Properties:  map[string]interface{}{},
		Childs:      []*ApiJsonNode{},
	}

	for _, propNode := range n.properties {
		if propNode.PropertyType().IsPluginProperty() {
			apiNode.Properties[strcase.ToLowerCamel(propNode.BrowseName.Name)] = propNode.GetValue().Value
		}
	}

	if depth {
		for _, child := range n.childs.Values() {
			apiNode.Childs = append(apiNode.Childs, NewApiJsonNode(child.(*ObjectNode), depth))
		}
	}
	return apiNode
}
