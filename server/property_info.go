package server

type PropertyInfo struct {
	BrowseName  string      `json:"browseName"`
	DisplayName string      `json:"displayName"`
	NodeID      string      `json:"nodeID"`
	BrowsePath  string      `json:"browsePath"`
	Description string      `json:"description"`
	Value       interface{} `json:"value"`
	DataType    string      `json:"dataType"`
	AccessLevel string      `json:"accessLevel"`
}

func NewPropertyInfo(node *VariableNode) *PropertyInfo {
	return &PropertyInfo{
		BrowseName:  node.GetBrowseName().Name,
		DisplayName: node.GetDisplayName().Text,
		BrowsePath:  node.GetBrowsePath(),
		Value:       node.GetValue().Value,
		DataType:    GetDataTypeNameByNodeID(node.DataType),
		NodeID:      node.GetNodeID().String(),
		Description: node.GetDescription().Text,
		AccessLevel: node.GetAccessLevelString(),
	}
}
