package server

import (
	"github.com/emirpasic/gods/lists/arraylist"
	"github.com/emirpasic/gods/utils"
)

type NodeInfo struct {
	BrowseName  string          `json:"browseName"`
	DisplayName string          `json:"displayName"`
	NodeID      string          `json:"nodeID"`
	BrowsePath  string          `json:"browsePath"`
	Description string          `json:"description"`
	Properties  []*PropertyInfo `json:"properties"`
	PluginProps interface{}     `json:"pluginProps"`
}

func NewNodeInfo(node *ObjectNode) *NodeInfo {
	nodeInfo := &NodeInfo{
		BrowseName:  node.GetBrowseName().Name,
		DisplayName: node.GetDisplayName().Text,
		NodeID:      node.GetNodeID().String(),
		Description: node.GetDescription().Text,
		BrowsePath:  node.GetFullPath(),
		Properties:  []*PropertyInfo{},
		PluginProps: node.GetPluginProps(),
	}

	// get property names to sort
	propKeys := arraylist.New()
	for k := range node.GetProperties() {
		propKeys.Add(k)
	}
	propKeys.Sort(utils.StringComparator)

	// create property info
	for _, k := range propKeys.Values() {
		propInfo := NewPropertyInfo(node.GetProperties()[k.(string)])
		nodeInfo.Properties = append(nodeInfo.Properties, propInfo)
	}

	return nodeInfo
}
