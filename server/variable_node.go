package server

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"sync"

	"github.com/afs/server/config"
	"github.com/afs/server/pkg/opcua/ua"
	"github.com/karlseguin/jsonwriter"
)

type VariableNode struct {
	sync.RWMutex

	NodeId                  ua.NodeID               `json:"NodeId"`
	BrowseName              ua.QualifiedName        `json:"BrowseName"`
	DisplayName             ua.LocalizedText        `json:"DisplayName"`
	Description             ua.LocalizedText        `json:"Description"`
	NodeClass               ua.NodeClass            `json:"NodeClass"`
	RolePermissions         []ua.RolePermissionType `json:"RolePermissions"`
	AccessRestrictions      uint16                  `json:"AccessRestrictions"`
	References              []ua.Reference          `json:"References"`
	Value                   ua.DataValue            `json:"Value"`
	DataType                ua.NodeID               `json:"DataType"`
	ValueRank               int32                   `json:"ValueRank"`
	ArrayDimensions         []uint32                `json:"ArrayDimensions"`
	AccessLevel             byte                    `json:"AccessLevel"`
	MinimumSamplingInterval float64                 `json:"MinimumSamplingInterval"`
	Historizing             bool                    `json:"Historizing"`

	ctx               context.Context                                                    `json:"-"`
	parent            *ObjectNode                                                        `json:"-"`
	propType          JsonPropertyType                                                   `json:"-"`
	historian         HistoryReadWriter                                                  `json:"-"`
	ReadValueHandler  func(context.Context, ua.ReadValueID) ua.DataValue                 `json:"-"`
	WriteValueHandler func(context.Context, ua.WriteValue) (ua.DataValue, ua.StatusCode) `json:"-"`
}

var _ Node = (*VariableNode)(nil)

func NewVariableNode(nodeID ua.NodeID, browseName ua.QualifiedName, displayName ua.LocalizedText, description ua.LocalizedText, rolePermissions []ua.RolePermissionType, references []ua.Reference, value ua.DataValue, dataType ua.NodeID, valueRank int32, arrayDimensions []uint32, accessLevel byte, minimumSamplingInterval float64, historizing bool, historian HistoryReadWriter) *VariableNode {
	return &VariableNode{
		NodeId:                  nodeID,
		NodeClass:               ua.NodeClassVariable,
		BrowseName:              browseName,
		DisplayName:             displayName,
		Description:             description,
		RolePermissions:         rolePermissions,
		AccessRestrictions:      0,
		References:              references,
		Value:                   value,
		DataType:                dataType,
		ValueRank:               valueRank,
		ArrayDimensions:         arrayDimensions,
		AccessLevel:             accessLevel,
		MinimumSamplingInterval: minimumSamplingInterval,
		Historizing:             historizing,
		historian:               historian,
	}
}

// func NewPropertyNode(
// 	nodeID ua.NodeID,
// 	propType JsonPropertyType,
// 	name string,
// 	description string,
// 	initValue ua.DataValue,
// 	dataType ua.NodeID,
// 	accessLevel byte,
// 	ctx context.Context) *VariableNode {

// 	return &VariableNode{
// 		NodeId:                  nodeID,
// 		propType:                propType,
// 		NodeClass:               ua.NodeClassVariable,
// 		BrowseName:              ua.NewQualifiedName(DefaultNameSpace, name),
// 		DisplayName:             ua.NewLocalizedText(name, DefaultLocale),
// 		Description:             ua.NewLocalizedText(description, DefaultLocale),
// 		RolePermissions:         nil,
// 		AccessRestrictions:      0,
// 		Value:                   initValue,
// 		DataType:                dataType,
// 		ValueRank:               ua.ValueRankScalar,
// 		ArrayDimensions:         []uint32{},
// 		AccessLevel:             accessLevel,
// 		MinimumSamplingInterval: float64(ctx.Value(CtxKeyConfig).(*util.Config).MinSamplingInterval),
// 		Historizing:             false,
// 		historian:               nil,
// 		References: []ua.Reference{
// 			ua.NewReference(ua.ReferenceTypeIDHasTypeDefinition, false, ua.NewExpandedNodeID(ua.VariableTypeIDPropertyType)),
// 		},
// 	}
// }

// NodeID returns the NodeID attribute of this node.
func (n *VariableNode) GetNodeID() ua.NodeID {
	return n.NodeId
}

func (n *VariableNode) SetNodeID(id ua.NodeID) {
	n.Lock()
	defer n.Unlock()
	n.NodeId = id
}

func (n *VariableNode) ReplaceNodeIDPrefix(oldPrefix, newPrefix string) {
	currentID := []byte{}
	currentID = append(currentID, newPrefix...)
	currentID = append(currentID, n.GetNodeID().GetID().(string)[len(oldPrefix):]...)
	n.SetNodeID(ua.NewNodeIDString(DefaultNameSpace, string(currentID)))
}

// NodeClass returns the NodeClass attribute of this node.
func (n *VariableNode) GetNodeClass() ua.NodeClass {
	return n.NodeClass
}

// BrowseName returns the BrowseName attribute of this node.
func (n *VariableNode) GetBrowseName() ua.QualifiedName {
	return n.BrowseName
}

// DisplayName returns the DisplayName attribute of this node.
func (n *VariableNode) GetDisplayName() ua.LocalizedText {
	return n.DisplayName
}

// Description returns the Description attribute of this node.
func (n *VariableNode) GetDescription() ua.LocalizedText {
	return n.Description
}

// RolePermissions returns the RolePermissions attribute of this node.
func (n *VariableNode) GetRolePermissions() []ua.RolePermissionType {
	return n.RolePermissions
}

// UserRolePermissions returns the RolePermissions attribute of this node for the current user.
func (n *VariableNode) GetUserRolePermissions(ctx context.Context) []ua.RolePermissionType {
	filteredPermissions := []ua.RolePermissionType{}
	if ctx.Value(SessionKey) != nil {
		session, ok := ctx.Value(SessionKey).(*Session)
		if !ok {
			return filteredPermissions
		}
		roles := session.UserRoles()
		rolePermissions := n.GetRolePermissions()
		if rolePermissions == nil {
			rolePermissions = session.Server().RolePermissions()
		}
		for _, role := range roles {
			for _, rp := range rolePermissions {
				if rp.RoleID == role {
					filteredPermissions = append(filteredPermissions, rp)
				}
			}
		}
	} else if roles, ok := ctx.Value(CtxKeyUserRoles).([]ua.NodeID); ok && roles != nil {
		rolePermissions := n.GetRolePermissions()
		if rolePermissions == nil {
			rolePermissions = n.ctx.Value(CtxKeyUAServer).(*UAServer).RolePermissions()
		}
		for _, role := range roles {
			for _, rp := range rolePermissions {
				if rp.RoleID == role {
					filteredPermissions = append(filteredPermissions, rp)
				}
			}
		}
	}
	return filteredPermissions
}

// References returns the References of this node.
func (n *VariableNode) GetReferences() []ua.Reference {
	n.RLock()
	res := n.References
	n.RUnlock()
	return res
}

// SetReferences sets the References of this node.
func (n *VariableNode) SetReferences(value []ua.Reference) {
	n.Lock()
	n.References = value
	n.Unlock()
}

// GetValue returns the value of the Variable.
func (n *VariableNode) GetValue() ua.DataValue {
	n.RLock()
	res := n.Value
	n.RUnlock()
	return res
}

// SetValue sets the value of the Variable.
func (n *VariableNode) SetValue(value ua.DataValue) bool {
	n.Lock()

	hasChanged := false
	if !reflect.DeepEqual(n.Value, value) {
		n.Value = value
		hasChanged = true
	}

	if n.Historizing {
		n.historian.WriteValue(context.Background(), n.NodeId, value)
	}

	n.Unlock()

	if n.propType.IsPluginProperty() {
		if hasChanged && n.parent != nil {
			if n.parent != nil && !n.parent.isUpdating {
				n.parent.pluginProps.UpdateProps()
			}
		}
	}

	return hasChanged
}

// GetDataType returns the GetDataType attribute of this node.
func (n *VariableNode) GetDataType() ua.NodeID {
	return n.DataType
}

// Set the DataType attribute of this node.
func (n *VariableNode) SetDataType(dataTypeID ua.NodeID) {
	n.DataType = dataTypeID
}

// GetValueRank returns the GetValueRank attribute of this node.
func (n *VariableNode) GetValueRank() int32 {
	return n.ValueRank
}

// GetArrayDimensions returns the GetArrayDimensions attribute of this node.
func (n *VariableNode) GetArrayDimensions() []uint32 {
	return n.ArrayDimensions
}

// GetAccessLevel returns the GetAccessLevel attribute of this node.
func (n *VariableNode) GetAccessLevel() byte {
	return n.AccessLevel
}

// Set AccessLevel attribute
func (n *VariableNode) SetAccessLevel(accessLevel byte) {
	n.AccessLevel = accessLevel
}

// UserAccessLevel returns the AccessLevel attribute of this node for this user.
func (n *VariableNode) UserAccessLevel(ctx context.Context) byte {
	accessLevel := n.AccessLevel
	session, ok := ctx.Value(SessionKey).(*Session)
	if !ok {
		return 0
	}
	roles := session.UserRoles()
	rolePermissions := n.GetRolePermissions()
	if rolePermissions == nil {
		rolePermissions = session.Server().RolePermissions()
	}
	var currentRead, currentWrite, historyRead bool
	for _, role := range roles {
		for _, rp := range rolePermissions {
			if rp.RoleID == role {
				if rp.Permissions&ua.PermissionTypeRead != 0 {
					currentRead = true
				}
				if rp.Permissions&ua.PermissionTypeWrite != 0 {
					currentWrite = true
				}
				if rp.Permissions&ua.PermissionTypeReadHistory != 0 {
					historyRead = true
				}
			}
		}
	}
	if !currentRead {
		accessLevel &^= ua.AccessLevelsCurrentRead
	}
	if !currentWrite {
		accessLevel &^= ua.AccessLevelsCurrentWrite
	}
	if !historyRead {
		accessLevel &^= ua.AccessLevelsHistoryRead
	}
	return accessLevel
}

// GetMinimumSamplingInterval returns the GetMinimumSamplingInterval attribute of this node.
func (n *VariableNode) GetMinimumSamplingInterval() float64 {
	return n.MinimumSamplingInterval
}

// GetHistorizing returns the GetHistorizing attribute of this node.
func (n *VariableNode) GetHistorizing() bool {
	n.RLock()
	ret := n.Historizing
	n.RUnlock()
	return ret
}

// SetHistorizing sets the Historizing attribute of this node.
func (n *VariableNode) SetHistorizing(historizing bool) {
	n.Lock()
	n.Historizing = historizing
	n.Unlock()
}

// SetReadValueHandler sets the ReadValueHandler of this node.
func (n *VariableNode) SetReadValueHandler(value func(context.Context, ua.ReadValueID) ua.DataValue) {
	n.Lock()
	n.ReadValueHandler = value
	n.Unlock()
}

// SetWriteValueHandler sets the WriteValueHandler of this node.
func (n *VariableNode) SetWriteValueHandler(value func(context.Context, ua.WriteValue) (ua.DataValue, ua.StatusCode)) {
	n.Lock()
	n.WriteValueHandler = value
	n.Unlock()
}

// IsAttributeIDValid returns true if attributeId is supported for the node.
func (n *VariableNode) IsAttributeIDValid(attributeID uint32) bool {
	switch attributeID {
	case ua.AttributeIDNodeID, ua.AttributeIDNodeClass, ua.AttributeIDBrowseName,
		ua.AttributeIDDisplayName, ua.AttributeIDDescription, ua.AttributeIDRolePermissions,
		ua.AttributeIDUserRolePermissions, ua.AttributeIDValue, ua.AttributeIDDataType,
		ua.AttributeIDValueRank, ua.AttributeIDArrayDimensions, ua.AttributeIDAccessLevel,
		ua.AttributeIDUserAccessLevel, ua.AttributeIDMinimumSamplingInterval, ua.AttributeIDHistorizing:
		return true
	default:
		return false
	}
}

// Config returns an config instance from context
func (n *VariableNode) Config() *config.Config {
	return n.ctx.Value(CtxKeyConfig).(*config.Config)
}

// Validate check the value of this VariableNode if it valid
func (n *VariableNode) Validate() error {
	fm := FieldMap{}
	fm[n.BrowseName.Name] = n.Value.Value
	fieldErrors, _ := n.parent.GetPlugin().CheckUpdateValid(n.parent, fm)
	return fieldErrors[n.BrowseName.Name]
}

// PropertyType returns the property type of this node
func (n *VariableNode) PropertyType() JsonPropertyType {
	return n.propType
}

// Owner returns an node that owner this property
func (n *VariableNode) Owner() *ObjectNode {
	return n.parent
}

// SetOwner set the node that owner this property
func (n *VariableNode) SetOwner(parent *ObjectNode) {
	n.parent = parent
}

// GetBrowsePath returs the browse path to access to this node
func (n *VariableNode) GetBrowsePath() string {
	return n.Owner().GetFullPath() + PathSeparator + n.GetBrowseName().Name
}

// GetAccessLevelString retuns an string that representation the access level
func (n *VariableNode) GetAccessLevelString() string {
	level := n.GetAccessLevel()
	levelStrings := []string{}

	if level&ua.AccessLevelsNone == ua.AccessLevelsNone {
		levelStrings = append(levelStrings, "None")
	}

	if level&ua.AccessLevelsCurrentRead == ua.AccessLevelsCurrentRead {
		levelStrings = append(levelStrings, "Read")
	}

	if level&ua.AccessLevelsCurrentWrite == ua.AccessLevelsCurrentWrite {
		levelStrings = append(levelStrings, "Write")
	}

	if level&ua.AccessLevelsHistoryRead == ua.AccessLevelsHistoryRead {
		levelStrings = append(levelStrings, "HistoryRead")
	}

	if level&ua.AccessLevelsHistoryWrite == ua.AccessLevelsHistoryWrite {
		levelStrings = append(levelStrings, "HistoryWrite")
	}

	if level&ua.AccessLevelsSemanticChange == ua.AccessLevelsSemanticChange {
		levelStrings = append(levelStrings, "SemanticChange")
	}

	if level&ua.AccessLevelsStatusWrite == ua.AccessLevelsStatusWrite {
		levelStrings = append(levelStrings, "StatusWrite")
	}

	if level&ua.AccessLevelsTimestampWrite == ua.AccessLevelsTimestampWrite {
		levelStrings = append(levelStrings, "TimestampWrite")
	}
	return strings.Join(levelStrings, "|")
}

func (n *VariableNode) MarshalJSON() ([]byte, error) {
	n.Lock()
	defer n.Unlock()
	buffer := new(bytes.Buffer)
	writer := jsonwriter.New(buffer)
	writer.RootObject(func() {
		// writer.KeyValue("nodeId", n.NodeId)
		// writer.KeyValue("nodeClass", n.NodeClass)
		// writer.KeyValue("browseName", n.BrowseName)
		// writer.KeyValue("displayName", n.DisplayName)
		// writer.KeyValue("description", n.Description)
		// writer.KeyValue("internalId", fmt.Sprintf("%s", n.MustGetProperty(PropertyNameInternalId).GetValue().Value))
		// if n.parent != nil {
		// 	writer.KeyValue("parentId", fmt.Sprintf("%s", n.parent.MustGetProperty(PropertyNameInternalId).GetValue().Value))
		// }
		// writer.ArrayValues("rolePermissions", n.RolePermissions)
		// writer.Separator()
		// writer.KeyValue("accessRestrictions", n.AccessRestrictions)
		// writer.ArrayValues("references", n.References)

		writer.KeyValue("nodeId", n.NodeId.GetID())
		writer.KeyValue("ownerId", n.Owner().GetInternalId().String())
		writer.KeyValue("browseName", n.BrowseName.Name)
		writer.KeyValue("displayName", n.DisplayName.Text)
		writer.KeyValue("description", n.Description.Text)
		writer.KeyValue("value", n.Value)
		writer.KeyValue("valueRank", n.ValueRank)
		writer.ArrayValues("rolePermissions", n.RolePermissions)
		writer.Separator()
		writer.KeyValue("accessRestrictions", n.AccessRestrictions)
		writer.ArrayValues("references", n.References)
	})
	return buffer.Bytes(), nil
}
