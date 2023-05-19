// Copyright 2021 Converter Systems LLC. All rights reserved.

package server

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/afs/server/pkg/opcua/ua"
	"github.com/emirpasic/gods/lists/arraylist"
	"github.com/google/uuid"
	"github.com/karlseguin/jsonwriter"
)

type IDValue struct {
	ID    uuid.UUID
	Value interface{}
}

// ObjectNode ...
type ObjectNode struct {
	sync.RWMutex
	NodeId             ua.NodeID
	NodeClass          ua.NodeClass
	BrowseName         ua.QualifiedName
	DisplayName        ua.LocalizedText
	Description        ua.LocalizedText
	RolePermissions    []ua.RolePermissionType
	AccessRestrictions uint16
	References         []ua.Reference

	// extend properties
	ctx           context.Context
	parent        *ObjectNode
	properties    map[string]*VariableNode
	childs        *arraylist.List
	plugin        Plugin
	pluginProps   PluginProps
	nodeType      NodeType
	eventNotifier byte
	subs          map[EventListener]struct{}
	entry         bool
	isUpdating    bool
}

var _ Node = (*ObjectNode)(nil)

// NewObjectNode ...
func NewObjectNode(nodeID ua.NodeID, browseName ua.QualifiedName, displayName ua.LocalizedText, description ua.LocalizedText, rolePermissions []ua.RolePermissionType, references []ua.Reference, eventNotifier byte) *ObjectNode {
	return &ObjectNode{
		NodeId:             nodeID,
		NodeClass:          ua.NodeClassObject,
		BrowseName:         browseName,
		DisplayName:        displayName,
		Description:        description,
		RolePermissions:    rolePermissions,
		AccessRestrictions: 0,
		References:         references,
		eventNotifier:      eventNotifier,
		subs:               map[EventListener]struct{}{},
	}
}

// NewDefaultObjectNode returns new instance of ObjectNode
func NewDefaultObjectNode(
	parent *ObjectNode,
	name ua.QualifiedName,
	displayName ua.LocalizedText,
	description ua.LocalizedText,
	nodeType ua.DataValue,
	pluginID ua.DataValue,
	internalID ua.DataValue,
	ctx context.Context) *ObjectNode {

	id := name.Name
	if parent != nil {
		id = parent.GetNodeID().GetID().(string) + PathSeparator + name.Name
		id = strings.TrimLeft(id, PathSeparator)
	}

	n := NewObjectNode(
		ua.NewNodeIDString(DefaultNameSpace, id),
		name,
		displayName,
		description,
		nil,
		[]ua.Reference{},
		0,
	)
	n.parent = parent
	n.ctx = ctx
	n.nodeType = NodeType(nodeType.Value.(int64))
	n.NodeClass = n.nodeType.GetNodeClass()
	n.plugin = n.ctx.Value(CtxKeyPluginManager).(*PluginManager).GetPlugin(pluginID.Value.(int16))
	n.properties = map[string]*VariableNode{}
	n.childs = arraylist.New()

	// create ParentID property
	if parent != nil {
		n.References = append(n.References, ua.NewReference(ua.ReferenceTypeIDHasComponent, true, ua.NewExpandedNodeID(parent.GetNodeID())))
	}

	// create InternalId property
	propInternalID := NewVariableNode(
		ua.NewNodeIDString(DefaultNameSpace, id+PathSeparator+PropertyNameInternalId),
		ua.NewQualifiedName(DefaultNameSpace, PropertyNameInternalId),
		ua.NewLocalizedText(PropertyNameInternalId, DefaultLocale),
		ua.NewLocalizedText(PropertyDescInternalId, DefaultLocale),
		nil,
		[]ua.Reference{
			ua.NewReference(ua.ReferenceTypeIDHasTypeDefinition, false, ua.NewExpandedNodeID(ua.VariableTypeIDPropertyType)),
			ua.NewReference(ua.ReferenceTypeIDHasProperty, true, ua.NewExpandedNodeID(n.NodeId)),
		},
		internalID,
		ua.DataTypeIDGUID,
		ua.ValueRankScalar,
		[]uint32{},
		ua.AccessLevelsCurrentRead,
		-1,
		false,
		nil,
	)
	propInternalID.SetOwner(n)
	n.properties[PropertyNameInternalId] = propInternalID

	// create PluginId property
	propPluginId := NewVariableNode(
		ua.NewNodeIDString(DefaultNameSpace, id+PathSeparator+PropertyNamePluginId),
		ua.NewQualifiedName(DefaultNameSpace, PropertyNamePluginId),
		ua.NewLocalizedText(PropertyNamePluginId, DefaultLocale),
		ua.NewLocalizedText(PropertyDescPluginId, DefaultLocale),
		nil,
		[]ua.Reference{
			ua.NewReference(ua.ReferenceTypeIDHasTypeDefinition, false, ua.NewExpandedNodeID(ua.VariableTypeIDPropertyType)),
			ua.NewReference(ua.ReferenceTypeIDHasProperty, true, ua.NewExpandedNodeID(n.NodeId)),
		},
		pluginID,
		ua.DataTypeIDInt16,
		ua.ValueRankScalar,
		[]uint32{},
		ua.AccessLevelsCurrentRead,
		-1,
		false,
		nil,
	)
	propPluginId.SetOwner(n)
	n.properties[PropertyNamePluginId] = propPluginId

	// create NodeType property
	propNodeType := NewVariableNode(
		ua.NewNodeIDString(DefaultNameSpace, id+PathSeparator+PropertyNameNodeType),
		ua.NewQualifiedName(DefaultNameSpace, PropertyNameNodeType),
		ua.NewLocalizedText(PropertyNameNodeType, DefaultLocale),
		ua.NewLocalizedText(PropertyDescNodeType, DefaultLocale),
		nil,
		[]ua.Reference{
			ua.NewReference(ua.ReferenceTypeIDHasTypeDefinition, false, ua.NewExpandedNodeID(ua.VariableTypeIDPropertyType)),
			ua.NewReference(ua.ReferenceTypeIDHasProperty, true, ua.NewExpandedNodeID(n.NodeId)),
		},
		nodeType,
		ua.DataTypeIDInt64,
		ua.ValueRankScalar,
		[]uint32{},
		ua.AccessLevelsCurrentRead,
		-1,
		false,
		nil,
	)
	propNodeType.SetOwner(n)
	n.properties[PropertyNameNodeType] = propNodeType
	if n.nodeType.IsRoot() {
		n.References = append(n.References, ua.NewReference(ua.ReferenceTypeIDOrganizes, true, ua.NewExpandedNodeID(ua.ObjectIDObjectsFolder)))
	}
	n.References = append(n.References, ua.NewReference(ua.ReferenceTypeIDHasTypeDefinition, false, ua.NewExpandedNodeID(n.nodeType.GetTypeID())))

	// create Entry property
	n.entry = n.plugin.IsPluginEntry(n)
	propEntry := NewVariableNode(
		ua.NewNodeIDString(DefaultNameSpace, id+PathSeparator+PropertyNameEntry),
		ua.NewQualifiedName(DefaultNameSpace, PropertyNameEntry),
		ua.NewLocalizedText(PropertyNameEntry, DefaultLocale),
		ua.NewLocalizedText(PropertyDescEntry, DefaultLocale),
		nil,
		[]ua.Reference{
			ua.NewReference(ua.ReferenceTypeIDHasTypeDefinition, false, ua.NewExpandedNodeID(ua.VariableTypeIDPropertyType)),
			ua.NewReference(ua.ReferenceTypeIDHasProperty, true, ua.NewExpandedNodeID(n.NodeId)),
		},
		ua.NewDataValue(n.entry, ua.Good, time.Now(), 0, time.Now(), 0),
		ua.DataTypeIDBoolean,
		ua.ValueRankScalar,
		[]uint32{},
		ua.AccessLevelsCurrentRead,
		-1,
		false,
		nil,
	)
	propEntry.SetOwner(n)
	n.properties[PropertyNameEntry] = propEntry

	if n.nodeType == NodeTypeTag {
		// create Value property
		propValue := NewVariableNode(
			ua.NewNodeIDString(DefaultNameSpace, id+PathSeparator+PropertyNameValue),
			ua.NewQualifiedName(DefaultNameSpace, PropertyNameValue),
			ua.NewLocalizedText(PropertyNameValue, DefaultLocale),
			ua.NewLocalizedText(PropertyDescValue, DefaultLocale),
			nil,
			[]ua.Reference{
				ua.NewReference(ua.ReferenceTypeIDHasTypeDefinition, false, ua.NewExpandedNodeID(ua.VariableTypeIDPropertyType)),
				ua.NewReference(ua.ReferenceTypeIDHasProperty, true, ua.NewExpandedNodeID(n.NodeId)),
			},
			ua.NewDataValue(nil, ua.Good, time.Now(), 0, time.Now(), 0),
			ua.DataTypeIDString,
			ua.ValueRankScalar,
			[]uint32{},
			ua.AccessLevelsCurrentRead,
			-1,
			false,
			nil,
		)
		propValue.SetOwner(n)
		n.properties[PropertyNameValue] = propValue
	}
	return n
}

// Create new ObjectNode using key, value of FieldMap
func NewObjectNodeWithProperties(
	parent *ObjectNode,
	nodeType ua.DataValue,
	pluginID ua.DataValue,
	internalID ua.DataValue,
	fm FieldMap,
	ctx context.Context) (*ObjectNode, map[string]error) {

	fm.NormalizeFieldName()
	fieldErrors := map[string]error{}
	name, err := fm.GetString(PropertyNameBrowseName)
	if err != nil {
		fieldErrors["PropertyNameBrowseName"] = err
	}

	displayName, err := fm.GetString(PropertyNameDisplayName)
	if err != nil {
		fieldErrors["PropertyNameDisplayName"] = err
	}

	description, err := fm.GetString(PropertyNameDescription)
	if err != nil {
		fieldErrors["PropertyNameDescription"] = err
	}

	// create ObjectNode
	node := NewDefaultObjectNode(
		parent,
		ua.NewQualifiedName(DefaultNameSpace, name),
		ua.NewLocalizedText(displayName, DefaultLocale),
		ua.NewLocalizedText(description, DefaultLocale),
		nodeType,
		pluginID,
		internalID,
		ctx,
	)
	nodeID := node.GetNodeID().GetID().(string)

	fm.RemoveNonPluginFields(node.plugin.GetPluginConfig(), node.nodeType)
	for k, v := range fm {
		if _, ok := node.properties[k]; !ok {
			if valid, validValue, fe := node.CheckPropertyValue(k, v); valid {
				if fe != nil {
					fieldErrors[k] = fe
				} else {
					fd := node.plugin.GetPluginConfig().GetFieldDef(k, node.nodeType)
					propNode := NewVariableNode(
						ua.NewNodeIDString(DefaultNameSpace, nodeID+PathSeparator+fd.Name),
						ua.NewQualifiedName(DefaultNameSpace, fd.Name),
						ua.NewLocalizedText(fd.DisplayName, DefaultLocale),
						ua.NewLocalizedText(fd.Description, DefaultLocale),
						nil,
						[]ua.Reference{
							ua.NewReference(ua.ReferenceTypeIDHasTypeDefinition, false, ua.NewExpandedNodeID(ua.VariableTypeIDPropertyType)),
							ua.NewReference(ua.ReferenceTypeIDHasProperty, true, ua.NewExpandedNodeID(node.GetNodeID())),
						},
						ua.NewDataValue(validValue, ua.Good, time.Now(), 0, time.Now(), 0),
						fd.GetDataTypeID(),
						ua.ValueRankScalar,
						[]uint32{},
						ua.AccessLevelsCurrentRead|ua.AccessLevelsCurrentWrite|ua.AccessLevelsHistoryRead,
						-1,
						false,
						nil,
					)
					propNode.SetOwner(node)
					node.properties[propNode.BrowseName.Name] = propNode
				}
			}
		}
	}

	return node, fieldErrors
}

// NodeID returns the NodeID attribute of this node.
func (n *ObjectNode) GetNodeID() ua.NodeID {
	return n.NodeId
}

func (n *ObjectNode) SetNodeID(id ua.NodeID) {
	n.Lock()
	defer n.Unlock()
	if n.NodeId != id {
		oldID := n.NodeId
		n.NodeId = id
		n.Context().Value(CtxKeyProjectManager).(*ProjectManager).ReplaceNodeID(oldID, id)
	}
}

func (n *ObjectNode) ReplaceNodeIDPrefix(oldPrefix, newPrefix string) {
	currentID := []byte{}
	currentID = append(currentID, newPrefix...)
	currentID = append(currentID, n.GetNodeID().GetID().(string)[len(oldPrefix):]...)
	n.SetNodeID(ua.NewNodeIDString(DefaultNameSpace, string(currentID)))
}

// NodeClass returns the NodeClass attribute of this node.
func (n *ObjectNode) GetNodeClass() ua.NodeClass {
	return n.NodeClass
}

// BrowseName returns the BrowseName attribute of this node.
func (n *ObjectNode) GetBrowseName() ua.QualifiedName {
	return n.BrowseName
}

// DisplayName returns the DisplayName attribute of this node.
func (n *ObjectNode) GetDisplayName() ua.LocalizedText {
	return n.DisplayName
}

// Description returns the Description attribute of this node.
func (n *ObjectNode) GetDescription() ua.LocalizedText {
	return n.Description
}

// RolePermissions returns the RolePermissions attribute of this node.
func (n *ObjectNode) GetRolePermissions() []ua.RolePermissionType {
	return n.RolePermissions
}

// UserRolePermissions returns the RolePermissions attribute of this node for the current user.
func (n *ObjectNode) GetUserRolePermissions(ctx context.Context) []ua.RolePermissionType {
	filteredPermissions := []ua.RolePermissionType{}
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
	return filteredPermissions
}

// References returns the References of this node.
func (n *ObjectNode) GetReferences() []ua.Reference {
	n.RLock()
	res := n.References
	n.RUnlock()
	return res
}

// SetReferences sets the References of the Variable.
func (n *ObjectNode) SetReferences(value []ua.Reference) {
	n.Lock()
	n.References = value
	n.Unlock()
}

// EventNotifier returns the EventNotifier attribute of this node.
func (n *ObjectNode) EventNotifier() byte {
	return n.eventNotifier
}

// OnEvent raises an event from this node.
func (n *ObjectNode) OnEvent(evt ua.Event) {
	n.RLock()
	defer n.RUnlock()
	for sub := range n.subs {
		sub.OnEvent(evt)
	}
}

type EventListener interface {
	OnEvent(ua.Event)
}

func (n *ObjectNode) AddEventListener(listener EventListener) {
	n.Lock()
	n.subs[listener] = struct{}{}
	n.Unlock()
}

func (n *ObjectNode) RemoveEventListener(listener EventListener) {
	n.Lock()
	delete(n.subs, listener)
	n.Unlock()
}

// IsAttributeIDValid returns true if attributeId is supported for the node.
func (n *ObjectNode) IsAttributeIDValid(attributeID uint32) bool {
	switch attributeID {
	case ua.AttributeIDNodeID, ua.AttributeIDNodeClass, ua.AttributeIDBrowseName,
		ua.AttributeIDDisplayName, ua.AttributeIDDescription, ua.AttributeIDRolePermissions,
		ua.AttributeIDUserRolePermissions, ua.AttributeIDEventNotifier:
		return true
	default:
		return false
	}
}

// ===========================================================
// ObjectNodeExtend implements
// ===========================================================

// SetBrowseName set BrowseName attribute of this node
func (n *ObjectNode) SetBrowseName(value string) error {
	n.Lock()
	defer n.Unlock()
	if n.BrowseName.Name != value {
		n.BrowseName.Name = value
		namespaceManager := n.Context().Value(CtxKeyNamespaceManager).(*NamespaceManager)

		// update new NodeId
		id := n.BrowseName.Name
		if n.parent != nil {
			id = n.parent.NodeId.GetID().(string) + PathSeparator + n.BrowseName.Name
		}
		newNodeID := ua.NewNodeIDString(DefaultNameSpace, id)
		n.Unlock()
		namespaceManager.UpdateNodeID(n, newNodeID)
		n.Lock()
	}
	return nil
}

// SetDisplayName set DisplayName attribute of this node
func (n *ObjectNode) SetDisplayName(value string) error {
	n.Lock()
	defer n.Unlock()
	n.DisplayName.Text = value
	return nil
}

// SetDescription set Description attribute of this node
func (n *ObjectNode) SetDescription(value string) error {
	n.Lock()
	defer n.Unlock()
	n.Description.Text = value
	return nil
}

// GetProperties returns all name property map of this node
func (n *ObjectNode) GetProperties() map[string]*VariableNode {
	return n.properties
}

// GetChilds returns all child of this node
func (n *ObjectNode) GetChilds() *arraylist.List {
	return n.childs
}

// GetNodeType returns the GetNodeType of this node
func (n *ObjectNode) GetNodeType() NodeType {
	return n.nodeType
}

// IsEntry returns true if this node is an entry for plugin
func (n *ObjectNode) IsEntry() bool {
	return n.entry
}

// GetParent returns the parent that has this node
func (n *ObjectNode) GetParent() *ObjectNode {
	return n.parent
}

// GetPlugin returns an plugin of this node
func (n *ObjectNode) GetPlugin() Plugin {
	return n.plugin
}

// GetPluginProps returns an PluginPros that use in Plugin
func (n *ObjectNode) GetPluginProps() PluginProps {
	return n.pluginProps
}

// AssignPluginProps assign plugin properties for node
func (n *ObjectNode) AssignPluginProps() {
	if n.pluginProps == nil {
		n.pluginProps = n.plugin.GetPluginProps(n)
		if n.pluginProps != nil {
			n.pluginProps.AssignNode(n)
		}
	}
}

// GetInternalId returns an internal id of this ObjectNode
func (n *ObjectNode) GetInternalId() uuid.UUID {
	return n.MustGetProperty(PropertyNameInternalId).Value.Value.(uuid.UUID)
}

// GetProperty returns an property of this ObjectNode by specified property name
func (n *ObjectNode) GetProperty(propName string) (*VariableNode, bool) {
	propNode, found := n.properties[propName]
	return propNode, found
}

// MustGetProperty returns an property of this ObjectNode by specified property name
// will panic if property not exists
func (n *ObjectNode) MustGetProperty(propName string) *VariableNode {
	prop, ok := n.GetProperty(propName)
	if !ok || prop == nil {
		log.Panicf("property '%s' not found in node Name: %s, ID: %s", propName, n.BrowseName.Name, n.NodeId.String())
	}
	return prop
}

// AddProperty add a property to this node
func (n *ObjectNode) AddProperty(propNode *VariableNode) error {
	if _, ok := n.properties[propNode.BrowseName.Name]; ok {
		return ErrFieldExisted
	}

	if propNode.BrowseName.Name != PropertyNameValue {
		valid, _, fe := n.CheckPropertyValue(propNode.BrowseName.Name, propNode.Value.Value)
		if !valid {
			return ErrInvalidField
		}

		if fe != nil {
			return fe
		}
	}

	propNode.SetOwner(n)
	n.properties[propNode.BrowseName.Name] = propNode
	return nil
}

// Dispose will release all resources of this node
func (n *ObjectNode) Dispose() {

}

// GetFullPath returns the full path to access this node
func (n *ObjectNode) GetFullPath() string {
	n.RLock()
	defer n.RUnlock()
	if n.parent != nil {
		// get the parent NodeType property
		if parentNodeType, ok := n.parent.GetProperty(PropertyNameNodeType); ok {
			if NodeType(parentNodeType.GetValue().Value.(int64)) != NodeTypeRoot {
				return n.parent.GetFullPath() + PathSeparator + n.BrowseName.Name
			} else {
				return n.BrowseName.Name
			}
		}
	}
	return PathSeparator + n.BrowseName.Name
}

// CanAddChild returns true if this node can add node with specified node type
func (n *ObjectNode) CanAddChild(nodeType NodeType) bool {
	if n.plugin == nil {
		return false
	}
	return n.plugin.CanAddNodeType(n, nodeType)
}

// AddChild add an node into childs
func (n *ObjectNode) AddChild(child *ObjectNode) error {
	n.Lock()
	defer n.Unlock()

	childType := child.GetNodeType()
	if !n.CanAddChild(childType) {
		return ErrNodeTypeNotAccepted
	}
	n.childs.Add(child)
	n.plugin.AddNode(n, child)
	return nil
}

// InsertChild insert a node into specified index
func (n *ObjectNode) InsertChild(index int, child *ObjectNode) error {
	n.Lock()
	defer n.Unlock()

	propNodeType := child.MustGetProperty(PropertyNameNodeType)
	childType := NodeType(propNodeType.GetValue().Value.(int64))
	if !n.CanAddChild(childType) {
		return ErrNodeTypeNotAccepted
	}

	n.childs.Insert(index, child)
	n.plugin.AddNode(n, child)
	return nil
}

// MoveBefore will move the specified node before the target node
func (n *ObjectNode) MoveBefore(node *ObjectNode, target *ObjectNode) error {
	n.Lock()
	defer n.Unlock()

	index := n.childs.IndexOf(node)
	if index < 0 {
		return ErrInvalidIndex
	}

	targetIndex := n.childs.IndexOf(target)
	if targetIndex < 0 {
		return ErrInvalidIndex
	}

	if index != targetIndex {
		n.childs.Remove(index)
		if index < targetIndex {
			targetIndex--
		}
		n.childs.Insert(targetIndex, node)
	}
	return nil
}

// MoveBefore will move the specified node to the last
func (n *ObjectNode) MoveToLast(node *ObjectNode) error {
	n.Lock()
	defer n.Unlock()

	index := n.childs.IndexOf(node)
	if index < 0 {
		return ErrInvalidIndex
	}
	n.childs.Remove(index)
	n.childs.Add(node)
	return nil
}

// RemoveChild remove an specified node from childs
func (n *ObjectNode) RemoveChild(child *ObjectNode) error {
	n.Lock()
	defer n.Unlock()

	index := n.childs.IndexOf(child)
	if index < 0 {
		return ErrInvalidIndex
	}
	n.childs.Remove(index)
	n.plugin.RemoveNode(n, child)
	child.Dispose()
	return nil
}

// Update the node via FieldMap
func (n *ObjectNode) Update(fm FieldMap) map[string]error {
	fieldErrors := map[string]error{}
	validFields := FieldMap{}

	fm.NormalizeFieldName()

	name, err := fm.GetString(PropertyNameBrowseName)
	if err == nil {
		_, validValue, fe := n.CheckPropertyValue(PropertyNameBrowseName, name)
		if fe != nil {
			fieldErrors[PropertyNameBrowseName] = fe
		} else {
			validFields[PropertyNameBrowseName] = validValue
		}
	}

	displayName, err := fm.GetString(PropertyNameDisplayName)
	if err == nil {
		_, validValue, fe := n.CheckPropertyValue(PropertyNameDisplayName, displayName)
		if fe != nil {
			fieldErrors[PropertyNameDisplayName] = fe
		} else {
			validFields[PropertyNameDisplayName] = validValue
		}
	}

	description, err := fm.GetString(PropertyNameDescription)
	if err == nil {
		_, validValue, fe := n.CheckPropertyValue(PropertyNameDescription, description)
		if fe != nil {
			fieldErrors[PropertyNameDescription] = fe
		} else {
			validFields[PropertyNameDescription] = validValue
		}
	}

	fm.RemoveNonPluginFields(n.plugin.GetPluginConfig(), n.nodeType)
	if len(fieldErrors) == 0 {
		fes, vf := n.plugin.CheckUpdateValid(n, fm)
		for k, v := range vf {
			validFields[k] = v
		}
		for k, err := range fes {
			fieldErrors[k] = err
		}
	}

	n.BeginUpdate()
	hasChanged := false
	if len(fieldErrors) == 0 {
		for k, v := range validFields {
			switch k {
			case PropertyNameBrowseName:
				n.SetBrowseName(v.(string))
			case PropertyNameDisplayName:
				n.SetDisplayName(v.(string))
			case PropertyNameDescription:
				n.SetDescription((v.(string)))
			default:
				if !hasChanged {
					hasChanged = n.properties[k].SetValue(ua.NewDataValue(v, ua.Good, time.Now(), 0, time.Now(), 0))
				} else {
					n.properties[k].SetValue(ua.NewDataValue(v, ua.Good, time.Now(), 0, time.Now(), 0))
				}

			}
		}
	}
	if hasChanged {
		if n.pluginProps != nil {
			n.pluginProps.UpdateProps()
		}
	}
	n.EndUpdate()

	return fieldErrors
}

// BeginUpdate notify this node was being update
func (n *ObjectNode) BeginUpdate() {
	n.isUpdating = true
}

// EndUpdate notify this node was updated
func (n *ObjectNode) EndUpdate() {
	n.isUpdating = false
}

// First retuns the first child node that match with specified predicate
func (n *ObjectNode) First(predicate func(child *ObjectNode) bool) *ObjectNode {
	for _, item := range n.childs.Values() {
		if predicate(item.(*ObjectNode)) {
			return item.(*ObjectNode)
		}
	}
	return nil
}

// DescendantParentFirst returns the first parent that match with specified predicate
func (n *ObjectNode) DescendantParentFirst(predicate func(node *ObjectNode) bool) *ObjectNode {
	if n.parent == nil {
		return nil
	}
	if predicate(n.parent) {
		return n.parent
	}
	return n.parent.DescendantParentFirst(predicate)
}

// GetChildByPath returns the depth child node that match with full path
func (n *ObjectNode) GetChildByPath(path string) *ObjectNode {
	if len(path) > 0 {
		pathSplit := strings.SplitN(path, PathSeparator, 2)
		if len(pathSplit) == 1 {
			for _, child := range n.childs.Values() {
				if child.(Node).GetBrowseName().Name == pathSplit[0] {
					return child.(*ObjectNode)
				}
			}
		} else {
			for _, child := range n.childs.Values() {
				if child.(Node).GetBrowseName().Name == pathSplit[0] {
					return child.(*ObjectNode).GetChildByPath(pathSplit[1])
				}
			}
		}
	}
	return nil
}

// ForEach loop through it childs
func (n *ObjectNode) ForEach(action func(child *ObjectNode)) {
	for _, item := range n.childs.Values() {
		action(item.(*ObjectNode))
	}
}

// ForEachSelf loop through childs and it self
func (n *ObjectNode) ForEachSelf(action func(child *ObjectNode)) {
	action(n)
	n.ForEach(action)
}

// ForEachDepth loop through childs and dept childs
func (n *ObjectNode) ForEachDepth(action func(child *ObjectNode)) {
	for _, item := range n.childs.Values() {
		action(item.(*ObjectNode))
		item.(*ObjectNode).ForEachDepth(action)
	}
}

// ForEachSelfDepth loop through childs and it self and dept childs
func (n *ObjectNode) ForEachSelfDepth(action func(child *ObjectNode)) {
	action(n)
	for _, item := range n.childs.Values() {
		item.(*ObjectNode).ForEachSelfDepth(action)
	}
}

// Validate check all of property and attribute of this node if it valid
func (n *ObjectNode) Validate() map[string]error {
	fieldErros := map[string]error{}
	fe := n.ValidateProperty(PropertyNameBrowseName)
	if fe != nil {
		fieldErros[PropertyNameBrowseName] = fe
	}
	fe = n.ValidateProperty(PropertyNameDisplayName)
	if fe != nil {
		fieldErros[PropertyNameDisplayName] = fe
	}
	fe = n.ValidateProperty(PropertyNameDescription)
	if fe != nil {
		fieldErros[PropertyNameDescription] = fe
	}
	for k, err := range n.plugin.Validate(n) {
		fieldErros[k] = err
	}
	return fieldErros
}

// ValidateProperty check the value of specified property if it valid
func (n *ObjectNode) ValidateProperty(name string) error {
	switch name {
	case PropertyNameBrowseName:
		_, _, fe := n.CheckPropertyValue(name, n.BrowseName.Name)
		return fe
	case PropertyNameDisplayName:
		_, _, fe := n.CheckPropertyValue(name, n.DisplayName.Text)
		return fe
	case PropertyNameDescription:
		_, _, fe := n.CheckPropertyValue(name, n.Description.Text)
		return fe
	}

	if prop, ok := n.properties[name]; ok {
		return prop.Validate()
	}

	return ErrInvalidField
}

// CheckPropertyValue will check the property name and the value is valid for add or update the node
func (n *ObjectNode) CheckPropertyValue(name string, value interface{}) (bool, interface{}, error) {
	if name == PropertyNameBrowseName {
		return CheckBrowseName(value, n, n.parent)
	} else if name == PropertyNameDisplayName {
		return CheckDisplayName(value, n, n.parent)
	} else if name == PropertyNameDescription {
		return CheckDescription(value, n, n.parent)
	}
	return n.GetPlugin().CheckPropertyValue(n, name, value)
}

// Context returns the current context of this node
func (n *ObjectNode) Context() context.Context {
	return n.ctx
}

func (n *ObjectNode) MarshalJSON() ([]byte, error) {
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
		writer.KeyValue("internalId", fmt.Sprintf("%s", n.MustGetProperty(PropertyNameInternalId).GetValue().Value))
		if n.parent != nil {
			writer.KeyValue("parentId", fmt.Sprintf("%s", n.parent.MustGetProperty(PropertyNameInternalId).GetValue().Value))
		}
		writer.KeyValue("pluginId", n.MustGetProperty(PropertyNamePluginId).GetValue().Value)
		writer.KeyValue("nodeType", n.nodeType)
		writer.KeyValue("browseName", n.BrowseName.Name)
		writer.KeyValue("displayName", n.DisplayName.Text)
		writer.KeyValue("description", n.Description.Text)
		writer.Object("properties", func() {
			for _, prop := range n.properties {
				writer.KeyValue(prop.BrowseName.Name, prop)
			}
		})
		writer.ArrayValues("rolePermissions", n.RolePermissions)
		writer.Separator()
		writer.KeyValue("accessRestrictions", n.AccessRestrictions)
		writer.ArrayValues("references", n.References)
	})
	return buffer.Bytes(), nil
}

// implements Filterable for filter
func (n *ObjectNode) GetPropertyValue(propName string) (interface{}, error) {
	switch propName {
	case "BrowseName":
		return n.BrowseName.Name, nil
	case "DisplayName":
		return n.DisplayName.Text, nil
	case "Description":
		return n.Description.Text, nil
	case "NodeType":
		return n.nodeType, nil
	case "PluginId":
		return n.plugin.GetId(), nil
	case "NodeId":
		return n.NodeId.String(), nil
	case "InternalId":
		return n.MustGetProperty(PropertyNameInternalId).GetValue().Value, nil
	}

	return nil, ErrInvalidField
}
