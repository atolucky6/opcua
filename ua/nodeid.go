// Copyright 2020 Converter Systems LLC. All rights reserved.

package ua

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	uuid "github.com/google/uuid"
	"github.com/karlseguin/jsonwriter"
	"github.com/tidwall/gjson"
)

var (
	errInvalidIDType = errors.New("invalid IdType")
)

// NodeID identifies a Node.
type NodeID interface {
	GetID() Variant
	GetNamespaceIndex() uint16
	GetIDType() IDType
	String() string
}

// NodeIDNumeric is a NodeID of numeric type.
type NodeIDNumeric struct {
	IDType         IDType `json:"idType"`
	NamespaceIndex uint16 `json:"namespace,omitempty"`
	ID             uint32 `json:"id"`
}

// NewNodeIDNumeric makes a NodeID of numeric type.
func NewNodeIDNumeric(ns uint16, id uint32) NodeIDNumeric {
	return NodeIDNumeric{IDTypeNumeric, ns, id}
}

func (n NodeIDNumeric) GetID() Variant {
	return n.ID
}

func (n NodeIDNumeric) GetNamespaceIndex() uint16 {
	return n.NamespaceIndex
}

func (n NodeIDNumeric) GetIDType() IDType {
	return n.IDType
}

// String returns a string representation, e.g. "i=85"
func (n NodeIDNumeric) String() string {
	if n.NamespaceIndex == 0 {
		return fmt.Sprintf("i=%d", n.ID)
	}
	return fmt.Sprintf("ns=%d;i=%d", n.NamespaceIndex, n.ID)
}

func (n *NodeIDNumeric) UnmarshalJSON(b []byte) error {
	nodeId, err := ParseNodeIDBytes(b)
	if err != nil {
		return err
	}

	n.ID = nodeId.GetID().(uint32)
	n.IDType = nodeId.GetIDType()
	n.NamespaceIndex = nodeId.GetNamespaceIndex()
	return nil
}

func (n NodeIDNumeric) MarshalJSON() ([]byte, error) {
	buffer := new(bytes.Buffer)
	writer := jsonwriter.New(buffer)
	writer.RootObject(func() {
		writer.KeyValue("id", n.ID)
		writer.KeyValue("namespace", n.NamespaceIndex)
	})
	return buffer.Bytes(), nil
}

// NodeIDString is a NodeID of string type.
type NodeIDString struct {
	IDType         IDType `json:"idType"`
	NamespaceIndex uint16 `json:"namespace,omitempty"`
	ID             string `json:"id"`
}

// NewNodeIDString makes a NodeID of string type.
func NewNodeIDString(ns uint16, id string) NodeIDString {
	return NodeIDString{IDTypeString, ns, id}
}

func (n NodeIDString) GetID() Variant {
	return n.ID
}

func (n NodeIDString) GetNamespaceIndex() uint16 {
	return n.NamespaceIndex
}

func (n NodeIDString) GetIDType() IDType {
	return n.IDType
}

// String returns a string representation, e.g. "ns=2;s=Demo.Static.Scalar.Float"
func (n NodeIDString) String() string {
	if n.NamespaceIndex == 0 {
		return fmt.Sprintf("s=%s", n.ID)
	}
	return fmt.Sprintf("ns=%d;s=%s", n.NamespaceIndex, n.ID)
}

func (n *NodeIDString) UnmarshalJSON(b []byte) error {
	nodeId, err := ParseNodeIDBytes(b)
	if err != nil {
		return err
	}

	n.ID = nodeId.GetID().(string)
	n.IDType = nodeId.GetIDType()
	n.NamespaceIndex = nodeId.GetNamespaceIndex()
	return nil
}

func (n NodeIDString) MarshalJSON() ([]byte, error) {
	buffer := new(bytes.Buffer)
	writer := jsonwriter.New(buffer)
	writer.RootObject(func() {
		writer.KeyValue("idType", int32(n.IDType))
		writer.KeyString("id", n.ID)
		writer.KeyValue("namespace", n.NamespaceIndex)
	})
	return buffer.Bytes(), nil
}

// NodeIDGUID is a NodeID of GUID type.
type NodeIDGUID struct {
	IDType         IDType    `json:"idType"`
	NamespaceIndex uint16    `json:"namespace,omitempty"`
	ID             uuid.UUID `json:"id"`
}

// NewNodeIDGUID makes a NodeID of GUID type.
func NewNodeIDGUID(ns uint16, id uuid.UUID) NodeIDGUID {
	return NodeIDGUID{IDTypeGUID, ns, id}
}

func (n NodeIDGUID) GetID() Variant {
	return n.ID
}

func (n NodeIDGUID) GetNamespaceIndex() uint16 {
	return n.NamespaceIndex
}

func (n NodeIDGUID) GetIDType() IDType {
	return n.IDType
}

func (n *NodeIDGUID) UnmarshalJSON(b []byte) error {
	nodeId, err := ParseNodeIDBytes(b)
	if err != nil {
		return err
	}

	n.ID = nodeId.GetID().(uuid.UUID)
	n.IDType = nodeId.GetIDType()
	n.NamespaceIndex = nodeId.GetNamespaceIndex()
	return nil
}

func (n NodeIDGUID) MarshalJSON() ([]byte, error) {
	buffer := new(bytes.Buffer)
	writer := jsonwriter.New(buffer)
	writer.RootObject(func() {
		writer.KeyValue("idType", int32(n.IDType))
		writer.KeyValue("id", n.ID)
		writer.KeyValue("namespace", n.NamespaceIndex)
	})
	return buffer.Bytes(), nil
}

// String returns a string representation, e.g. "ns=2;g=5ce9dbce-5d79-434c-9ac3-1cfba9a6e92c"
func (n NodeIDGUID) String() string {
	if n.NamespaceIndex == 0 {
		return fmt.Sprintf("g=%s", n.ID)
	}
	return fmt.Sprintf("ns=%d;g=%s", n.NamespaceIndex, n.ID)
}

// NodeIDOpaque is a new NodeID of opaque type.
type NodeIDOpaque struct {
	IDType         IDType     `json:"idType"`
	NamespaceIndex uint16     `json:"namespace,omitempty"`
	ID             ByteString `json:"id"`
}

// NewNodeIDOpaque makes a NodeID of opaque type.
func NewNodeIDOpaque(ns uint16, id ByteString) NodeIDOpaque {
	return NodeIDOpaque{IDTypeOpaque, ns, id}
}

func (n NodeIDOpaque) GetID() Variant {
	return n.ID
}

func (n NodeIDOpaque) GetNamespaceIndex() uint16 {
	return n.NamespaceIndex
}

func (n NodeIDOpaque) GetIDType() IDType {
	return n.IDType
}

// String returns a string representation, e.g. "ns=2;b=YWJjZA=="
func (n NodeIDOpaque) String() string {
	if n.NamespaceIndex == 0 {
		return fmt.Sprintf("b=%s", base64.StdEncoding.EncodeToString([]byte(n.ID)))
	}
	return fmt.Sprintf("ns=%d;b=%s", n.NamespaceIndex, base64.StdEncoding.EncodeToString([]byte(n.ID)))
}

func (n *NodeIDOpaque) UnmarshalJSON(b []byte) error {
	nodeId, err := ParseNodeIDBytes(b)
	if err != nil {
		return err
	}

	n.ID = nodeId.GetID().(ByteString)
	n.IDType = nodeId.GetIDType()
	n.NamespaceIndex = nodeId.GetNamespaceIndex()
	return nil
}

func (n NodeIDOpaque) MarshalJSON() ([]byte, error) {
	buffer := new(bytes.Buffer)
	writer := jsonwriter.New(buffer)
	writer.RootObject(func() {
		writer.KeyValue("idType", int32(n.IDType))
		writer.KeyString("id", base64.StdEncoding.EncodeToString([]byte(n.ID)))
		writer.KeyValue("namespace", n.NamespaceIndex)
	})
	return buffer.Bytes(), nil
}

// ParseNodeID returns a NodeID from a string representation.
//   - ParseNodeID("i=85") // integer, assumes ns=0
//   - ParseNodeID("ns=2;s=Demo.Static.Scalar.Float") // string
//   - ParseNodeID("ns=2;g=5ce9dbce-5d79-434c-9ac3-1cfba9a6e92c") // guid
//   - ParseNodeID("ns=2;b=YWJjZA==") // opaque byte string
func ParseNodeID(v interface{}) NodeID {
	vType := fmt.Sprintf("%T", v)
	switch vType {
	case "string":
		return ParseNodeIDString(v.(string))
	case "map[string]interface {}":
		data := v.(map[string]interface{})

		id := data["id"]
		if id == nil {
			return nil
		}

		ns := uint16(0)
		if item, ok := data["namespace"]; ok {
			ns = uint16(item.(float64))
		}

		if IDType, ok := data["idType"]; ok {
			switch IDType {
			case float64(IDTypeNumeric):
				return NewNodeIDNumeric(ns, uint32(id.(float64)))
			case float64(IDTypeString):
				return NewNodeIDString(ns, id.(string))
			case float64(IDTypeGUID):
				return NewNodeIDGUID(ns, uuid.MustParse(id.(string)))
			case float64(IDTypeOpaque):
				return NewNodeIDOpaque(ns, ByteString(id.(string)))
			}
		}
	}
	return nil
}

// ParseNodeIDString returns a NodeID from a string representation.
//   - ParseNodeIDString("i=85") // integer, assumes ns=0
//   - ParseNodeIDString("ns=2;s=Demo.Static.Scalar.Float") // string
//   - ParseNodeIDString("ns=2;g=5ce9dbce-5d79-434c-9ac3-1cfba9a6e92c") // guid
//   - ParseNodeIDString("ns=2;b=YWJjZA==") // opaque byte string
func ParseNodeIDString(s string) NodeID {
	var ns uint64
	var err error
	if strings.HasPrefix(s, "ns=") {
		var pos = strings.Index(s, ";")
		if pos == -1 {
			return nil
		}
		ns, err = strconv.ParseUint(s[3:pos], 10, 16)
		if err != nil {
			return nil
		}
		s = s[pos+1:]
	}
	switch {
	case strings.HasPrefix(s, "i="):
		var id, err = strconv.ParseUint(s[2:], 10, 32)
		if err != nil {
			return nil
		}
		if id == 0 && ns == 0 {
			return nil
		}
		return NewNodeIDNumeric(uint16(ns), uint32(id))
	case strings.HasPrefix(s, "s="):
		return NewNodeIDString(uint16(ns), s[2:])
	case strings.HasPrefix(s, "g="):
		var id, err = uuid.Parse(s[2:])
		if err != nil {
			return nil
		}
		return NewNodeIDGUID(uint16(ns), id)
	case strings.HasPrefix(s, "b="):
		var id, err = base64.StdEncoding.DecodeString(s[2:])
		if err != nil {
			return nil
		}
		return NewNodeIDOpaque(uint16(ns), ByteString(id))
	}
	return nil
}

func ParseNodeIDBytes(b []byte) (NodeID, error) {
	jeIdType := gjson.GetBytes(b, "idType")
	jeId := gjson.GetBytes(b, "id")
	jeNamespace := gjson.GetBytes(b, "namespace")

	idType := IDType(int32(jeIdType.Int()))
	switch idType {
	case IDTypeNumeric:
		var id uint32
		err := json.Unmarshal([]byte(jeId.Raw), &id)
		if err != nil {
			return nil, err
		}
		return NodeIDNumeric{
			NamespaceIndex: uint16(jeNamespace.Uint()),
			IDType:         idType,
			ID:             id,
		}, nil
	case IDTypeString:
		var id string
		err := json.Unmarshal([]byte(jeId.Raw), &id)
		if err != nil {
			return nil, err
		}
		return NodeIDString{
			NamespaceIndex: uint16(jeNamespace.Uint()),
			IDType:         idType,
			ID:             id,
		}, nil
	case IDTypeGUID:
		var id uuid.UUID
		err := json.Unmarshal([]byte(jeId.Raw), &id)
		if err != nil {
			return nil, err
		}
		return NodeIDGUID{
			NamespaceIndex: uint16(jeNamespace.Uint()),
			IDType:         idType,
			ID:             id,
		}, nil
	case IDTypeOpaque:
		var id string
		err := json.Unmarshal([]byte(jeId.Raw), &id)
		if err != nil {
			return nil, err
		}
		return NodeIDOpaque{
			NamespaceIndex: uint16(jeNamespace.Uint()),
			IDType:         idType,
			ID:             ByteString(id),
		}, nil
	}

	return nil, errInvalidIDType
}

// ToExpandedNodeID converts the NodeID to an ExpandedNodeID.
// Note: When creating a reference, and the target NodeID is a local node,
// use: NewExpandedNodeID(nodeId)
func ToExpandedNodeID(n NodeID, namespaceURIs []string) ExpandedNodeID {
	switch n2 := n.(type) {
	case NodeIDNumeric:
		if n2.NamespaceIndex > 0 && n2.NamespaceIndex < uint16(len(namespaceURIs)) {
			return ExpandedNodeID{n.GetIDType(), 0, namespaceURIs[n2.NamespaceIndex], n}
		}
		return ExpandedNodeID{NodeID: n}
	case NodeIDString:
		if n2.NamespaceIndex > 0 && n2.NamespaceIndex < uint16(len(namespaceURIs)) {
			return ExpandedNodeID{n.GetIDType(), 0, namespaceURIs[n2.NamespaceIndex], n}
		}
		return ExpandedNodeID{NodeID: n}
	case NodeIDGUID:
		if n2.NamespaceIndex > 0 && n2.NamespaceIndex < uint16(len(namespaceURIs)) {
			return ExpandedNodeID{n.GetIDType(), 0, namespaceURIs[n2.NamespaceIndex], n}
		}
		return ExpandedNodeID{NodeID: n}
	case NodeIDOpaque:
		if n2.NamespaceIndex > 0 && n2.NamespaceIndex < uint16(len(namespaceURIs)) {
			return ExpandedNodeID{n.GetIDType(), 0, namespaceURIs[n2.NamespaceIndex], n}
		}
		return ExpandedNodeID{NodeID: n}
	default:
		return NilExpandedNodeID
	}
}
