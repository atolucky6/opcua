// Copyright 2020 Converter Systems LLC. All rights reserved.

package ua

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/karlseguin/jsonwriter"
	"github.com/tidwall/gjson"
)

// ExpandedNodeID identifies a remote Node.
type ExpandedNodeID struct {
	IdType       IDType `json:"idType"`
	ServerIndex  uint32 `json:"serverUri,omitempty"`
	NamespaceURI string `json:"namespace,omitempty"`
	NodeID       NodeID `json:"id"`
}

func NewExpandedNodeID(nodeID NodeID) ExpandedNodeID {
	return ExpandedNodeID{nodeID.GetIDType(), 0, "", nodeID}
}

func (eni *ExpandedNodeID) UnmarshalJSON(b []byte) error {
	jeIdType := gjson.GetBytes(b, "idType")
	eni.IdType = IDType(int32(jeIdType.Int()))

	jeNamespace := gjson.GetBytes(b, "namespace")
	eni.NamespaceURI = jeNamespace.Raw

	jeServerUri := gjson.GetBytes(b, "serverUri")
	eni.ServerIndex = uint32(jeServerUri.Uint())

	jeId := gjson.GetBytes(b, "id")
	switch eni.IdType {
	case IDTypeNumeric:
		var id uint32
		err := json.Unmarshal([]byte(jeId.Raw), &id)
		if err != nil {
			return err
		}
		eni.NodeID = NodeIDNumeric{
			NamespaceIndex: uint16(jeNamespace.Uint()),
			IDType:         eni.IdType,
			ID:             id,
		}
	case IDTypeString:
		var id string
		err := json.Unmarshal([]byte(jeId.Raw), &id)
		if err != nil {
			return err
		}
		eni.NodeID = NodeIDString{
			NamespaceIndex: uint16(jeNamespace.Uint()),
			IDType:         eni.IdType,
			ID:             id,
		}
	case IDTypeGUID:
		var id uuid.UUID
		err := json.Unmarshal([]byte(jeId.Raw), &id)
		if err != nil {
			return err
		}
		eni.NodeID = NodeIDGUID{
			NamespaceIndex: uint16(jeNamespace.Uint()),
			IDType:         eni.IdType,
			ID:             id,
		}
	case IDTypeOpaque:
		var id string
		err := json.Unmarshal([]byte(jeId.Raw), &id)
		if err != nil {
			return err
		}
		eni.NodeID = NodeIDOpaque{
			NamespaceIndex: uint16(jeNamespace.Uint()),
			IDType:         eni.IdType,
			ID:             ByteString(id),
		}
	}
	return nil
}

func (eni ExpandedNodeID) MarshalJSON() ([]byte, error) {
	buffer := new(bytes.Buffer)
	writer := jsonwriter.New(buffer)
	writer.RootObject(func() {
		if eni.IdType != 0 {
			writer.KeyValue("idType", int32(eni.IdType))
		}
		writer.KeyValue("id", eni.NodeID.GetID())

		if len(eni.NamespaceURI) > 0 {
			writer.KeyValue("namespace", eni.NamespaceURI)
		}

		if eni.ServerIndex != 0 {
			writer.KeyValue("serverUri", eni.ServerIndex)
		}
	})
	return buffer.Bytes(), nil
}

// NilExpandedNodeID is the nil value.
var NilExpandedNodeID = ExpandedNodeID{IDTypeNumeric, 0, "", nil}

// ParseExpandedNodeID returns a NodeID from a string representation.
//   - ParseExpandedNodeID("i=85") // integer, assumes nsu=http://opcfoundation.org/UA/
//   - ParseExpandedNodeID("nsu=http://www.unifiedautomation.com/DemoServer/;s=Demo.Static.Scalar.Float") // string
//   - ParseExpandedNodeID("nsu=http://www.unifiedautomation.com/DemoServer/;g=5ce9dbce-5d79-434c-9ac3-1cfba9a6e92c") // guid
//   - ParseExpandedNodeID("nsu=http://www.unifiedautomation.com/DemoServer/;b=YWJjZA==") // opaque byte string
func ParseExpandedNodeID(s string) ExpandedNodeID {
	var svr uint64
	var err error
	if strings.HasPrefix(s, "svr=") {
		var pos = strings.Index(s, ";")
		if pos == -1 {
			return NilExpandedNodeID
		}

		svr, err = strconv.ParseUint(s[4:pos], 10, 32)
		if err != nil {
			return NilExpandedNodeID
		}
		s = s[pos+1:]
	}

	var nsu string
	if strings.HasPrefix(s, "nsu=") {
		var pos = strings.Index(s, ";")
		if pos == -1 {
			return NilExpandedNodeID
		}

		nsu = s[4:pos]
		s = s[pos+1:]
	}
	nodeId := ParseNodeID(s)
	return ExpandedNodeID{nodeId.GetIDType(), uint32(svr), nsu, nodeId}
}

// String returns a string representation of the ExpandedNodeID, e.g. "nsu=http://www.unifiedautomation.com/DemoServer/;s=Demo"
func (n ExpandedNodeID) String() string {
	b := new(strings.Builder)
	if n.ServerIndex > 0 {
		fmt.Fprintf(b, "svr=%d;", n.ServerIndex)
	}
	if len(n.NamespaceURI) > 0 {
		fmt.Fprintf(b, "nsu=%s;", n.NamespaceURI)
	}
	switch n2 := n.NodeID.(type) {
	case NodeIDNumeric:
		b.WriteString(n2.String())
	case NodeIDString:
		b.WriteString(n2.String())
	case NodeIDGUID:
		b.WriteString(n2.String())
	case NodeIDOpaque:
		b.WriteString(n2.String())
	default:
		b.WriteString("i=0")
	}
	return b.String()
}

// ToNodeID converts ExpandedNodeID to NodeID by looking up the NamespaceURI and replacing it with the index.
func ToNodeID(n ExpandedNodeID, namespaceURIs []string) NodeID {
	if n.NamespaceURI == "" {
		return n.NodeID
	}
	ns := uint16(0)
	flag := false
	for i, uri := range namespaceURIs {
		if uri == n.NamespaceURI {
			ns = uint16(i)
			flag = true
			break
		}
	}
	if !flag {
		return nil
	}
	switch n2 := n.NodeID.(type) {
	case NodeIDNumeric:
		return NewNodeIDNumeric(ns, n2.ID)
	case NodeIDString:
		return NewNodeIDString(ns, n2.ID)
	case NodeIDGUID:
		return NewNodeIDGUID(ns, n2.ID)
	case NodeIDOpaque:
		return NewNodeIDOpaque(ns, n2.ID)
	default:
		return nil
	}
}
