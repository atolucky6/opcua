// Copyright 2021 Converter Systems LLC. All rights reserved.

package ua

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/karlseguin/jsonwriter"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

// VariantTypes
const (
	VariantTypeNull byte = iota
	VariantTypeBoolean
	VariantTypeSByte
	VariantTypeByte
	VariantTypeInt16
	VariantTypeUInt16
	VariantTypeInt32
	VariantTypeUInt32
	VariantTypeInt64
	VariantTypeUInt64
	VariantTypeFloat
	VariantTypeDouble
	VariantTypeString
	VariantTypeDateTime
	VariantTypeGUID
	VariantTypeByteString
	VariantTypeXMLElement
	VariantTypeNodeID
	VariantTypeExpandedNodeID
	VariantTypeStatusCode
	VariantTypeQualifiedName
	VariantTypeLocalizedText
	VariantTypeExtensionObject
	VariantTypeDataValue
	VariantTypeVariant
	VariantTypeDiagnosticInfo
)

/*
Variant stores a single value or slice of the following types:

	bool, int8, uint8, int16, uint16, int32, uint32
	int64, uint64, float32, float64, string
	time.Time, uuid.UUID, ByteString, XmlElement
	NodeId, ExpandedNodeId, StatusCode, QualifiedName
	LocalizedText, DataValue, Variant

In addition, you may store any type that is registered with the BinaryEncoder.
These types will be encoded as an ExtensionObject by the BinaryEncoder.
*/
type Variant interface{}

type JsonVariant struct {
	Type       byte        `json:"type,omitempty"`
	Body       interface{} `json:"body"`
	Dimensions []uint32    `json:"dimensions,omitempty"`
}

func NewJsonVariant(value Variant) JsonVariant {
	vType := byte(0)

	if value != nil {
		switch value.(type) {
		case bool:
			vType = VariantTypeBoolean
		case int8:
			vType = VariantTypeSByte
		case uint8:
			vType = VariantTypeByte
		case int16:
			vType = VariantTypeInt16
		case uint16:
			vType = VariantTypeUInt16
		case int32:
			vType = VariantTypeInt32
		case uint32:
			vType = VariantTypeUInt32
		case int64:
			vType = VariantTypeInt64
		case uint64:
			vType = VariantTypeUInt64
		case float32:
			vType = VariantTypeFloat
		case float64:
			vType = VariantTypeDouble
		case string:
			vType = VariantTypeString
		case time.Time:
			vType = VariantTypeDateTime
		case uuid.UUID:
			vType = VariantTypeGUID
		case ByteString:
			vType = VariantTypeByteString
		case XMLElement:
			vType = VariantTypeXMLElement
		case NodeID:
			vType = VariantTypeNodeID
		case ExpandedNodeID:
			vType = VariantTypeExpandedNodeID
		case StatusCode:
			vType = VariantTypeStatusCode
		case QualifiedName:
			vType = VariantTypeQualifiedName
		case LocalizedText:
			vType = VariantTypeLocalizedText
		case DataValue:
			vType = VariantTypeDataValue
		case UAVariant:
			vType = VariantTypeVariant
		case DiagnosticInfo:
			vType = VariantTypeDiagnosticInfo
		case ExtensionObject:
			vType = VariantTypeExtensionObject
		}
	}

	return JsonVariant{
		Type: vType,
		Body: value,
	}
}

func (jv *JsonVariant) ToVariant() Variant {
	return jv.Body
}

func (jv *JsonVariant) UnmarshalJSON(b []byte) error {
	jeBody := gjson.GetBytes(b, "body")
	if !jeBody.Exists() {
		jv.Body = nil
	}

	jeType := gjson.GetBytes(b, "type")
	bodyType := byte(0)
	if jeType.Exists() {
		res, err := strconv.ParseInt(jeType.Raw, 10, 8)
		if err != nil {
			return err
		}
		bodyType = byte(res)
	}

	switch bodyType {
	case VariantTypeBoolean:
		jv.Body = jeBody.Bool()
	case VariantTypeSByte:
		body, err := strconv.ParseInt(jeBody.Raw, 10, 8)
		if err != nil {
			return err
		}
		jv.Body = int8(body)
	case VariantTypeByte:
		body, err := strconv.ParseUint(jeBody.Raw, 10, 8)
		if err != nil {
			return err
		}
		jv.Body = uint8(body)
	case VariantTypeInt16:
		body, err := strconv.ParseInt(jeBody.Raw, 10, 16)
		if err != nil {
			return err
		}
		jv.Body = int16(body)
	case VariantTypeUInt16:
		body, err := strconv.ParseUint(jeBody.Raw, 10, 16)
		if err != nil {
			return err
		}
		jv.Body = uint16(body)
	case VariantTypeInt32:
		body, err := strconv.ParseInt(jeBody.Raw, 10, 32)
		if err != nil {
			return err
		}
		jv.Body = int32(body)
	case VariantTypeUInt32:
		body, err := strconv.ParseUint(jeBody.Raw, 10, 32)
		if err != nil {
			return err
		}
		jv.Body = uint32(body)
	case VariantTypeInt64:
		body, err := strconv.ParseInt(jeBody.Raw, 10, 64)
		if err != nil {
			return err
		}
		jv.Body = body
	case VariantTypeUInt64:
		body, err := strconv.ParseUint(jeBody.Raw, 10, 64)
		if err != nil {
			return err
		}
		jv.Body = body
	case VariantTypeFloat:
		body, err := strconv.ParseFloat(jeBody.Raw, 32)
		if err != nil {
			return err
		}
		jv.Body = float32(body)
	case VariantTypeDouble:
		body, err := strconv.ParseFloat(jeBody.Raw, 64)
		if err != nil {
			return err
		}
		jv.Body = body
	case VariantTypeString:
		jv.Body = jeBody.String()
	case VariantTypeDateTime:
		var body time.Time
		err := json.Unmarshal([]byte(jeBody.Raw), &body)
		if err != nil {
			return err
		}
		jv.Body = body
	case VariantTypeGUID:
		var body uuid.UUID
		err := json.Unmarshal([]byte(jeBody.Raw), &body)
		if err != nil {
			return err
		}
		jv.Body = body
	case VariantTypeByteString:
		_, err := base64.StdEncoding.DecodeString(jeBody.Raw)
		if err != nil {
			return err
		}
		jv.Body = jeBody.Raw
	case VariantTypeXMLElement:
		var body XMLElement
		err := json.Unmarshal([]byte(jeBody.Raw), &body)
		if err != nil {
			return err
		}
		jv.Body = body
	case VariantTypeNodeID:
		body, err := ParseNodeIDBytes([]byte(jeBody.Raw))
		if err != nil {
			return err
		}
		jv.Body = body
	case VariantTypeExpandedNodeID:
		var body ExpandedNodeID
		err := json.Unmarshal([]byte(jeBody.Raw), &body)
		if err != nil {
			return err
		}
		jv.Body = body
	case VariantTypeStatusCode:
		var body StatusCode
		err := json.Unmarshal([]byte(jeBody.Raw), &body)
		if err != nil {
			return err
		}
		jv.Body = body
	case VariantTypeQualifiedName:
		var body QualifiedName
		err := json.Unmarshal([]byte(jeBody.Raw), &body)
		if err != nil {
			return err
		}
		jv.Body = body
	case VariantTypeLocalizedText:
		var body LocalizedText
		err := json.Unmarshal([]byte(jeBody.Raw), &body)
		if err != nil {
			return err
		}
		jv.Body = body
	case VariantTypeExtensionObject:
		log.Panicln("json variant decoder not supported ExtensionObject type")
	case VariantTypeDataValue:
		var body DataValue
		err := json.Unmarshal([]byte(jeBody.Raw), &body)
		if err != nil {
			return err
		}
		jv.Body = body
	case VariantTypeVariant:
		var body Variant
		err := json.Unmarshal([]byte(jeBody.Raw), &body)
		if err != nil {
			return err
		}
		jv.Body = body
	case VariantTypeDiagnosticInfo:
		var body DiagnosticInfo
		err := json.Unmarshal([]byte(jeBody.Raw), &body)
		if err != nil {
			return err
		}
		jv.Body = body
	}

	return nil
}

func (jv JsonVariant) MarshalJSON() ([]byte, error) {
	buffer := new(bytes.Buffer)
	writer := jsonwriter.New(buffer)
	writer.RootObject(func() {
		if jv.Type != 0 {
			writer.KeyValue("type", jv.Type)
		}
		if jv.Type == VariantTypeGUID {
			writer.KeyValue("body", jv.Body.(uuid.UUID).String())
		} else {
			writer.KeyValue("body", jv.Body)
		}
	})
	return buffer.Bytes(), nil
}
