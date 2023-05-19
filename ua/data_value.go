// Copyright 2021 Converter Systems LLC. All rights reserved.

package ua

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/karlseguin/jsonwriter"
	"github.com/tidwall/gjson"
)

// DataValue holds the value, quality and timestamp
type DataValue struct {
	Value             Variant    `json:"value"`
	StatusCode        StatusCode `json:"statusCode"`
	SourceTimestamp   time.Time  `json:"sourceTimestamp"`
	SourcePicoseconds uint16     `json:"sourcePicoseconds"`
	ServerTimestamp   time.Time  `json:"serverTimestamp"`
	ServerPicoseconds uint16     `json:"serverPicoseconds"`
}

func NewDataValue(value Variant, status StatusCode, sourceTimestamp time.Time, sourcePicoseconds uint16, serverTimestamp time.Time, serverPicoseconds uint16) DataValue {
	return DataValue{value, status, sourceTimestamp, sourcePicoseconds, serverTimestamp, serverPicoseconds}
}

// NilDataValue is the nil value.
var NilDataValue = DataValue{}

func (dv *DataValue) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		return nil
	}

	jeValue := gjson.GetBytes(b, "value")
	var jv JsonVariant
	if err := json.Unmarshal([]byte(jeValue.Raw), &jv); err != nil {
		return err
	}
	dv.Value = jv.Body

	jeStatus := gjson.GetBytes(b, "status")

	if string(jeStatus.Raw) == "null" {
		dv.StatusCode = Good
	} else {
		var statusCode StatusCode
		if err := json.Unmarshal([]byte(jeStatus.Raw), &statusCode); err != nil {
			return err
		}
		dv.StatusCode = statusCode
	}

	jeServerTimestamp := gjson.GetBytes(b, "serverTimestamp")
	var serverTimestamp time.Time
	if err := json.Unmarshal([]byte(jeServerTimestamp.Raw), &serverTimestamp); err != nil {
		return err
	}
	dv.ServerTimestamp = serverTimestamp

	jeServerPicoseconds := gjson.GetBytes(b, "serverPicoseconds")
	var serverPicoseconds uint16
	if err := json.Unmarshal([]byte(jeServerPicoseconds.Raw), &serverPicoseconds); err != nil {
		return err
	}
	dv.ServerPicoseconds = serverPicoseconds

	jeSourceTimestamp := gjson.GetBytes(b, "sourceTimestamp")
	var sourceTimestamp time.Time
	if err := json.Unmarshal([]byte(jeSourceTimestamp.Raw), &sourceTimestamp); err != nil {
		return err
	}
	dv.SourceTimestamp = sourceTimestamp

	jeSourcePicoseconds := gjson.GetBytes(b, "sourcePicoseconds")
	var sourcePicoseconds uint16
	if err := json.Unmarshal([]byte(jeSourcePicoseconds.Raw), &sourcePicoseconds); err != nil {
		return err
	}
	dv.SourcePicoseconds = sourcePicoseconds
	return nil
}

func (dv DataValue) MarshalJSON() ([]byte, error) {
	buffer := new(bytes.Buffer)
	writer := jsonwriter.New(buffer)
	writer.RootObject(func() {
		writer.KeyValue("value", NewJsonVariant(dv.Value))
		writer.KeyValue("status", dv.StatusCode)
		writer.KeyValue("sourceTimestamp", dv.SourceTimestamp)
		writer.KeyValue("sourcePicoseconds", dv.SourcePicoseconds)
		writer.KeyValue("serverTimestamp", dv.ServerTimestamp)
		writer.KeyValue("serverPicoseconds", dv.ServerPicoseconds)
	})
	return buffer.Bytes(), nil
}
