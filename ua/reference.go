package ua

import (
	"bytes"
	"encoding/json"

	"github.com/karlseguin/jsonwriter"
	"github.com/tidwall/gjson"
)

// Reference ...
type Reference struct {
	ReferenceTypeID NodeID         `json:"referenceTypeId"`
	IsInverse       bool           `json:"isInverse"`
	TargetID        ExpandedNodeID `json:"targetId"`
}

func NewReference(referenceTypeID NodeID, isInverse bool, targetID ExpandedNodeID) Reference {
	return Reference{referenceTypeID, isInverse, targetID}
}

func (ref Reference) MarshalJSON() ([]byte, error) {
	buffer := new(bytes.Buffer)
	writer := jsonwriter.New(buffer)
	writer.RootObject(func() {
		writer.KeyValue("referenceTypeId", ref.ReferenceTypeID)
		writer.KeyValue("isInverse", ref.IsInverse)
		writer.KeyValue("targetId", ref.TargetID)
	})
	return buffer.Bytes(), nil
}

func (ref *Reference) UnmarshalJSON(b []byte) error {
	jeReferenceTypeId := gjson.GetBytes(b, "referenceTypeId")
	referenceTypeId, err := ParseNodeIDBytes([]byte(jeReferenceTypeId.Raw))
	if err != nil {
		return err
	}
	ref.ReferenceTypeID = referenceTypeId

	jeIsInverse := gjson.GetBytes(b, "isInverse")
	ref.IsInverse = jeIsInverse.Bool()

	jeTargetId := gjson.GetBytes(b, "targetId")
	var targetId ExpandedNodeID
	err = json.Unmarshal([]byte(jeTargetId.Raw), &targetId)
	if err != nil {
		return err
	}
	ref.TargetID = targetId
	return nil
}
