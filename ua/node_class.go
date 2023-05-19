package ua

import "encoding/json"

func (n NodeClass) MarshalJSON() ([]byte, error) {
	return json.Marshal(int32(n))
}

func (n *NodeClass) UnmarshalJSON(b []byte) error {
	var nodeClass int32
	err := json.Unmarshal(b, &nodeClass)
	if err != nil {
		return err
	}
	*n = NodeClass(nodeClass)
	return nil
}
