package ua

import (
	"bytes"

	"github.com/karlseguin/jsonwriter"
	"github.com/tidwall/gjson"
)

// RolePermissionType structure.
type RolePermissionType struct {
	RoleID      NodeID
	Permissions PermissionType
}

func (rpt RolePermissionType) MarshalJSON() ([]byte, error) {
	buffer := new(bytes.Buffer)
	writer := jsonwriter.New(buffer)
	writer.RootObject(func() {
		writer.KeyValue("roleId", rpt.RoleID)
		writer.KeyValue("permissions", rpt.Permissions)
	})
	return buffer.Bytes(), nil
}

func (rpt *RolePermissionType) UnmarshalJSON(b []byte) error {
	jeRoleId := gjson.GetBytes(b, "roleId")
	roleId, err := ParseNodeIDBytes([]byte(jeRoleId.Raw))
	if err != nil {
		return err
	}
	rpt.RoleID = roleId

	jePermissions := gjson.GetBytes(b, "permissions")
	rpt.Permissions = PermissionType(int32(jePermissions.Int()))
	return nil
}
