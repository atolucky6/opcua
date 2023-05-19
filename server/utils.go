package server

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/Eun/go-convert"
	"github.com/afs/server/pkg/eris"
	"github.com/afs/server/pkg/msg"
	"github.com/afs/server/pkg/opcua/ua"
	"github.com/google/uuid"
)

var (
	regex_InvalidName *regexp.Regexp = regexp.MustCompile(`(?P<INVALID>\.|/|\\|:)`)
)

func ParsePluginId(value interface{}) (int16, error) {
	var id int16
	err := convert.Convert(value, &id)
	if err != nil {
		return -1, err
	}
	return id, nil
}

func IsGUID(u string) bool {
	_, err := uuid.Parse(u)
	return err == nil
}

func IsValidName(value string) error {
	if len(value) == 0 {
		return ErrFieldRequired
	}
	matchIndexs := regex_InvalidName.FindAllStringSubmatchIndex(value, -1)
	if len(matchIndexs) > 0 {
		expNames := regex_InvalidName.SubexpNames()
		for i, name := range expNames {
			if len(name) > 0 {
				if matchIndexs[0][i*2] >= 0 && matchIndexs[0][i*2+1] >= 1 {
					invalidChar := value[matchIndexs[0][i*2]:matchIndexs[0][i*2+1]]
					return fmt.Errorf("the name can't contains '%s'", invalidChar)
				}
			}
		}
	}
	return nil
}

func IsUniqueName(name string, parent Node, originNode Node) error {
	if parent != nil {
		if parent.(NodeEx).First(func(child *ObjectNode) bool { return child.GetBrowseName().Name == name && child != originNode }) != nil {
			return fmt.Errorf("the name '%s' is already taken", name)
		}
	}
	return nil
}

// CheckBrowseName will check the given value is valid to set for BrowseName property of target node
func CheckBrowseName(value interface{}, target *ObjectNode, parent *ObjectNode) (bool, interface{}, error) {
	var validValue string
	err := convert.Convert(value, &validValue)
	if err != nil {
		return true, "", eris.Wrap(err, msg.InvalidValue)
	}

	// trim space
	validValue = strings.Trim(validValue, " ")
	err = IsValidName(validValue)
	if err != nil {
		return true, "", err
	}

	if !target.nodeType.IsRoot() {
		err = IsUniqueName(validValue, parent, target)
		if err != nil {
			return true, "", err
		}
	}

	return true, validValue, nil
}

// CheckDisplayName will check the given value is valid to set for DisplayName property of target node
func CheckDisplayName(value interface{}, target *ObjectNode, parent *ObjectNode) (bool, interface{}, error) {
	var validValue string
	err := convert.Convert(value, &validValue)
	if err != nil {
		return true, "", eris.Wrap(err, msg.InvalidValue)
	}
	return true, validValue, nil
}

// CheckDescription will check the given value is valid to set for Description property of target node
func CheckDescription(value interface{}, target *ObjectNode, parent *ObjectNode) (bool, interface{}, error) {
	var validValue string
	err := convert.Convert(value, &validValue)
	if err != nil {
		return true, "", eris.Wrap(err, msg.InvalidValue)
	}
	return true, validValue, nil
}

// GetDataTypeNameByNodeID returns the relative data type name by node id
func GetDataTypeNameByNodeID(nodeID ua.NodeID) string {
	switch nodeID {
	case ua.DataTypeIDBoolean:
		return "Boolean"
	case ua.DataTypeIDSByte:
		return "SByte"
	case ua.DataTypeIDByte:
		return "Byte"
	case ua.DataTypeIDInt16:
		return "Int16"
	case ua.DataTypeIDInt32:
		return "Int32"
	case ua.DataTypeIDInt64:
		return "Int64"
	case ua.DataTypeIDUInt16:
		return "UInt16"
	case ua.DataTypeIDUInt32:
		return "UInt32"
	case ua.DataTypeIDUInt64:
		return "UInt64"
	case ua.DataTypeIDFloat:
		return "Float"
	case ua.DataTypeIDDouble:
		return "Double"
	case ua.DataTypeIDString:
		return "String"
	case ua.DataTypeIDDateTime:
		return "DateTime"
	case ua.DataTypeIDGUID:
		return "GUID"
	case ua.DataTypeIDByteString:
		return "ByteString"
	case ua.DataTypeIDXMLElement:
		return "XmlElement"
	case ua.DataTypeIDNodeID:
		return "NodeID"
	case ua.DataTypeIDExpandedNodeID:
		return "ExpandedNodeId"
	case ua.DataTypeIDStatusCode:
		return "StatusCode"
	case ua.DataTypeIDQualifiedName:
		return "QualifiedName"
	case ua.DataTypeIDLocalizedText:
		return "LocalizedText"
	case ua.DataTypeIDDataValue:
		return "DataValue"
	case ua.DataTypeIDDiagnosticInfo:
		return "DiagnosticInfo"
	default:
		return "Unknown"
	}
}

func WildcardMatch(s string, p string) bool {
	runeInput := []rune(s)
	runePattern := []rune(p)

	lenInput := len(runeInput)
	lenPattern := len(runePattern)

	isMatchingMatrix := make([][]bool, lenInput+1)

	for i := range isMatchingMatrix {
		isMatchingMatrix[i] = make([]bool, lenPattern+1)
	}

	isMatchingMatrix[0][0] = true
	for i := 1; i < lenInput; i++ {
		isMatchingMatrix[i][0] = false
	}

	if lenPattern > 0 {
		if runePattern[0] == '*' {
			isMatchingMatrix[0][1] = true
		}
	}

	for j := 2; j <= lenPattern; j++ {
		if runePattern[j-1] == '*' {
			isMatchingMatrix[0][j] = isMatchingMatrix[0][j-1]
		}

	}

	for i := 1; i <= lenInput; i++ {
		for j := 1; j <= lenPattern; j++ {

			if runePattern[j-1] == '*' {
				isMatchingMatrix[i][j] = isMatchingMatrix[i-1][j] || isMatchingMatrix[i][j-1]
			}

			if runePattern[j-1] == '?' || runeInput[i-1] == runePattern[j-1] {
				isMatchingMatrix[i][j] = isMatchingMatrix[i-1][j-1]
			}
		}
	}

	return isMatchingMatrix[lenInput][lenPattern]
}
