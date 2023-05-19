package server

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/Eun/go-convert"
	"github.com/afs/server/pkg/opcua/ua"
	"github.com/afs/server/pkg/util"
)

var (
	errInvalidDataTypeSyntax    error = errors.New("invalid data type name syntax")
	errByteOrBitIndexOutOfRange error = errors.New("byte or bit index is out of range")
	errConvertValueIsNull       error = errors.New("can not convert null value")
	errConvertValueOutOfRange   error = errors.New("value out of range")
)

func NewDataType(name string) (IDataType, error) {
	name = strings.ToLower(name)
	if name == "bool" {
		dt := &DTBool{}
		dt.Name = "Bool"
		dt.BitSize = 1
		dt.TotalSize = 1
		dt.Count = 1
		return dt, nil
	} else if name == "byte" {
		dt := &DTByte{}
		dt.Name = "Byte"
		dt.BitSize = 8
		dt.TotalSize = 8
		dt.Count = 1
		return dt, nil
	} else if name == "sbyte" {
		dt := &DTSByte{}
		dt.Name = "SByte"
		dt.BitSize = 8
		dt.TotalSize = 8
		dt.Count = 1
		return dt, nil
	} else if name == "uint16" {
		dt := &UInt16{}
		dt.Name = "UInt16"
		dt.BitSize = 16
		dt.TotalSize = 16
		dt.Count = 1
		return dt, nil
	} else if name == "uint32" {
		dt := &DTUInt32{}
		dt.Name = "UInt32"
		dt.BitSize = 32
		dt.TotalSize = 32
		dt.Count = 1
		return dt, nil
	} else if name == "uint64" {
		dt := &DTUInt64{}
		dt.Name = "UInt64"
		dt.BitSize = 64
		dt.TotalSize = 64
		dt.Count = 1
		return dt, nil
	} else if name == "int16" {
		dt := &DTInt16{}
		dt.Name = "Int16"
		dt.BitSize = 16
		dt.TotalSize = 16
		dt.Count = 1
		return dt, nil
	} else if name == "int32" {
		dt := &DTInt32{}
		dt.Name = "Int32"
		dt.BitSize = 32
		dt.TotalSize = 32
		dt.Count = 1
		return dt, nil
	} else if name == "int64" {
		dt := &DTLInt{}
		dt.Name = "Int64"
		dt.BitSize = 64
		dt.TotalSize = 64
		dt.Count = 1
		return dt, nil
	} else if name == "float" {
		dt := &DTFloat{}
		dt.Name = "Float"
		dt.BitSize = 32
		dt.TotalSize = 32
		dt.Count = 1
		return dt, nil
	} else if name == "double" {
		dt := &DTLReal{}
		dt.Name = "Double"
		dt.BitSize = 64
		dt.TotalSize = 64
		dt.Count = 1
		return dt, nil
	} else if name == "string" {
		dt := &DTString{}
		dt.Name = "String"
		dt.BitSize = 8
		return dt, nil
	}
	return nil, errInvalidDataTypeSyntax
}

type IDataType interface {
	GetName() string
	GetBitSize() int
	GetTotalSize() int
	SetTotalSize(size int)
	GetCount() int
	SetCount(count int)
	Decode(buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) (interface{}, error)
	Encode(value interface{}, buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) error
	CreateEmptyBuffer() []byte
	GetNodeID() ua.NodeID
	Convert(src interface{}) (interface{}, error)
}

type DataTypeBase struct {
	Name        string `json:"name,omitempty"`
	BitSize     int    `json:"bitSize,omitempty"`
	TotalSize   int    `json:"totalSize,omitempty"`
	Count       int    `json:"count,omitempty"`
	Description string `json:"description,omitempty"`
}

func (d *DataTypeBase) GetName() string {
	return d.Name
}

func (d *DataTypeBase) GetBitSize() int {
	return d.BitSize
}

func (d *DataTypeBase) GetTotalSize() int {
	return d.TotalSize
}

func (d *DataTypeBase) SetTotalSize(size int) {
	if size != 0 {
		d.TotalSize = size
	}
}

func (d *DataTypeBase) GetCount() int {
	return d.Count
}

func (d *DataTypeBase) SetCount(count int) {
	d.Count = count
}

func (d *DataTypeBase) GetDescription() string {
	return d.Description
}

/*
Bool - A two-state logical value (true or false).
*/
type DTBool struct {
	DataTypeBase
}

func (dt *DTBool) Decode(buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) (interface{}, error) {
	if bitIndex > 7 && bitIndex < 16 {
		byteIndex += 1
		bitIndex = bitIndex - 8
	}

	if len(buffer) > byteIndex {
		return buffer[byteIndex]&(1<<bitIndex) != 0, nil
	}
	return nil, errByteOrBitIndexOutOfRange
}

func (dt *DTBool) Encode(value interface{}, buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) error {
	result, err := dt.Convert(value)
	if err != nil {
		return err
	}

	if bitIndex > 7 && bitIndex < 16 {
		if len(buffer) > byteIndex+2 {
			bitIndex = bitIndex % 8
			if result.(bool) {
				buffer[byteIndex] = buffer[byteIndex] | (1 << bitIndex)
			} else {
				buffer[byteIndex] = buffer[byteIndex] &^ (1 << bitIndex)
			}

		}
	} else {
		if len(buffer) > byteIndex {
			if result.(bool) {
				buffer[byteIndex] = buffer[byteIndex] | (1 << bitIndex)
			} else {
				buffer[byteIndex] = buffer[byteIndex] &^ (1 << bitIndex)
			}
		}
	}
	return errByteOrBitIndexOutOfRange
}

func (dt *DTBool) CreateEmptyBuffer() []byte {
	return make([]byte, 1)
}

func (dt *DTBool) GetNodeID() ua.NodeID {
	return ua.DataTypeIDBoolean
}

func (dt *DTBool) Convert(src interface{}) (interface{}, error) {
	result, err := strconv.ParseBool(fmt.Sprintf("%v", src))
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (dt *DTBool) Min() float64 {
	return 0
}

func (dt *DTBool) Max() float64 {
	return 1
}

/*
Byte - An integer value between 0 and 255 inclusive.
*/
type DTByte struct {
	DataTypeBase
}

func (dt *DTByte) Decode(buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) (interface{}, error) {
	if byteIndex <= len(buffer) {
		return buffer[byteIndex], nil
	}
	return nil, errByteOrBitIndexOutOfRange
}

func (dt *DTByte) Encode(value interface{}, buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) error {
	if byteIndex > len(buffer) {
		return errByteOrBitIndexOutOfRange
	}

	result, err := dt.Convert(value)
	if err != nil {
		return err
	}
	buffer[byteIndex] = result.(byte)
	return nil
}

func (dt *DTByte) CreateEmptyBuffer() []byte {
	return make([]byte, 1)
}

func (dt *DTByte) GetNodeID() ua.NodeID {
	return ua.DataTypeIDByte
}

func (dt *DTByte) Convert(src interface{}) (interface{}, error) {
	num, err := strconv.ParseUint(fmt.Sprintf("%v", src), 10, 8)
	if err != nil {
		return nil, err
	}
	return byte(num), nil
}

func (dt *DTByte) Min() float64 {
	return 0
}

func (dt *DTByte) Max() float64 {
	return 255
}

/*
UInt16 - An unsigned integer value between 0 and 65 535 inclusive.
*/
type UInt16 struct {
	DataTypeBase
}

func (dt *UInt16) Decode(buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) (interface{}, error) {
	if byteIndex+2 <= len(buffer) {
		bs := buffer[byteIndex : byteIndex+2]
		return util.BytesToUInt16(bs, byteOrder), nil
	}
	return nil, errByteOrBitIndexOutOfRange
}

func (dt *UInt16) Encode(value interface{}, buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) error {
	if byteIndex+2 > len(buffer) {
		return errByteOrBitIndexOutOfRange
	}
	result, err := dt.Convert(value)
	if err != nil {
		return err
	}
	if byteOrder.IsBigEndian() {
		binary.BigEndian.PutUint16(buffer[byteIndex:byteIndex+2], result.(uint16))
	} else {
		binary.LittleEndian.PutUint16(buffer[byteIndex:byteIndex+2], result.(uint16))
	}
	return nil
}

func (dt *UInt16) CreateEmptyBuffer() []byte {
	return make([]byte, 2)
}

func (dt *UInt16) GetNodeID() ua.NodeID {
	return ua.DataTypeIDUInt16
}

func (dt *UInt16) Convert(src interface{}) (interface{}, error) {
	num, err := strconv.ParseUint(fmt.Sprintf("%v", src), 10, 16)
	if err != nil {
		return nil, err
	}
	return uint16(num), nil
}

func (dt *UInt16) Min() float64 {
	return 0
}

func (dt *UInt16) Max() float64 {
	return math.MaxUint16
}

/*
UInt32 - An unsigned integer value between 0 and 4 294 967 295 inclusive.
*/
type DTUInt32 struct {
	DataTypeBase
}

func (dt *DTUInt32) Decode(buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) (interface{}, error) {
	if byteIndex+4 <= len(buffer) {
		bs := buffer[byteIndex : byteIndex+4]
		return util.BytesToUInt32(bs, byteOrder), nil
	}
	return nil, errByteOrBitIndexOutOfRange
}

func (dt *DTUInt32) Encode(value interface{}, buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) error {
	if byteIndex+4 > len(buffer) {
		return errByteOrBitIndexOutOfRange
	}
	result, err := dt.Convert(value)
	if err != nil {
		return err
	}
	if byteOrder.IsBigEndian() {
		binary.BigEndian.PutUint32(buffer[byteIndex:byteIndex+4], result.(uint32))
	} else {
		binary.LittleEndian.PutUint32(buffer[byteIndex:byteIndex+4], result.(uint32))
	}
	return nil
}

func (dt *DTUInt32) CreateEmptyBuffer() []byte {
	return make([]byte, 4)
}

func (dt *DTUInt32) GetNodeID() ua.NodeID {
	return ua.DataTypeIDUInt32
}

func (dt *DTUInt32) Convert(src interface{}) (interface{}, error) {
	num, err := strconv.ParseUint(fmt.Sprintf("%v", src), 10, 32)
	if err != nil {
		return nil, err
	}
	return uint32(num), nil
}

func (dt *DTUInt32) Min() float64 {
	return 0
}

func (dt *DTUInt32) Max() float64 {
	return math.MaxUint32
}

/*
UInt64 - An unsigned integer value between 0 and 18 446 744 073 709 551 615 inclusive.
*/
type DTUInt64 struct {
	DataTypeBase
}

func (dt *DTUInt64) Decode(buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) (interface{}, error) {
	if byteIndex+8 <= len(buffer) {
		bs := buffer[byteIndex : byteIndex+8]
		return util.BytesToUInt64(bs, byteOrder), nil
	}
	return nil, errByteOrBitIndexOutOfRange
}

func (dt *DTUInt64) Encode(value interface{}, buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) error {
	if byteIndex+8 > len(buffer) {
		return errByteOrBitIndexOutOfRange
	}
	result, err := dt.Convert(value)
	if err != nil {
		return err
	}
	if byteOrder.IsBigEndian() {
		binary.BigEndian.PutUint64(buffer[byteIndex:byteIndex+8], result.(uint64))
	} else {
		binary.LittleEndian.PutUint64(buffer[byteIndex:byteIndex+8], result.(uint64))
	}
	return nil
}

func (dt *DTUInt64) CreateEmptyBuffer() []byte {
	return make([]byte, 8)
}

func (dt *DTUInt64) GetNodeID() ua.NodeID {
	return ua.DataTypeIDUInt64
}

func (dt *DTUInt64) Convert(src interface{}) (interface{}, error) {
	num, err := strconv.ParseUint(fmt.Sprintf("%v", src), 10, 64)
	if err != nil {
		return nil, err
	}
	return num, nil
}

func (dt *DTUInt64) Min() float64 {
	return 0
}

func (dt *DTUInt64) Max() float64 {
	return math.MaxUint64
}

/*
SByte - An integer value between −128 and 127 inclusive.
*/
type DTSByte struct {
	DataTypeBase
}

func (dt *DTSByte) Decode(buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) (interface{}, error) {
	if byteIndex <= len(buffer) {
		return int8(buffer[byteIndex]), nil
	}
	return nil, errByteOrBitIndexOutOfRange
}

func (dt *DTSByte) Encode(value interface{}, buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) error {
	if byteIndex > len(buffer) {
		return errByteOrBitIndexOutOfRange
	}
	result, err := dt.Convert(value)
	if err != nil {
		return err
	}
	buffer[byteIndex] = result.(byte)
	return nil
}

func (dt *DTSByte) CreateEmptyBuffer() []byte {
	return make([]byte, 1)
}

func (dt *DTSByte) GetNodeID() ua.NodeID {
	return ua.DataTypeIDSByte
}

func (dt *DTSByte) Convert(src interface{}) (interface{}, error) {
	num, err := strconv.ParseInt(fmt.Sprintf("%v", src), 10, 8)
	if err != nil {
		return nil, err
	}
	return int8(num), nil
}

/*
Int16 - An integer value between −32 768 and 32 767 inclusive.
*/
type DTInt16 struct {
	DataTypeBase
}

func (dt *DTInt16) Decode(buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) (interface{}, error) {
	if byteIndex+2 <= len(buffer) {
		bs := buffer[byteIndex : byteIndex+2]
		return util.BytesToInt16(bs, byteOrder), nil
	}
	return nil, errByteOrBitIndexOutOfRange
}

func (dt *DTInt16) Encode(value interface{}, buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) error {
	if byteIndex+2 > len(buffer) {
		return errByteOrBitIndexOutOfRange
	}
	result, err := dt.Convert(value)
	if err != nil {
		return err
	}
	if byteOrder.IsBigEndian() {
		binary.BigEndian.PutUint16(buffer[byteIndex:byteIndex+2], uint16(result.(int16)))
	} else {
		binary.LittleEndian.PutUint16(buffer[byteIndex:byteIndex+2], uint16(result.(int16)))
	}
	return nil
}

func (dt *DTInt16) CreateEmptyBuffer() []byte {
	return make([]byte, 2)
}

func (dt *DTInt16) GetNodeID() ua.NodeID {
	return ua.DataTypeIDInt16
}

func (dt *DTInt16) Convert(src interface{}) (interface{}, error) {
	num, err := strconv.ParseInt(fmt.Sprintf("%v", src), 10, 16)
	if err != nil {
		return nil, err
	}
	return int16(num), nil
}

/*
Int32 - An integer value between −2 147 483 648 and 2 147 483 647 inclusive.
*/
type DTInt32 struct {
	DataTypeBase
}

func (dt *DTInt32) Decode(buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) (interface{}, error) {
	if byteIndex+4 <= len(buffer) {
		bs := buffer[byteIndex : byteIndex+4]
		return util.BytesToInt32(bs, byteOrder), nil
	}
	return nil, errByteOrBitIndexOutOfRange
}

func (dt *DTInt32) Encode(value interface{}, buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) error {
	if byteIndex+4 > len(buffer) {
		return errByteOrBitIndexOutOfRange
	}
	result, err := dt.Convert(value)
	if err != nil {
		return err
	}
	if byteOrder.IsBigEndian() {
		binary.BigEndian.PutUint32(buffer[byteIndex:byteIndex+4], uint32(result.(int32)))
	} else {
		binary.LittleEndian.PutUint32(buffer[byteIndex:byteIndex+4], uint32(result.(int32)))
	}
	return nil
}

func (dt *DTInt32) CreateEmptyBuffer() []byte {
	return make([]byte, 4)
}

func (dt *DTInt32) GetNodeID() ua.NodeID {
	return ua.DataTypeIDInt32
}

func (dt *DTInt32) Convert(src interface{}) (interface{}, error) {
	num, err := strconv.ParseInt(fmt.Sprintf("%v", src), 10, 32)
	if err != nil {
		return nil, err
	}
	return int32(num), nil
}

/*
Int64 - An integer value between −9 223 372 036 854 775 808 and 9 223 372 036 854 775 807 inclusive.
*/
type DTLInt struct {
	DataTypeBase
}

func (dt *DTLInt) Decode(buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) (interface{}, error) {
	if byteIndex+8 <= len(buffer) {
		bs := buffer[byteIndex : byteIndex+8]
		return util.BytesToInt64(bs, byteOrder), nil
	}
	return nil, errByteOrBitIndexOutOfRange
}

func (dt *DTLInt) Encode(value interface{}, buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) error {
	if byteIndex+8 > len(buffer) {
		return errByteOrBitIndexOutOfRange
	}
	result, err := dt.Convert(value)
	if err != nil {
		return err
	}
	if byteOrder.IsBigEndian() {
		binary.BigEndian.PutUint64(buffer[byteIndex:byteIndex+8], uint64(result.(int64)))
	} else {
		binary.LittleEndian.PutUint64(buffer[byteIndex:byteIndex+8], uint64(result.(int64)))
	}
	return nil
}

func (dt *DTLInt) CreateEmptyBuffer() []byte {
	return make([]byte, 8)
}

func (dt *DTLInt) GetNodeID() ua.NodeID {
	return ua.DataTypeIDInt64
}

func (dt *DTLInt) Convert(src interface{}) (interface{}, error) {
	num, err := strconv.ParseInt(fmt.Sprintf("%v", src), 10, 64)
	if err != nil {
		return nil, err
	}
	return num, nil
}

/*
Float - An IEEE single precision (32 bit) floating point value.
*/
type DTFloat struct {
	DataTypeBase
}

func (dt *DTFloat) Decode(buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) (interface{}, error) {
	if byteIndex+4 <= len(buffer) {
		bs := buffer[byteIndex : byteIndex+4]
		return util.BytesToFloat32(bs, byteOrder), nil
	}
	return nil, errByteOrBitIndexOutOfRange
}

func (dt *DTFloat) Encode(value interface{}, buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) error {
	if byteIndex+4 > len(buffer) {
		return errByteOrBitIndexOutOfRange
	}
	result, err := dt.Convert(value)
	if err != nil {
		return err
	}
	if byteOrder.IsBigEndian() {
		binary.BigEndian.PutUint32(buffer[byteIndex:byteIndex+4], uint32(result.(float32)))
	} else {
		binary.LittleEndian.PutUint32(buffer[byteIndex:byteIndex+4], uint32(result.(float32)))
	}
	return nil
}

func (dt *DTFloat) CreateEmptyBuffer() []byte {
	return make([]byte, 4)
}

func (dt *DTFloat) GetNodeID() ua.NodeID {
	return ua.DataTypeIDFloat
}

func (dt *DTFloat) Convert(src interface{}) (interface{}, error) {
	num, err := strconv.ParseFloat(fmt.Sprintf("%v", src), 32)
	if err != nil {
		return nil, err
	}
	return float32(num), nil
}

/*
Double - An IEEE double precision (64 bit) floating point value.
*/
type DTLReal struct {
	DataTypeBase
}

func (dt *DTLReal) Decode(buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) (interface{}, error) {
	if byteIndex+8 <= len(buffer) {
		bs := buffer[byteIndex : byteIndex+8]
		return util.BytesToFloat64(bs, byteOrder), nil
	}
	return nil, errByteOrBitIndexOutOfRange
}

func (dt *DTLReal) Encode(value interface{}, buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) error {
	if byteIndex+8 > len(buffer) {
		return errByteOrBitIndexOutOfRange
	}
	result, err := dt.Convert(value)
	if err != nil {
		return err
	}
	if byteOrder.IsBigEndian() {
		binary.BigEndian.PutUint64(buffer[byteIndex:byteIndex+8], uint64(result.(float64)))
	} else {
		binary.LittleEndian.PutUint64(buffer[byteIndex:byteIndex+8], uint64(result.(float64)))
	}
	return nil
}

func (dt *DTLReal) CreateEmptyBuffer() []byte {
	return make([]byte, 8)
}

func (dt *DTLReal) GetNodeID() ua.NodeID {
	return ua.DataTypeIDDouble
}

func (dt *DTLReal) Convert(src interface{}) (interface{}, error) {
	num, err := strconv.ParseFloat(fmt.Sprintf("%v", src), 64)
	if err != nil {
		return nil, err
	}
	return num, nil
}

// ======================================================
// --- Char
// ======================================================

type DTChar struct {
	DataTypeBase
}

func (dt *DTChar) Decode(buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) (interface{}, error) {
	if byteIndex <= len(buffer) {
		return string(buffer[byteIndex]), nil
	}
	return nil, errByteOrBitIndexOutOfRange
}

func (dt *DTChar) Encode(value interface{}, buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) error {
	if byteIndex <= len(buffer) {
		var result uint8
		err := convert.Convert(value, &result)
		if err != nil {
			return err
		}
		buffer[byteIndex] = result
	}
	return errByteOrBitIndexOutOfRange
}

func (dt *DTChar) CreateEmptyBuffer() []byte {
	return make([]byte, 1)
}

func (dt *DTChar) GetNodeID() ua.NodeID {
	return ua.DataTypeIDString
}

func (dt *DTChar) Convert(src interface{}) (interface{}, error) {
	if src == nil {
		return nil, errConvertValueIsNull
	}
	str := fmt.Sprintf("%v", src)
	if len(str) > 1 {
		return nil, errConvertValueOutOfRange
	}
	return str, nil
}

// ======================================================
// --- WChar
// ======================================================

type DTWChar struct {
	DataTypeBase
}

func (dt *DTWChar) Decode(buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) (interface{}, error) {
	if byteIndex+2 <= len(buffer) {
		bs := buffer[byteIndex : byteIndex+2]
		return string(bs), nil
	}
	return nil, errByteOrBitIndexOutOfRange
}

func (dt *DTWChar) Encode(value interface{}, buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) error {
	// if byteIndex+2 <= len(buffer) {
	// 	if len(value) <= 2 {
	// 		bs := []byte(value)
	// 		for i := 0; i < len(bs); i++ {
	// 			buffer[byteIndex+i] = bs[i]
	// 		}
	// 	}
	// }
	return errByteOrBitIndexOutOfRange
}

func (dt *DTWChar) CreateEmptyBuffer() []byte {
	return make([]byte, 2)
}

func (dt *DTWChar) GetNodeID() ua.NodeID {
	return ua.DataTypeIDString
}

func (dt *DTWChar) Convert(src interface{}) (interface{}, error) {
	if src == nil {
		return nil, errConvertValueIsNull
	}
	str := fmt.Sprintf("%v", src)
	if len(str) > 2 {
		return nil, errConvertValueOutOfRange
	}
	return str, nil
}

/*
String - A sequence of Unicode characters.
*/
type DTString struct {
	DataTypeBase
}

func (dt *DTString) Decode(buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) (interface{}, error) {
	if byteIndex+2 <= len(buffer) {
		bs := buffer[byteIndex : byteIndex+2]
		return string(bs), nil
	}
	return nil, errByteOrBitIndexOutOfRange
}

func (dt *DTString) Encode(value interface{}, buffer []byte, byteIndex int, bitIndex byte, byteOrder util.ByteOrder) error {
	// if byteIndex+2 <= len(buffer) {
	// 	if len(value) <= 2 {
	// 		bs := []byte(value)
	// 		for i := 0; i < len(bs); i++ {
	// 			buffer[byteIndex+i] = bs[i]
	// 		}
	// 	}
	// }
	return errByteOrBitIndexOutOfRange
}

func (dt *DTString) CreateEmptyBuffer() []byte {
	return make([]byte, 2)
}

func (dt *DTString) GetNodeID() ua.NodeID {
	return ua.DataTypeIDString
}

func (dt *DTString) Convert(src interface{}) (interface{}, error) {
	if src == nil {
		return nil, errConvertValueIsNull
	}
	str := fmt.Sprintf("%v", src)
	if len(str)*8 > dt.TotalSize {
		return nil, errConvertValueOutOfRange
	}
	return str, nil
}
