package server

import (
	"fmt"
	"log"

	"github.com/Eun/go-convert"
	"github.com/afs/server/pkg/eris"
	"github.com/afs/server/pkg/msg"
	"github.com/afs/server/pkg/opcua/ua"
	"github.com/iancoleman/strcase"
	"gopkg.in/guregu/null.v4"
)

type FieldMap map[string]interface{}

type FieldDef struct {
	Required    bool          `json:"required,omitempty"`
	Name        string        `json:"name,omitempty"`
	DisplayName string        `json:"displayName,omitempty"`
	Description string        `json:"description,omitempty"`
	Type        string        `json:"type,omitempty"`
	Hint        string        `json:"hint,omitempty"`
	Min         null.Int      `json:"min,omitempty"`
	Max         null.Int      `json:"max,omitempty"`
	Options     []interface{} `json:"options,omitempty"`
}

// RemoveNonPluginFields remove all field that not required for plugin
func (m *FieldMap) RemoveNonPluginFields(cfg *PluginConfig, nodeType NodeType) {
	for k := range *m {
		if cfg.GetFieldDef(k, nodeType) == nil {
			delete(*m, k)
		}
	}
}

// normalize key from lower camel to camel
func (m *FieldMap) NormalizeFieldName() {
	for k, v := range *m {
		normalizeName := strcase.ToCamel(k)
		delete(*m, k)
		(*m)[normalizeName] = v
	}
}

func (m *FieldMap) GetString(field string) (string, error) {
	if value, found := (*m)[field]; found {
		var result string
		err := convert.Convert(value, &result)
		if err != nil {
			return "", eris.Wrap(err, msg.InvalidValue)
		}
		return result, err
	}
	return "", ErrNotFound
}

func (m *FieldMap) GetBool(field string) (bool, error) {
	if value, found := (*m)[field]; found {
		var result bool
		err := convert.Convert(value, &result)
		if err != nil {
			return false, eris.Wrap(err, msg.InvalidValue)
		}
		return result, err
	}
	return false, ErrNotFound
}

func (m *FieldMap) GetFloat64(field string) (float64, error) {
	if value, found := (*m)[field]; found {
		var result float64
		err := convert.Convert(value, &result)
		if err != nil {
			return 0, eris.Wrap(err, msg.InvalidValue)
		}
		return result, err
	}
	return 0, ErrNotFound
}

func (m *FieldMap) GetFloat32(field string) (float32, error) {
	if value, found := (*m)[field]; found {
		var result float32
		err := convert.Convert(value, &result)
		if err != nil {
			return 0, eris.Wrap(err, msg.InvalidValue)
		}
		return result, err
	}
	return 0, ErrNotFound
}

func (m *FieldMap) GetInt64(field string) (int64, error) {
	if value, found := (*m)[field]; found {
		var result int64
		err := convert.Convert(value, &result)
		if err != nil {
			return 0, eris.Wrap(err, msg.InvalidValue)
		}
		return result, err
	}
	return 0, ErrNotFound
}

func (m *FieldMap) GetInt32(field string) (int32, error) {
	if value, found := (*m)[field]; found {
		var result int32
		err := convert.Convert(value, &result)
		if err != nil {
			return 0, eris.Wrap(err, msg.InvalidValue)
		}
		return result, err
	}
	return 0, ErrNotFound
}

func (m *FieldMap) GetInt(field string) (int, error) {
	if value, found := (*m)[field]; found {
		var result int
		err := convert.Convert(value, &result)
		if err != nil {
			return 0, eris.Wrap(err, msg.InvalidValue)
		}
		return result, err
	}
	return 0, ErrNotFound
}

func (m *FieldMap) GetInt16(field string) (int16, error) {
	if value, found := (*m)[field]; found {
		var result int16
		err := convert.Convert(value, &result)
		if err != nil {
			return 0, eris.Wrap(err, msg.InvalidValue)
		}
		return result, err
	}
	return 0, ErrNotFound
}

func (m *FieldMap) GetInt8(field string) (int8, error) {
	if value, found := (*m)[field]; found {
		var result int8
		err := convert.Convert(value, &result)
		if err != nil {
			return 0, eris.Wrap(err, msg.InvalidValue)
		}
		return result, err
	}
	return 0, ErrNotFound
}

func (m *FieldMap) GetUInt64(field string) (uint64, error) {
	if value, found := (*m)[field]; found {
		var result uint64
		err := convert.Convert(value, &result)
		if err != nil {
			return 0, eris.Wrap(err, msg.InvalidValue)
		}
		return result, err
	}
	return 0, ErrNotFound
}

func (m *FieldMap) GetUInt32(field string) (uint32, error) {
	if value, found := (*m)[field]; found {
		var result uint32
		err := convert.Convert(value, &result)
		if err != nil {
			return 0, eris.Wrap(err, msg.InvalidValue)
		}
		return result, err
	}
	return 0, ErrNotFound
}

func (m *FieldMap) GetUInt(field string) (uint, error) {
	if value, found := (*m)[field]; found {
		var result uint
		err := convert.Convert(value, &result)
		if err != nil {
			return 0, eris.Wrap(err, msg.InvalidValue)
		}
		return result, err
	}
	return 0, ErrNotFound
}

func (m *FieldMap) GetUInt16(field string) (uint16, error) {
	if value, found := (*m)[field]; found {
		var result uint16
		err := convert.Convert(value, &result)
		if err != nil {
			return 0, eris.Wrap(err, msg.InvalidValue)
		}
		return result, err
	}
	return 0, ErrNotFound
}

func (m *FieldMap) GetUInt8(field string) (uint8, error) {
	if value, found := (*m)[field]; found {
		var result uint8
		err := convert.Convert(value, &result)
		if err != nil {
			return 0, eris.Wrap(err, msg.InvalidValue)
		}
		return result, err
	}
	return 0, ErrNotFound
}

func (m *FieldMap) GetByte(field string) (byte, error) {
	if value, found := (*m)[field]; found {
		var result byte
		err := convert.Convert(value, &result)
		if err != nil {
			return 0, eris.Wrap(err, msg.InvalidValue)
		}
		return result, err
	}
	return 0, ErrNotFound
}

// validate the given value and return fieldError if value isn't valid
// and the valid value if valid
func (f *FieldDef) ValidateValue(value interface{}) (interface{}, error) {
	if f.Options == nil {
		if f.Type == "string" {
			var validValue string
			err := convert.Convert(value, &validValue)
			if err != nil {
				return nil, eris.Wrap(err, msg.InvalidValue)
			}

			if f.Required {
				if len(validValue) == 0 {
					return nil, ErrFieldRequired
				}
			}
			return validValue, nil
		} else if f.Type == "byte" {
			var validValue byte
			err := convert.Convert(value, &validValue)
			if err != nil {
				return nil, eris.Wrap(err, msg.InvalidValue)
			}

			if f.Min.Valid && float64(validValue) < float64(f.Min.Int64) {
				return nil, ErrValueOutOfRange
			}
			if f.Max.Valid && float64(validValue) > float64(f.Max.Int64) {
				return nil, ErrValueOutOfRange
			}
			return validValue, nil
		} else if f.Type == "uint8" {
			var validValue uint8
			err := convert.Convert(value, &validValue)
			if err != nil {
				return nil, eris.Wrap(err, msg.InvalidValue)
			}

			if f.Min.Valid && float64(validValue) < float64(f.Min.Int64) {
				return nil, ErrValueOutOfRange
			}
			if f.Max.Valid && float64(validValue) > float64(f.Max.Int64) {
				return nil, ErrValueOutOfRange
			}
			return validValue, nil
		} else if f.Type == "uint16" {
			var validValue uint16
			err := convert.Convert(value, &validValue)
			if err != nil {
				return nil, eris.Wrap(err, msg.InvalidValue)
			}

			if f.Min.Valid && float64(validValue) < float64(f.Min.Int64) {
				return nil, ErrValueOutOfRange
			}
			if f.Max.Valid && float64(validValue) > float64(f.Max.Int64) {
				return nil, ErrValueOutOfRange
			}
			return validValue, nil
		} else if f.Type == "uint32" {
			var validValue uint32
			err := convert.Convert(value, &validValue)
			if err != nil {
				return nil, eris.Wrap(err, msg.InvalidValue)
			}

			if f.Min.Valid && float64(validValue) < float64(f.Min.Int64) {
				return nil, ErrValueOutOfRange
			}
			if f.Max.Valid && float64(validValue) > float64(f.Max.Int64) {
				return nil, ErrValueOutOfRange
			}
			return validValue, nil
		} else if f.Type == "uint64" {
			var validValue uint64
			err := convert.Convert(value, &validValue)
			if err != nil {
				return nil, eris.Wrap(err, msg.InvalidValue)
			}

			if f.Min.Valid && float64(validValue) < float64(f.Min.Int64) {
				return nil, ErrValueOutOfRange
			}
			if f.Max.Valid && float64(validValue) > float64(f.Max.Int64) {
				return nil, ErrValueOutOfRange
			}
			return validValue, nil
		} else if f.Type == "int8" {
			var validValue int8
			err := convert.Convert(value, &validValue)
			if err != nil {
				return nil, eris.Wrap(err, msg.InvalidValue)
			}

			if f.Min.Valid && float64(validValue) < float64(f.Min.Int64) {
				return nil, ErrValueOutOfRange
			}
			if f.Max.Valid && float64(validValue) > float64(f.Max.Int64) {
				return nil, ErrValueOutOfRange
			}
			return validValue, nil
		} else if f.Type == "int16" {
			var validValue int16
			err := convert.Convert(value, &validValue)
			if err != nil {
				return nil, eris.Wrap(err, msg.InvalidValue)
			}

			if f.Min.Valid && float64(validValue) < float64(f.Min.Int64) {
				return nil, ErrValueOutOfRange
			}
			if f.Max.Valid && float64(validValue) > float64(f.Max.Int64) {
				return nil, ErrValueOutOfRange
			}
			return validValue, nil
		} else if f.Type == "int32" {
			var validValue int32
			err := convert.Convert(value, &validValue)
			if err != nil {
				return nil, eris.Wrap(err, msg.InvalidValue)
			}

			if f.Min.Valid && float64(validValue) < float64(f.Min.Int64) {
				return nil, ErrValueOutOfRange
			}
			if f.Max.Valid && float64(validValue) > float64(f.Max.Int64) {
				return nil, ErrValueOutOfRange
			}
			return validValue, nil
		} else if f.Type == "int" {
			var validValue int
			err := convert.Convert(value, &validValue)
			if err != nil {
				return nil, eris.Wrap(err, msg.InvalidValue)
			}

			if f.Min.Valid && float64(validValue) < float64(f.Min.Int64) {
				return nil, ErrValueOutOfRange
			}
			if f.Max.Valid && float64(validValue) > float64(f.Max.Int64) {
				return nil, ErrValueOutOfRange
			}
			return validValue, nil
		} else if f.Type == "int64" {
			var validValue int64
			err := convert.Convert(value, &validValue)
			if err != nil {
				return nil, eris.Wrap(err, msg.InvalidValue)
			}

			if f.Min.Valid && float64(validValue) < float64(f.Min.Int64) {
				return nil, ErrValueOutOfRange
			}
			if f.Max.Valid && float64(validValue) > float64(f.Max.Int64) {
				return nil, ErrValueOutOfRange
			}
			return validValue, nil
		} else if f.Type == "float32" {
			var validValue float32
			err := convert.Convert(value, &validValue)
			if err != nil {
				return nil, eris.Wrap(err, msg.InvalidValue)
			}

			if f.Min.Valid && float64(validValue) < float64(f.Min.Int64) {
				return nil, ErrValueOutOfRange
			}
			if f.Max.Valid && float64(validValue) > float64(f.Max.Int64) {
				return nil, ErrValueOutOfRange
			}
			return validValue, nil
		} else if f.Type == "float64" {
			var validValue float64
			err := convert.Convert(value, &validValue)
			if err != nil {
				return nil, eris.Wrap(err, msg.InvalidValue)
			}

			if f.Min.Valid && float64(validValue) < float64(f.Min.Int64) {
				return nil, ErrValueOutOfRange
			}
			if f.Max.Valid && float64(validValue) > float64(f.Max.Int64) {
				return nil, ErrValueOutOfRange
			}
			return validValue, nil
		} else if f.Type == "bool" {
			var validValue bool
			err := convert.Convert(value, &validValue)
			if err != nil {
				return nil, eris.Wrap(err, msg.InvalidValue)
			}
			return validValue, nil
		}
	} else {
		for _, opt := range f.Options {
			if fmt.Sprint(opt) == fmt.Sprint(value) {
				if f.Type == "string" {
					return fmt.Sprint(opt), nil
				} else if f.Type == "byte" || f.Type == "uint8" {
					return byte(opt.(float64)), nil
				} else if f.Type == "uint16" {
					return uint16(opt.(float64)), nil
				} else if f.Type == "uint32" {
					return uint32(opt.(float64)), nil
				} else if f.Type == "uint64" {
					return int64(opt.(float64)), nil
				} else if f.Type == "int8" {
					return int8(opt.(float64)), nil
				} else if f.Type == "int16" {
					return int16(opt.(float64)), nil
				} else if f.Type == "int32" || f.Type == "int" {
					return int32(opt.(float64)), nil
				} else if f.Type == "int64" {
					return int64(opt.(float64)), nil
				} else if f.Type == "float32" {
					return float32(opt.(float64)), nil
				} else if f.Type == "float64" {
					return opt.(float64), nil
				} else if f.Type == "bool" {
					return opt.(bool), nil
				}
			}
		}
		return nil, ErrInvalidValue
	}
	return nil, nil
}

func (f *FieldDef) GetDataTypeID() ua.NodeID {
	switch f.Type {
	case "bool":
		return ua.DataTypeIDBoolean
	case "byte":
		return ua.DataTypeIDByte
	case "int16":
		return ua.DataTypeIDInt16
	case "int32":
		return ua.DataTypeIDInt32
	case "int64":
		return ua.DataTypeIDInt64
	case "uint16":
		return ua.DataTypeIDUInt16
	case "uint32":
		return ua.DataTypeIDUInt32
	case "uint64":
		return ua.DataTypeIDUInt64
	case "float32":
		return ua.DataTypeIDFloat
	case "float64":
		return ua.DataTypeIDDouble
	case "string":
		return ua.DataTypeIDString
	}
	log.Panicf("invalid data type %s in FieldDef ", f.Type)
	return ua.DataTypeIDBoolean
}
