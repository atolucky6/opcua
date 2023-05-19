package server

import (
	"errors"
	"math"

	"github.com/Eun/go-convert"
	"github.com/afs/server/pkg/eris"
	"github.com/afs/server/pkg/msg"
)

const (
	SCALE_TYPE_NONE        = "None"
	SCALE_TYPE_LINEAR      = "Linear"
	SCALE_TYPE_SQUARE_ROOT = "Square root"
)

var (
	errScaleValueIsNull = errors.New("can't scale nil value")
	errInvalidScaleType = errors.New("invalid scale type")
)

func ReadScale(mode string, value interface{}, scaledDT IDataType, rawLow, rawHigh, scaledLow, scaledHigh, readScaleFactor float64, clampLow, clamHigh, negateValue bool) (interface{}, error) {
	if value == nil {
		return nil, errScaleValueIsNull
	}

	if mode == SCALE_TYPE_NONE {
		return value, nil
	}

	if mode == SCALE_TYPE_LINEAR {
		return ReadLinearScale(value, scaledDT, rawLow, rawHigh, scaledLow, scaledHigh, readScaleFactor, clampLow, clamHigh, negateValue)
	}

	if mode == SCALE_TYPE_SQUARE_ROOT {
		return ReadSquareRootScale(value, scaledDT, rawLow, rawHigh, scaledLow, scaledHigh, readScaleFactor, clampLow, clamHigh, negateValue)
	}

	return nil, errInvalidScaleType
}

func WriteScale(mode string, value interface{}, scaledDT IDataType, rawLow, rawHigh, scaledLow, scaledHigh, writeScaleFactor float64, clampLow, clamHigh, negateValue bool) (interface{}, error) {
	if value == nil {
		return nil, errScaleValueIsNull
	}

	if mode == SCALE_TYPE_NONE {
		return value, nil
	}

	if mode == SCALE_TYPE_LINEAR {
		return WriteLinearScale(value, scaledDT, rawLow, rawHigh, scaledLow, scaledHigh, writeScaleFactor, clampLow, clamHigh, negateValue)
	}

	if mode == SCALE_TYPE_SQUARE_ROOT {
		return WriteSquareRootScale(value, scaledDT, rawLow, rawHigh, scaledLow, scaledHigh, writeScaleFactor, clampLow, clamHigh, negateValue)
	}

	return nil, errInvalidScaleType
}

func ReadLinearScale(value interface{}, scaledDT IDataType, rawLow, rawHigh, scaledLow, scaledHigh, readScaleFactor float64, clampLow, clamHigh, negateValue bool) (interface{}, error) {
	var valuef64 float64
	err := convert.Convert(value, &valuef64)
	if err != nil {
		return nil, err
	}

	scaledValue := (valuef64-rawLow)*readScaleFactor + scaledLow
	if clampLow && scaledValue < scaledLow {
		scaledValue = scaledLow
	}

	if clamHigh && scaledValue > scaledHigh {
		scaledValue = scaledHigh
	}

	if negateValue {
		scaledValue = scaledValue * -1
	}
	return scaledDT.Convert(scaledValue)
}

func WriteLinearScale(value interface{}, scaledDT IDataType, rawLow, rawHigh, scaledLow, scaledHigh, writeScaleFactor float64, clampLow, clamHigh, negateValue bool) (interface{}, error) {
	var valuef64 float64
	err := convert.Convert(value, &valuef64)
	if err != nil {
		return nil, err
	}

	if negateValue {
		valuef64 = valuef64 * -1
	}
	if clampLow && valuef64 < scaledLow {
		valuef64 = scaledLow
	}
	if clamHigh && valuef64 > scaledHigh {
		valuef64 = scaledHigh
	}

	scaledValue := (valuef64-scaledLow)*writeScaleFactor + rawLow
	return scaledDT.Convert(scaledValue)
}

func ReadSquareRootScale(value interface{}, scaledDT IDataType, rawLow, rawHigh, scaledLow, scaledHigh, readScaleFactor float64, clampLow, clamHigh, negateValue bool) (interface{}, error) {
	var valuef64 float64
	err := convert.Convert(value, &valuef64)
	if err != nil {
		return nil, err
	}
	scaledValue := math.Sqrt(valuef64-rawLow)*readScaleFactor + scaledLow
	if clampLow && scaledValue < scaledLow {
		scaledValue = scaledLow
	}

	if clamHigh && scaledValue > scaledHigh {
		scaledValue = scaledHigh
	}

	if negateValue {
		scaledValue = scaledValue * -1
	}
	return scaledDT.Convert(scaledValue)
}

func WriteSquareRootScale(value interface{}, scaledDT IDataType, rawLow, rawHigh, scaledLow, scaledHigh, writeScaleFactor float64, clampLow, clamHigh, negateValue bool) (interface{}, error) {
	var valuef64 float64
	err := convert.Convert(value, &valuef64)
	if err != nil {
		return nil, err
	}

	if negateValue {
		valuef64 = valuef64 * -1
	}
	if clampLow && valuef64 < scaledLow {
		valuef64 = scaledLow
	}
	if clamHigh && valuef64 > scaledHigh {
		valuef64 = scaledHigh
	}
	scaledValue := math.Pow(valuef64-scaledLow, 2)*writeScaleFactor + rawLow
	return scaledDT.Convert(scaledValue)
}

func GetReadScaleFactor(mode string, rawLow, rawHigh, scaledLow, scaledHigh float64) float64 {
	if mode == SCALE_TYPE_LINEAR {
		return (scaledHigh - scaledLow) / (rawHigh - rawLow)
	}

	if mode == SCALE_TYPE_SQUARE_ROOT {
		return (scaledHigh - scaledLow) / math.Sqrt(rawHigh-rawLow)
	}
	return 1
}

func GetWriteScaleFactor(mode string, rawLow, rawHigh, scaledLow, scaledHigh float64) float64 {
	if mode == SCALE_TYPE_LINEAR {
		return (rawHigh - rawLow) / (scaledHigh - scaledLow)
	}

	if mode == SCALE_TYPE_SQUARE_ROOT {
		return (rawHigh - rawLow) / math.Pow(scaledHigh-scaledLow, 2)
	}
	return 1
}

func ValidateScalingProperties(tag *ObjectNode) map[string]error {
	fieldErrors := map[string]error{}
	rawLow := tag.MustGetProperty("RawLow").GetValue().Value.(float64)
	rawHigh := tag.MustGetProperty("RawHigh").GetValue().Value.(float64)
	scaledLow := tag.MustGetProperty("ScaledLow").GetValue().Value.(float64)
	scaledHigh := tag.MustGetProperty("ScaledHigh").GetValue().Value.(float64)

	if rawLow < 0 {
		fieldErrors["RawLow"] = eris.New(msg.RawLowOutOfRange)
	}

	if rawHigh <= rawLow {
		fieldErrors["RawLow"] = eris.New(msg.RawLowMustBeSmallerThanRawHigh)
		fieldErrors["RawHigh"] = eris.New(msg.RawHighMustBeGreaterThanRawLow)
	}

	if scaledHigh <= scaledLow {
		fieldErrors["ScaledLow"] = eris.New(msg.ScaledLowMustBeSmallerThanScaledHigh)
		fieldErrors["ScaledHigh"] = eris.New(msg.ScaledHighMustBeGreaterThanScaledLow)
	}

	scaledDT, err := NewDataType(tag.MustGetProperty("ScaledDataType").GetValue().Value.(string))
	if err != nil {
		fieldErrors["ScaledDataType"] = eris.New(msg.InvalidScaledDataType)
	} else {
		_, err := scaledDT.Convert(scaledHigh)
		if err != nil {
			fieldErrors["ScaledDataType"] = eris.New(msg.ScaledDataTypeCouldNotConvertScaledHighValue)
		}

		_, err = scaledDT.Convert(scaledLow)
		if err != nil {
			fieldErrors["ScaledDataType"] = eris.New(msg.ScaledDataTypeCouldNotConvertScaledLowValue)
		}
	}
	return fieldErrors
}

func ValidateScaling(rawLow, rawHigh, scaledLow, scaledHigh float64, scaledDataType string) map[string]error {
	fieldErrors := map[string]error{}
	if rawLow < 0 {
		fieldErrors["RawLow"] = eris.New(msg.RawLowOutOfRange)
	}

	if rawHigh <= rawLow {
		fieldErrors["RawLow"] = eris.New(msg.RawLowMustBeSmallerThanRawHigh)
		fieldErrors["RawHigh"] = eris.New(msg.RawHighMustBeGreaterThanRawLow)
	}

	if scaledHigh <= scaledLow {
		fieldErrors["ScaledLow"] = eris.New(msg.ScaledLowMustBeSmallerThanScaledHigh)
		fieldErrors["ScaledHigh"] = eris.New(msg.ScaledHighMustBeGreaterThanScaledLow)
	}

	scaledDT, err := NewDataType(scaledDataType)
	if err != nil {
		fieldErrors["ScaledDataType"] = eris.New(msg.InvalidScaledDataType)
	} else {
		_, err := scaledDT.Convert(scaledHigh)
		if err != nil {
			fieldErrors["ScaledDataType"] = eris.New(msg.ScaledDataTypeCouldNotConvertScaledHighValue)
		}

		_, err = scaledDT.Convert(scaledLow)
		if err != nil {
			fieldErrors["ScaledDataType"] = eris.New(msg.ScaledDataTypeCouldNotConvertScaledLowValue)
		}
	}
	return fieldErrors
}
