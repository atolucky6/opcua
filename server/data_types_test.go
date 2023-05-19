package server_test

import (
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/afs/server/pkg/opcua/server"
)

func TestConvertDataType(t *testing.T) {
	dtInt := &server.DTInt16{}
	max64 := math.MaxInt64
	value, err := dtInt.Convert(max64)
	t.Logf("%v , %v \n", value, err)

	value, err = strconv.ParseInt(fmt.Sprintf("%v", nil), 10, 16)
	t.Logf("%v , %v \n", value, err)
}
