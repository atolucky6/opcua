// Copyright 2021 Converter Systems LLC. All rights reserved.

package ua

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/karlseguin/jsonwriter"
	"github.com/tidwall/gjson"
)

var (
	errFieldNameNotFound = errors.New("field 'name' not found")
)

// QualifiedName pairs a name and a namespace index.
type QualifiedName struct {
	NamespaceIndex uint16
	Name           string
}

// NewQualifiedName constructs a QualifiedName from a namespace index and a name.
func NewQualifiedName(ns uint16, text string) QualifiedName {
	return QualifiedName{ns, text}
}

// ParseQualifiedName returns a QualifiedName from a string, e.g. ParseQualifiedName("2:Demo")
func ParseQualifiedName(s string) QualifiedName {
	var ns uint64
	var pos = strings.Index(s, ":")
	if pos == -1 {
		return QualifiedName{uint16(ns), s}
	}
	ns, err := strconv.ParseUint(s[:pos], 10, 16)
	if err != nil {
		return QualifiedName{uint16(ns), s}
	}
	s = s[pos+1:]
	return QualifiedName{uint16(ns), s}
}

// ParseBrowsePath returns a slice of QualifiedNames from a string, e.g. ParseBrowsePath("2:Demo/2:Dynamic")
func ParseBrowsePath(s string) []QualifiedName {
	//TODO: see part4 Annex A.2
	if len(s) == 0 {
		return []QualifiedName{}
	}
	toks := strings.Split(s, "/")
	path := make([]QualifiedName, len(toks))
	for i, tok := range toks {
		path[i] = ParseQualifiedName(tok)
	}
	return path
}

// String returns a string representation, e.g. "2:Demo"
func (a QualifiedName) String() string {
	return fmt.Sprintf("%d:%s", a.NamespaceIndex, a.Name)
}

func (n QualifiedName) MarshalJSON() ([]byte, error) {
	buffer := new(bytes.Buffer)
	writer := jsonwriter.New(buffer)
	writer.RootObject(func() {
		if n.NamespaceIndex != 0 {
			writer.KeyValue("uri", n.NamespaceIndex)
		}
		writer.KeyString("name", n.Name)
	})
	return buffer.Bytes(), nil
}

func (n *QualifiedName) UnmarshalJSON(b []byte) error {
	jeUri := gjson.GetBytes(b, "uri")
	var uri uint16
	if jeUri.Exists() {
		err := json.Unmarshal([]byte(jeUri.Raw), &uri)
		if err != nil {
			return err
		}
	}
	jeName := gjson.GetBytes(b, "name")
	if !jeName.Exists() {
		return errFieldNameNotFound
	}
	var name string
	err := json.Unmarshal([]byte(jeName.Raw), &name)
	if err != nil {
		return err
	}
	n.NamespaceIndex = uri
	n.Name = name
	return nil
}
