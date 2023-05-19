// Copyright 2021 Converter Systems LLC. All rights reserved.

package ua

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/karlseguin/jsonwriter"
	"github.com/tidwall/gjson"
)

var (
	errFieldTextNotFound   = errors.New("field 'Text' not found")
	errFieldLocaleNotFound = errors.New("field 'Locale' not found")
)

// LocalizedText pairs text and a Locale string.
type LocalizedText struct {
	Text   string `xml:",innerxml"`
	Locale string `xml:"Locale,attr"`
}

// NewLocalizedText constructs a LocalizedText from text and Locale string.
func NewLocalizedText(text, locale string) LocalizedText {
	return LocalizedText{text, locale}
}

// String returns the string representation, e.g. "text (locale)"
func (a LocalizedText) String() string {
	if a.Locale == "" {
		return a.Text
	}
	return fmt.Sprintf("%s (%s)", a.Text, a.Locale)
}

func (n LocalizedText) MarshalJSON() ([]byte, error) {
	buffer := new(bytes.Buffer)
	writer := jsonwriter.New(buffer)
	writer.RootObject(func() {
		writer.KeyString("text", n.Text)
		writer.KeyString("locale", n.Locale)
	})
	return buffer.Bytes(), nil
}

func (n *LocalizedText) UnmarshalJSON(b []byte) error {
	jeText := gjson.GetBytes(b, "text")
	if !jeText.Exists() {
		return errFieldTextNotFound
	}
	var text string
	err := json.Unmarshal([]byte(jeText.Raw), &text)
	if err != nil {
		return err
	}

	jeLocale := gjson.GetBytes(b, "locale")
	if !jeLocale.Exists() {
		return errFieldLocaleNotFound
	}
	var locale string
	err = json.Unmarshal([]byte(jeLocale.Raw), &locale)
	if err != nil {
		return err
	}

	n.Text = text
	n.Locale = locale
	return nil
}
