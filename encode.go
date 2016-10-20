// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold

import (
	"bytes"
	"encoding/gob"
)

type encodeFunc func(value interface{}) ([]byte, error)
type decodeFunc func(data []byte, value interface{}) error

var encode encodeFunc
var decode decodeFunc

func defaultEncode(value interface{}) ([]byte, error) {
	var buff bytes.Buffer

	en := gob.NewEncoder(&buff)

	err := en.Encode(value)
	if err != nil {
		return nil, err
	}

	return buff.Bytes(), nil
}

func defaultDecode(data []byte, value interface{}) error {
	var buff bytes.Buffer
	de := gob.NewDecoder(&buff)

	_, err := buff.Write(data)
	if err != nil {
		return err
	}

	err = de.Decode(value)
	if err != nil {
		return err
	}

	return nil
}
