// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package gobstore

import (
	"bytes"
	"encoding/gob"
	"sync"
)

var encodePool, decodePool sync.Pool

func init() {
	encodePool = sync.Pool{
		New: func() interface{} {
			var buff bytes.Buffer
			return &encoder{
				buffer:  buff,
				Encoder: gob.NewEncoder(&buff),
			}
		},
	}

	decodePool = sync.Pool{
		New: func() interface{} {
			var buff bytes.Buffer
			return &decoder{
				buffer:  buff,
				Decoder: gob.NewDecoder(&buff),
			}
		},
	}

}

type encoder struct {
	buffer bytes.Buffer
	*gob.Encoder
}

type decoder struct {
	buffer bytes.Buffer
	*gob.Decoder
}

func encode(value interface{}) ([]byte, error) {
	en := encodePool.Get().(*encoder)
	defer encodePool.Put(en)

	en.buffer.Reset()

	err := en.Encode(value)
	if err != nil {
		return nil, err
	}

	return en.buffer.Bytes(), nil
}

func decode(data []byte, value interface{}) error {
	de := decodePool.Get().(*decoder)
	defer decodePool.Put(de)

	de.buffer.Reset()

	_, err := de.buffer.Write(data)
	if err != nil {
		return err
	}

	err = de.Decode(value)
	if err != nil {
		return err
	}

	return nil
}
