// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold

import (
	"reflect"

	"github.com/boltdb/bolt"
)

// Delete deletes a record from the bolthold, datatype just needs to be an example of the type stored so that
// indexes can be updated
func (s *Store) Delete(key, dataType interface{}) error {
	return s.Bolt().Update(func(tx *bolt.Tx) error {
		return s.TxDelete(tx, key, dataType)
	})
}

// TxDelete is the same as Delete except it allows you specify your own transaction
func (s *Store) TxDelete(tx *bolt.Tx, key, dataType interface{}) error {
	if !tx.Writable() {
		return bolt.ErrTxNotWritable
	}

	storer := newStorer(dataType)
	gk, err := encode(key)

	if err != nil {
		return err
	}

	b := tx.Bucket([]byte(storer.Type()))
	if b == nil {
		return ErrNotFound
	}

	value := reflect.New(reflect.TypeOf(dataType))

	bVal := b.Get(gk)

	err = decode(bVal, value)
	if err != nil {
		return err
	}

	// delete data
	err = b.Delete(gk)

	if err != nil {
		return err
	}

	// remove any indexes
	err = indexDelete(storer, tx, gk, value)
	if err != nil {
		return err
	}

	return nil
}
