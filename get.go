// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package gobstore

import (
	"errors"

	"github.com/boltdb/bolt"
)

// ErrNotFound is the error returned no data is found for the given key
var ErrNotFound = errors.New("No data found for this key")

// Get retrieves a value from the gobstore and puts it into result
func (s *Store) Get(key, result interface{}) error {
	return s.Bolt().View(func(tx *bolt.Tx) error {
		return s.TxGet(tx, key, result)
	})
}

// TxGet allows you to pass in your own bolt transaction to retrieve a value from the gobstore and puts it into result
func (s *Store) TxGet(tx *bolt.Tx, key, result interface{}) error {
	storer := newStorer(result)

	gk, err := encode(key)

	if err != nil {
		return err
	}

	value := tx.Bucket([]byte(storer.Type())).Get(gk)

	if value == nil {
		return ErrNotFound
	}

	return decode(value, result)
}

// exists returns if the given key exists in the passed in storer bucket
func (s *Store) exists(tx *bolt.Tx, key []byte, storer Storer) bool {
	return (tx.Bucket([]byte(storer.Type())).Get(key) != nil)
}
