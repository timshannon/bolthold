// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold

import (
	"errors"

	"github.com/boltdb/bolt"
)

// ErrNotFound is returned when no data is found for the given key
var ErrNotFound = errors.New("No data found for this key")

// Get retrieves a value from bolthold and puts it into result.  Result must be a pointer
func (s *Store) Get(key, result interface{}) error {
	return s.Bolt().View(func(tx *bolt.Tx) error {
		return s.TxGet(tx, key, result)
	})
}

// TxGet allows you to pass in your own bolt transaction to retrieve a value from the bolthold and puts it into result
func (s *Store) TxGet(tx *bolt.Tx, key, result interface{}) error {
	storer := newStorer(result)

	gk, err := encode(key)

	if err != nil {
		return err
	}

	bkt := tx.Bucket([]byte(storer.Type()))
	if bkt == nil {
		return ErrNotFound
	}

	value := bkt.Get(gk)
	if value == nil {
		return ErrNotFound
	}

	return decode(value, result)
}

// Find retrieves a set of values from the bolthold that matches the passed in query
// result must be a pointer to a slice.
// The result of the query will be appended to the passed in result slice, rather than the passed in slice being
// emptied.
func (s *Store) Find(result interface{}, query *Query) error {
	return s.Bolt().View(func(tx *bolt.Tx) error {
		return s.TxFind(tx, result, query)
	})
}

// TxFind allows you to pass in your own bolt transaction to retrieve a set of values from the bolthold
func (s *Store) TxFind(tx *bolt.Tx, result interface{}, query *Query) error {
	return findQuery(tx, result, query)
}
