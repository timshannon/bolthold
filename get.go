// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold

import (
	"errors"

	bolt "go.etcd.io/bbolt"
)

// ErrNotFound is returned when no data is found for the given key
var ErrNotFound = errors.New("No data found for this key")

// Get retrieves a value from bolthold and puts it into result.  Result must be a pointer
func (s *Store) Get(key, result interface{}) error {
	return s.Bolt().View(func(tx *bolt.Tx) error {
		return s.get(tx, key, result)
	})
}

// TxGet allows you to pass in your own bolt transaction to retrieve a value from the bolthold and puts it into result
func (s *Store) TxGet(tx *bolt.Tx, key, result interface{}) error {
	return s.get(tx, key, result)
}

// GetFromBucket allows you to specify the parent bucket for retrieving records
func (s *Store) GetFromBucket(parent *bolt.Bucket, key, result interface{}) error {
	return s.get(parent, key, result)
}

func (s *Store) get(source bucketSource, key, result interface{}) error {
	storer := s.newStorer(result)

	gk, err := s.encode(key)

	if err != nil {
		return err
	}

	bkt := source.Bucket([]byte(storer.Type()))
	if bkt == nil {
		return ErrNotFound
	}

	value := bkt.Get(gk)
	if value == nil {
		return ErrNotFound
	}

	return s.decode(value, result)
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
	return s.findQuery(tx, result, query)
}

// FindInBucket allows you to specify a parent bucke to search in
func (s *Store) FindInBucket(parent *bolt.Bucket, result interface{}, query *Query) error {
	return s.findQuery(parent, result, query)
}

// FindOne returns a single record, and so result is NOT a slice, but an pointer to a struct, if no record is found
// that matches the query, then it returns ErrNotFound
func (s *Store) FindOne(result interface{}, query *Query) error {
	return s.Bolt().View(func(tx *bolt.Tx) error {
		return s.findOneQuery(tx, result, query)
	})
}

// TxFindOne allows you to pass in your own bolt transaction to retrieve a single record from the bolthold
func (s *Store) TxFindOne(tx *bolt.Tx, result interface{}, query *Query) error {
	return s.findOneQuery(tx, result, query)
}

func (s *Store) TxFindOneInBucket(parent *bolt.Bucket, result interface{}, query *Query) error {
	return s.findOneQuery(parent, result, query)
}

// Count returns the current record count for the passed in datatype
func (s *Store) Count(dataType interface{}, query *Query) (int, error) {
	count := 0
	err := s.Bolt().View(func(tx *bolt.Tx) error {
		var txErr error
		count, txErr = s.TxCount(tx, dataType, query)
		return txErr
	})
	return count, err
}

// TxCount returns the current record count from within the given transaction for the passed in datatype
func (s *Store) TxCount(tx *bolt.Tx, dataType interface{}, query *Query) (int, error) {
	return s.countQuery(tx, dataType, query)
}

// CountInBucket returns the current record count from within the given parent bucket
func (s *Store) CountInBucket(parent *bolt.Bucket, dataType interface{}, query *Query) (int, error) {
	return s.countQuery(parent, dataType, query)
}
