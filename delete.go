// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold

import (
	"reflect"

	bolt "go.etcd.io/bbolt"
)

// Delete deletes a record from the bolthold, datatype just needs to be an example of the type stored so that
// the proper bucket and indexes are updated
func (s *Store) Delete(key, dataType interface{}) error {
	return s.Bolt().Update(func(tx *bolt.Tx) error {
		return s.delete(tx, key, dataType)
	})
}

// TxDelete is the same as Delete except it allows you specify your own transaction
func (s *Store) TxDelete(tx *bolt.Tx, key, dataType interface{}) error {
	if !tx.Writable() {
		return bolt.ErrTxNotWritable
	}
	return s.delete(tx, key, dataType)
}

// DeleteFromBucket allows you to specify the parent bucket to delete from
func (s *Store) DeleteFromBucket(parent *bolt.Bucket, key, dataType interface{}) error {
	if !parent.Tx().Writable() {
		return bolt.ErrTxNotWritable
	}
	return s.delete(parent, key, dataType)
}

func (s *Store) delete(source BucketSource, key, dataType interface{}) error {
	storer := s.newStorer(dataType)
	gk, err := s.encode(key)

	if err != nil {
		return err
	}

	b := source.Bucket([]byte(storer.Type()))
	if b == nil {
		return ErrNotFound
	}

	value := reflect.New(reflect.TypeOf(dataType)).Interface()

	bVal := b.Get(gk)

	err = s.decode(bVal, value)
	if err != nil {
		return err
	}

	// delete data
	err = b.Delete(gk)

	if err != nil {
		return err
	}

	// remove any indexes
	return s.deleteIndexes(storer, source, gk, value)
}

// DeleteMatching deletes all of the records that match the passed in query
func (s *Store) DeleteMatching(dataType interface{}, query *Query) error {
	return s.Bolt().Update(func(tx *bolt.Tx) error {
		return s.TxDeleteMatching(tx, dataType, query)
	})
}

// TxDeleteMatching does the same as DeleteMatching, but allows you to specify your own transaction
func (s *Store) TxDeleteMatching(tx *bolt.Tx, dataType interface{}, query *Query) error {
	return s.deleteQuery(tx, dataType, query)
}

// DeleteMatchingFromBucket does the same as DeleteMatching, but allows you to specify your own parent bucket
func (s *Store) DeleteMatchingFromBucket(parent *bolt.Bucket, dataType interface{}, query *Query) error {
	return s.deleteQuery(parent, dataType, query)
}
