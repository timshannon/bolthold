// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold

import (
	"errors"
	"reflect"

	"github.com/boltdb/bolt"
)

// ErrKeyExists is the error returned when data is being Inserted for a Key that already exists
var ErrKeyExists = errors.New("This Key already exists in this bolthold for this type")

// sequence tells bolthold to insert the key as the next sequence in the bucket
type sequence struct{}

// NextSequence is used to create a sequential key for inserts
// Inserts a uint64 as the key
// store.Insert(bolthold.NextSequence(), data)
func NextSequence() interface{} {
	return sequence{}
}

// Insert inserts the passed in data into the the bolthold
// If the the key already exists in the bolthold, then an ErrKeyExists is returned
func (s *Store) Insert(key, data interface{}) error {
	return s.Bolt().Update(func(tx *bolt.Tx) error {
		return s.TxInsert(tx, key, data)
	})
}

// TxInsert is the same as Insert except it allows you specify your own transaction
func (s *Store) TxInsert(tx *bolt.Tx, key, data interface{}) error {
	if !tx.Writable() {
		return bolt.ErrTxNotWritable
	}

	storer := newStorer(data)

	b, err := tx.CreateBucketIfNotExists([]byte(storer.Type()))
	if err != nil {
		return err
	}

	if _, ok := key.(sequence); ok {
		key, err = b.NextSequence()
		if err != nil {
			return err
		}
	}

	gk, err := encode(key)

	if err != nil {
		return err
	}

	if b.Get(gk) != nil {
		return ErrKeyExists
	}

	value, err := encode(data)
	if err != nil {
		return err
	}

	// insert data
	err = b.Put(gk, value)

	if err != nil {
		return err
	}

	// insert any indexes
	err = indexAdd(storer, tx, gk, data)
	if err != nil {
		return err
	}

	return nil
}

// Update updates an existing record in the bolthold
// if the Key doesn't already exist in the store, then it fails with ErrNotFound
func (s *Store) Update(key interface{}, data interface{}) error {
	return s.Bolt().Update(func(tx *bolt.Tx) error {
		return s.TxUpdate(tx, key, data)
	})
}

// TxUpdate is the same as Update except it allows you to specify your own transaction
func (s *Store) TxUpdate(tx *bolt.Tx, key interface{}, data interface{}) error {
	if !tx.Writable() {
		return bolt.ErrTxNotWritable
	}

	storer := newStorer(data)

	gk, err := encode(key)

	if err != nil {
		return err
	}

	b, err := tx.CreateBucketIfNotExists([]byte(storer.Type()))
	if err != nil {
		return err
	}

	existing := b.Get(gk)

	if existing == nil {
		return ErrNotFound
	}

	// delete any existing indexes
	existingVal := reflect.New(reflect.TypeOf(data)).Interface()

	err = decode(existing, existingVal)
	if err != nil {
		return err
	}

	err = indexDelete(storer, tx, gk, existingVal)
	if err != nil {
		return err
	}

	value, err := encode(data)
	if err != nil {
		return err
	}

	// put data
	err = b.Put(gk, value)
	if err != nil {
		return err
	}

	// insert any new indexes
	err = indexAdd(storer, tx, gk, data)
	if err != nil {
		return err
	}

	return nil
}

// Upsert inserts the record into the bolthold if it doesn't exist.  If it does already exist, then it updates
// the existing record
func (s *Store) Upsert(key interface{}, data interface{}) error {
	return s.Bolt().Update(func(tx *bolt.Tx) error {
		return s.TxUpsert(tx, key, data)
	})
}

// TxUpsert is the same as Upsert except it allows you to specify your own transaction
func (s *Store) TxUpsert(tx *bolt.Tx, key interface{}, data interface{}) error {
	if !tx.Writable() {
		return bolt.ErrTxNotWritable
	}

	storer := newStorer(data)

	gk, err := encode(key)

	if err != nil {
		return err
	}

	b, err := tx.CreateBucketIfNotExists([]byte(storer.Type()))
	if err != nil {
		return err
	}

	existing := b.Get(gk)

	if existing != nil {

		// delete any existing indexes
		existingVal := reflect.New(reflect.TypeOf(data)).Interface()

		err = decode(existing, existingVal)
		if err != nil {
			return err
		}

		err = indexDelete(storer, tx, gk, existingVal)
		if err != nil {
			return err
		}

	}

	value, err := encode(data)
	if err != nil {
		return err
	}

	// put data
	err = b.Put(gk, value)
	if err != nil {
		return err
	}

	// insert any new indexes
	err = indexAdd(storer, tx, gk, data)
	if err != nil {
		return err
	}

	return nil
}

// UpdateMatching runs the update function for every record that match the passed in query
// Note that the type  of record in the update func always has to be a pointer
func (s *Store) UpdateMatching(dataType interface{}, query *Query, update func(record interface{}) error) error {
	return s.Bolt().Update(func(tx *bolt.Tx) error {
		return s.TxUpdateMatching(tx, dataType, query, update)
	})
}

// TxUpdateMatching does the same as UpdateMatching, but allows you to specify your own transaction
func (s *Store) TxUpdateMatching(tx *bolt.Tx, dataType interface{}, query *Query, update func(record interface{}) error) error {
	return updateQuery(tx, dataType, query, update)
}
