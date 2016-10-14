// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package gobstore

import (
	"errors"

	"github.com/boltdb/bolt"
)

// ErrKeyExists is the error returned when data is being Inserted for a Key that already exists
var ErrKeyExists = errors.New("This Key already exists in this gobstore for this type")

// Insert inserts the passed in data into the the GobStore
// If the the key already exists in the gobstore, then an ErrKeyExists is returned
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

	gk, err := encode(key)

	if err != nil {
		return err
	}

	b, err := tx.CreateBucketIfNotExists([]byte(storer.Type()))
	if err != nil {
		return err
	}

	if s.exists(tx, gk, storer) {
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

// Update updates an existing record in the GobStore
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

	if !s.exists(tx, gk, storer) {
		return ErrNotFound
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

	// delete any existing indexes
	err = indexDelete(storer, tx, gk, data)
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

// Upsert inserts the record into the gobstore if it doesn't exist.  If it does already exist, then it updates
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

	exists := s.exists(tx, gk, storer)

	value, err := encode(data)
	if err != nil {
		return err
	}

	// put data
	err = b.Put(gk, value)
	if err != nil {
		return err
	}

	if exists {
		// delete any existing indexes
		err = indexDelete(storer, tx, gk, data)
		if err != nil {
			return err
		}
	}

	// insert any new indexes
	err = indexAdd(storer, tx, gk, data)
	if err != nil {
		return err
	}

	return nil
}
