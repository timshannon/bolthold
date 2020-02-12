// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold

import (
	"errors"
	"reflect"

	bolt "go.etcd.io/bbolt"
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
//
// If the the key already exists in the bolthold, then an ErrKeyExists is returned
// If the data struct has a field tagged as `boltholdKey` and it is the same type
// as the Insert key, AND the data struct is passed by reference, AND the key field
// is currently set to the zero-value for that type, then that field will be set to
// the value of the insert key.
//
// To use this with bolthold.NextSequence() use a type of `uint64` for the key field.
func (s *Store) Insert(key, data interface{}) error {
	return s.Bolt().Update(func(tx *bolt.Tx) error {
		return s.insert(tx, key, data)
	})
}

// TxInsert is the same as Insert except it allows you specify your own transaction
func (s *Store) TxInsert(tx *bolt.Tx, key, data interface{}) error {
	if !tx.Writable() {
		return bolt.ErrTxNotWritable
	}
	return s.insert(tx, key, data)
}

// InsertIntoBucket is the same as Insert except it allows you specify your own parent bucket
func (s *Store) InsertIntoBucket(parent *bolt.Bucket, key, data interface{}) error {
	if !parent.Tx().Writable() {
		return bolt.ErrTxNotWritable
	}
	return s.insert(parent, key, data)
}

func (s *Store) insert(source BucketSource, key, data interface{}) error {
	storer := s.newStorer(data)

	b, err := source.CreateBucketIfNotExists([]byte(storer.Type()))
	if err != nil {
		return err
	}

	if _, ok := key.(sequence); ok {
		key, err = b.NextSequence()
		if err != nil {
			return err
		}
	}

	gk, err := s.encode(key)

	if err != nil {
		return err
	}

	if b.Get(gk) != nil {
		return ErrKeyExists
	}

	value, err := s.encode(data)
	if err != nil {
		return err
	}

	// insert data
	err = b.Put(gk, value)

	if err != nil {
		return err
	}

	// insert any indexes
	err = s.addIndexes(storer, source, gk, data)
	if err != nil {
		return err
	}

	dataVal := reflect.Indirect(reflect.ValueOf(data))
	if !dataVal.CanSet() {
		return nil
	}
	dataType := dataVal.Type()

	for i := 0; i < dataType.NumField(); i++ {
		tf := dataType.Field(i)
		// XXX: should we require standard tag format so we can use StructTag.Lookup()?
		// XXX: should we use strings.Contains(string(tf.Tag), BoltholdKeyTag) so we don't require proper tags?
		if _, ok := tf.Tag.Lookup(BoltholdKeyTag); ok {
			fieldValue := dataVal.Field(i)
			keyValue := reflect.ValueOf(key)
			if keyValue.Type() != tf.Type {
				break
			}
			if !fieldValue.CanSet() {
				break
			}
			if !reflect.DeepEqual(fieldValue.Interface(), reflect.Zero(tf.Type).Interface()) {
				break
			}
			fieldValue.Set(keyValue)
			break
		}
	}

	return nil
}

// Update updates an existing record in the bolthold
// if the Key doesn't already exist in the store, then it fails with ErrNotFound
func (s *Store) Update(key interface{}, data interface{}) error {
	return s.Bolt().Update(func(tx *bolt.Tx) error {
		return s.update(tx, key, data)
	})
}

// TxUpdate is the same as Update except it allows you to specify your own transaction
func (s *Store) TxUpdate(tx *bolt.Tx, key interface{}, data interface{}) error {
	if !tx.Writable() {
		return bolt.ErrTxNotWritable
	}
	return s.update(tx, key, data)
}

// UpdateBucket allows you to run an update against any parent bucket
func (s *Store) UpdateBucket(parent *bolt.Bucket, key interface{}, data interface{}) error {
	if !parent.Tx().Writable() {
		return bolt.ErrTxNotWritable
	}
	return s.update(parent, key, data)

}

func (s *Store) update(source BucketSource, key interface{}, data interface{}) error {
	storer := s.newStorer(data)

	gk, err := s.encode(key)

	if err != nil {
		return err
	}

	b, err := source.CreateBucketIfNotExists([]byte(storer.Type()))
	if err != nil {
		return err
	}

	existing := b.Get(gk)

	if existing == nil {
		return ErrNotFound
	}

	// delete any existing indexes
	existingVal := reflect.New(reflect.TypeOf(data)).Interface()

	err = s.decode(existing, existingVal)
	if err != nil {
		return err
	}

	err = s.deleteIndexes(storer, source, gk, existingVal)
	if err != nil {
		return err
	}

	value, err := s.encode(data)
	if err != nil {
		return err
	}

	// put data
	err = b.Put(gk, value)
	if err != nil {
		return err
	}

	// insert any new indexes
	return s.addIndexes(storer, source, gk, data)
}

// Upsert inserts the record into the bolthold if it doesn't exist.  If it does already exist, then it updates
// the existing record
func (s *Store) Upsert(key interface{}, data interface{}) error {
	return s.Bolt().Update(func(tx *bolt.Tx) error {
		return s.upsert(tx, key, data)
	})
}

// TxUpsert is the same as Upsert except it allows you to specify your own transaction
func (s *Store) TxUpsert(tx *bolt.Tx, key interface{}, data interface{}) error {
	if !tx.Writable() {
		return bolt.ErrTxNotWritable
	}
	return s.upsert(tx, key, data)
}

// UpsertBucket allows you to run an upsert against any bucket parent
func (s *Store) UpsertBucket(parent *bolt.Bucket, key interface{}, data interface{}) error {
	if !parent.Tx().Writable() {
		return bolt.ErrTxNotWritable
	}
	return s.upsert(parent, key, data)
}

func (s *Store) upsert(source BucketSource, key interface{}, data interface{}) error {
	storer := s.newStorer(data)

	gk, err := s.encode(key)

	if err != nil {
		return err
	}

	b, err := source.CreateBucketIfNotExists([]byte(storer.Type()))
	if err != nil {
		return err
	}

	existing := b.Get(gk)

	if existing != nil {

		// delete any existing indexes
		existingVal := reflect.New(reflect.TypeOf(data)).Interface()

		err = s.decode(existing, existingVal)
		if err != nil {
			return err
		}

		err = s.deleteIndexes(storer, source, gk, existingVal)
		if err != nil {
			return err
		}

	}

	value, err := s.encode(data)
	if err != nil {
		return err
	}

	// put data
	err = b.Put(gk, value)
	if err != nil {
		return err
	}

	// insert any new indexes
	return s.addIndexes(storer, source, gk, data)
}

// UpdateMatching runs the update function for every record that match the passed in query
// Note that the type  of record in the update func always has to be a pointer
func (s *Store) UpdateMatching(dataType interface{}, query *Query, update func(record interface{}) error) error {
	return s.Bolt().Update(func(tx *bolt.Tx) error {
		return s.updateQuery(tx, dataType, query, update)
	})
}

// TxUpdateMatching does the same as UpdateMatching, but allows you to specify your own transaction
func (s *Store) TxUpdateMatching(tx *bolt.Tx, dataType interface{}, query *Query,
	update func(record interface{}) error) error {
	return s.updateQuery(tx, dataType, query, update)
}

// UpdateMatchingInBucket does the same as UpdateMatching, but allows you to specify your own parent bucket
func (s *Store) UpdateMatchingInBucket(parent *bolt.Bucket, dataType interface{}, query *Query,
	update func(record interface{}) error) error {
	return s.updateQuery(parent, dataType, query, update)
}
