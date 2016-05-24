// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package gobstore

import (
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/boltdb/bolt"
)

// Store is a gobstore wrapper around a bolt DB
type Store struct {
	db *bolt.DB
}

// Open opens or creates a gobstore file.  It uses a default timeout of 10 seconds, and a filemode of 0666
func Open(filename string) (*Store, error) {
	db, err := bolt.Open(filename, 0666, &bolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		return nil, err
	}

	return FromBolt(db)
}

// FromBolt returns a GobStore instance based on the already opened Bolt DB
func FromBolt(db *bolt.DB) (*Store, error) {
	return &Store{
		db: db,
	}, nil
}

// Bolt returns the underlying Bolt DB the gobstore is based on
func (s *Store) Bolt() *bolt.DB {
	return s.db
}

// Close closes the bolt db
func (s *Store) Close() error {
	return s.db.Close()
}

// AddIndexes adds all the indexes defined by the passed in Storer
// this automatically happens if an index is defined but not found in the store, and is checked on
// every write.  This Function allows you specify when the index write overhead happens
func (s *Store) AddIndexes(storer Storer) error {

	indexes := storer.Indexes()

	for i := range indexes {

	}

	return errors.New("TODO")
}

// RemoveIndex removes an index from the store.  If a Storer still has an index defined, or if a type
// as an index tag, it will be re-added on the next write
func (s *Store) RemoveIndex(storer Storer, indexName string) error {
	return errors.New("TODO")
}

// Storer is the Interface to implement to skip reflect calls on all data passed into the gobstore
type Storer interface {
	Type() string
	Indexes() []Index
}

// anonType is created from a reflection of an unknown interface
type anonStorer struct {
	rType   reflect.Type
	indexes []Index
}

// Type returns the name of the type as determined from the reflect package
func (t *anonStorer) Type() string {
	return t.rType.Name()
}

// Indexes returns the Indexes determined by the reflect package on this type
func (t *anonStorer) Indexes() []Index {
	return t.indexes
}

// NewStorer creates a type which satisfies the Storer interface based on reflection of the passed in dataType
// if the Type doesn't meet the requirements of a Storer (i.e. doesn't have a name) it panics
// You can avoid any reflection costs, by implementing the Storer interface on a type
func NewStorer(dataType interface{}) Storer {
	s, ok := dataType.(Storer)

	if ok {
		return s
	}

	storer := &anonStorer{
		rType: reflect.TypeOf(dataType),
	}

	if storer.rType.Name() == "" {
		panic("Invalid Type for Storer.  Type is unnamed")
	}

	for i := 0; i < storer.rType.NumField(); i++ {
		if strings.Contains(string(storer.rType.Field(i).Tag), GobStoreIndexTag) {
			indexName := storer.rType.Field(i).Tag.Get(GobStoreIndexTag)

			if indexName != "" {
				indexName = storer.rType.Field(i).Name
			}

			storer.indexes = append(storer.indexes, Index{
				Name: indexName,
				Func: func(name string, value interface{}) ([]byte, error) {
					return encode(reflect.ValueOf(value).FieldByName(name).Interface())
				},
			})
		}
	}

	return storer
}
