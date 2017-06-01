// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold

import (
	"os"
	"reflect"
	"strings"

	"github.com/boltdb/bolt"
)

// Store is a bolthold wrapper around a bolt DB
type Store struct {
	db *bolt.DB
}

// Options allows you set different options from the defaults
// For example the encoding and decoding funcs which default to Gob
type Options struct {
	Encoder EncodeFunc
	Decoder DecodeFunc
	*bolt.Options
}

// Open opens or creates a bolthold file.
func Open(filename string, mode os.FileMode, options *Options) (*Store, error) {
	options = fillOptions(options)

	encode = options.Encoder
	decode = options.Decoder

	db, err := bolt.Open(filename, mode, options.Options)
	if err != nil {
		return nil, err
	}

	return &Store{
		db: db,
	}, nil
}

// set any unspecified options to defaults
func fillOptions(options *Options) *Options {
	if options == nil {
		options = &Options{}
	}

	if options.Encoder == nil {
		options.Encoder = DefaultEncode
	}
	if options.Decoder == nil {
		options.Decoder = DefaultDecode
	}

	return options
}

// Bolt returns the underlying Bolt DB the bolthold is based on
func (s *Store) Bolt() *bolt.DB {
	return s.db
}

// Close closes the bolt db
func (s *Store) Close() error {
	return s.db.Close()
}

// ReIndex removes any existing indexes and adds all the indexes defined by the passed in datatype example
// This function allows you to index an already existing boltDB file, or refresh any missing indexes
// if bucketName is nil, then we'll assume a bucketName of storer.Type()
// if a bucketname is specified, then the data will be copied to the bolthold standard bucket of storer.Type()
func (s *Store) ReIndex(exampleType interface{}, bucketName []byte) error {
	storer := newStorer(exampleType)

	return s.Bolt().Update(func(tx *bolt.Tx) error {
		indexes := storer.Indexes()
		// delete existing indexes
		// TODO: Remove indexes not specified the storer index list?
		// good for cleanup, bad for possible side effects

		for indexName := range indexes {
			err := tx.DeleteBucket(indexBucketName(storer.Type(), indexName))
			if err != nil && err != bolt.ErrBucketNotFound {
				return err
			}
		}

		copyData := true

		if bucketName == nil {
			bucketName = []byte(storer.Type())
			copyData = false
		}

		c := tx.Bucket(bucketName).Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			if copyData {
				b, err := tx.CreateBucketIfNotExists([]byte(storer.Type()))
				if err != nil {
					return err
				}

				err = b.Put(k, v)
				if err != nil {
					return err
				}
			}
			err := decode(v, exampleType)
			if err != nil {
				return err
			}
			err = indexAdd(storer, tx, k, exampleType)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// RemoveIndex removes an index from the store.
func (s *Store) RemoveIndex(dataType interface{}, indexName string) error {
	storer := newStorer(dataType)
	return s.Bolt().Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(indexBucketName(storer.Type(), indexName))

	})
}

// Storer is the Interface to implement to skip reflect calls on all data passed into the bolthold
type Storer interface {
	Type() string              // used as the boltdb bucket name
	Indexes() map[string]Index //[indexname]indexFunc
}

// anonType is created from a reflection of an unknown interface
type anonStorer struct {
	rType   reflect.Type
	indexes map[string]Index
}

// Type returns the name of the type as determined from the reflect package
func (t *anonStorer) Type() string {
	return t.rType.Name()
}

// Indexes returns the Indexes determined by the reflect package on this type
func (t *anonStorer) Indexes() map[string]Index {
	return t.indexes
}

// newStorer creates a type which satisfies the Storer interface based on reflection of the passed in dataType
// if the Type doesn't meet the requirements of a Storer (i.e. doesn't have a name) it panics
// You can avoid any reflection costs, by implementing the Storer interface on a type
func newStorer(dataType interface{}) Storer {
	s, ok := dataType.(Storer)

	if ok {
		return s
	}

	tp := reflect.TypeOf(dataType)

	for tp.Kind() == reflect.Ptr {
		tp = tp.Elem()
	}

	storer := &anonStorer{
		rType:   tp,
		indexes: make(map[string]Index),
	}

	if storer.rType.Name() == "" {
		panic("Invalid Type for Storer.  Type is unnamed")
	}

	if storer.rType.Kind() != reflect.Struct {
		panic("Invalid Type for Storer.  BoltHold only works with structs")
	}

	for i := 0; i < storer.rType.NumField(); i++ {
		if strings.Contains(string(storer.rType.Field(i).Tag), BoltholdIndexTag) {
			indexName := storer.rType.Field(i).Tag.Get(BoltholdIndexTag)

			if indexName != "" {
				indexName = storer.rType.Field(i).Name
			}

			storer.indexes[indexName] = func(name string, value interface{}) ([]byte, error) {
				tp := reflect.ValueOf(value)
				for tp.Kind() == reflect.Ptr {
					tp = tp.Elem()
				}

				return encode(tp.FieldByName(name).Interface())
			}
		}
	}

	return storer
}
