// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	bolt "go.etcd.io/bbolt"
)

// Store is a bolthold wrapper around a bolt DB
type Store struct {
	db     *bolt.DB
	encode EncodeFunc
	decode DecodeFunc
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

	db, err := bolt.Open(filename, mode, options.Options)
	if err != nil {
		return nil, err
	}

	return &Store{
		db:     db,
		encode: options.Encoder,
		decode: options.Decoder,
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
	storer := s.newStorer(exampleType)

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

		sliceIndexes := storer.SliceIndexes()

		for indexName := range sliceIndexes {
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

		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			// no data / nothing to do,
			return nil
		}

		c := bucket.Cursor()

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
			err := s.decode(v, exampleType)
			if err != nil {
				return err
			}
			err = s.addIndexes(storer, tx, k, exampleType)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// RemoveIndex removes an index from the store.
func (s *Store) RemoveIndex(dataType interface{}, indexName string) error {
	storer := s.newStorer(dataType)
	return s.Bolt().Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(indexBucketName(storer.Type(), indexName))

	})
}

// Storer is the Interface to implement to skip reflect calls on all data passed into the bolthold
type Storer interface {
	Type() string                        // used as the boltdb bucket name
	Indexes() map[string]Index           // [indexname]indexFunc
	SliceIndexes() map[string]SliceIndex // [indexname]sliceIndexFunc
}

// anonType is created from a reflection of an unknown interface. This is the default storer used
type anonStorer struct {
	rType        reflect.Type
	indexes      map[string]Index
	sliceIndexes map[string]SliceIndex
}

// Type returns the name of the type as determined from the reflect package
func (t *anonStorer) Type() string {
	return t.rType.Name()
}

// Indexes returns the Indexes determined by the reflect package on this type
func (t *anonStorer) Indexes() map[string]Index {
	return t.indexes
}

// SliceIndexes returns the Indexes determined by the reflect package on this type
func (t *anonStorer) SliceIndexes() map[string]SliceIndex {
	return t.sliceIndexes
}

// newStorer creates a type which satisfies the Storer interface based on reflection of the passed in dataType
// if the Type doesn't meet the requirements of a Storer (i.e. doesn't have a name) it panics
// You can avoid any reflection costs, by implementing the Storer interface on a type
func (s *Store) newStorer(dataType interface{}) Storer {
	str, ok := dataType.(Storer)

	if ok {
		return str
	}

	tp := reflect.TypeOf(dataType)

	for tp.Kind() == reflect.Ptr {
		tp = tp.Elem()
	}

	storer := &anonStorer{
		rType:        tp,
		indexes:      make(map[string]Index),
		sliceIndexes: make(map[string]SliceIndex),
	}

	if storer.rType.Name() == "" {
		panic("Invalid Type for Storer.  Type is unnamed")
	}

	if storer.rType.Kind() != reflect.Struct {
		panic("Invalid Type for Storer.  BoltHold only works with structs")
	}

	for i := 0; i < storer.rType.NumField(); i++ {
		storer.addIndex(storer.rType.Field(i), s)
	}

	return storer
}

func (t *anonStorer) addIndex(field reflect.StructField, store *Store) {
	if field.Anonymous {
		anonType := field.Type
		if anonType.Kind() == reflect.Ptr {
			anonType = anonType.Elem()
		}
		for j := 0; j < anonType.NumField(); j++ {
			t.addIndex(anonType.Field(j), store)
		}
		return
	}

	if strings.Contains(string(field.Tag), BoltholdIndexTag) {
		indexName := field.Tag.Get(BoltholdIndexTag)

		if indexName == "" {
			indexName = field.Name
		}

		t.indexes[indexName] = func(name string, value interface{}) ([]byte, error) {
			val := findIndexValue(name, value, BoltholdIndexTag)
			if val == nil {
				return nil, nil
			}
			return store.encode(val)
		}
	}
	if strings.Contains(string(field.Tag), BoltholdSliceIndexTag) {
		indexName := field.Tag.Get(BoltholdSliceIndexTag)

		if indexName == "" {
			indexName = field.Name
		}

		t.sliceIndexes[indexName] = func(name string, value interface{}) ([][]byte, error) {
			val := reflect.ValueOf(value)
			for val.Kind() == reflect.Ptr {
				if val.IsNil() {
					return nil, nil
				}
				val = val.Elem()
			}

			fldValue := findIndexValue(name, value, BoltholdSliceIndexTag)
			if fldValue == nil {
				return nil, nil
			}
			fld := reflect.ValueOf(fldValue)

			if fld.Kind() != reflect.Slice {
				return nil, fmt.Errorf("Type %s is not a slice", fld.Type())
			}

			indexValue := make(keyList, 0)

			for i := 0; i < fld.Len(); i++ {
				b, err := store.encode(fld.Index(i).Interface())
				if err != nil {
					return nil, err
				}
				indexValue.add(b)
			}

			return indexValue, nil
		}
	}
}

// returns the value in the field with the matching indexStruct tag
func findIndexValue(name string, value interface{}, tag string) interface{} {
	val := reflect.ValueOf(value)
	if val.Kind() == reflect.Ptr && val.IsNil() {
		return nil
	}
	for val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	valType := val.Type()
	for i := 0; i < valType.NumField(); i++ {
		if valType.Field(i).Anonymous {
			anonVal := findIndexValue(name, val.Field(i).Interface(), tag)

			if anonVal != nil {
				return anonVal
			}
			continue
		}
		field := valType.Field(i)
		if strings.Contains(string(field.Tag), tag) {
			if field.Tag.Get(tag) == name || field.Name == name {
				return val.Field(i).Interface()
			}
		}
	}
	return nil
}

// BucketSource is the source of a bucket for running a query or updating data
// Buckets and Transactions both implement BucketSource.  This allows for choosing a specific bucket or transaction
// when running a query
type BucketSource interface {
	Bucket(name []byte) *bolt.Bucket
	CreateBucketIfNotExists(name []byte) (*bolt.Bucket, error)
}
