// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package gobstore

import (
	"errors"
	"reflect"

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

// Find retrieves a set of values from the gobstore that matches the passed in query
// result must be a pointer to a slice
func (s *Store) Find(result interface{}, query *Query) error {
	return s.Bolt().View(func(tx *bolt.Tx) error {
		return s.TxFind(tx, result, query)
	})
}

// TxFind allows you to pass in your own bolt transaction to retrieve a set of values from the gobstore
func (s *Store) TxFind(tx *bolt.Tx, result interface{}, query *Query) error {
	return s.runQuery(tx, result, query, nil)
}

func (s *Store) runQuery(tx *bolt.Tx, result interface{}, query *Query, retrievedKeys keyList) error {
	slicePtr := reflect.ValueOf(result)
	if slicePtr.Kind() != reflect.Ptr || slicePtr.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}

	sliceVal := slicePtr.Elem()
	elType := sliceVal.Type().Elem()
	sliceVal = sliceVal.Slice(0, 0) // empty slice

	iter, err := newIterator(tx, newStorer(reflect.New(elType).Interface()).Type(), query.index, query.fieldCriteria[query.index])
	if err != nil {
		return err
	}

	newKeys := make(keyList, 0)

	for k, v := iter.First(); k != nil; k, v = iter.Next() {
		if len(retrievedKeys) == 0 {
			// don't check this record if it's already been retrieved
			if retrievedKeys.in(k) {
				continue
			}
		}

		val := reflect.New(elType)

		err = decode(v, val.Interface())
		if err != nil {
			return err
		}

		ok, err := query.matchesAllFields(val)
		if err != nil {
			return err
		}

		if ok {
			// add to result
			sliceVal = reflect.Append(sliceVal, val)
			// track that this key's entry has been added to the result list
			newKeys.add(k)
		}
	}

	if len(query.ors) > 0 {
		for i := range newKeys {
			retrievedKeys.add(newKeys[i])
		}

		for i := range query.ors {
			err = s.runQuery(tx, result, query.ors[i], retrievedKeys)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
