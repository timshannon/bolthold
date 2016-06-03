// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package gobstore

import (
	"bytes"
	"sort"

	"github.com/boltdb/bolt"
)

// GobStoreIndexTag is the struct tag used to define an a field as indexable for a gobstore
const GobStoreIndexTag = "gobstoreIndex"

const indexBucketPrefix = "_index"

// Index is a function that returns the indexable bytes of the passed in value
type Index func(name string, value interface{}) ([]byte, error)

// adds an item to the index
func indexAdd(storer Storer, tx *bolt.Tx, key []byte, data interface{}) error {
	indexes := storer.Indexes()

	for name, index := range indexes {
		indexUpdate(storer.Type(), name, index, tx, key, data, false)
	}

	return nil
}

// removes an item from the index
func indexDelete(storer Storer, tx *bolt.Tx, key []byte, data interface{}) error {
	indexes := storer.Indexes()

	for name, index := range indexes {
		indexUpdate(storer.Type(), name, index, tx, key, data, true)
	}

	return nil
}

// adds or removes a specific index on an item
func indexUpdate(typeName, indexName string, index Index, tx *bolt.Tx, key []byte, value interface{}, delete bool) error {
	indexKey, err := index(indexName, value)
	if indexKey == nil {
		return nil
	}

	indexValue := make(indexKeys, 0)

	if err != nil {
		return err
	}

	// TODO: if it's not performant to do this on every single call, then do it once and store that it's done
	b, err := tx.CreateBucketIfNotExists(indexBucketName(typeName, indexName))
	if err != nil {
		return err
	}

	iVal := b.Get(indexKey)
	if iVal != nil {
		err = decode(iVal, &indexValue)
		if err != nil {
			return err
		}
	}

	if delete {
		indexValue.remove(key)
	} else {
		indexValue.add(key)
	}

	iVal, err = encode(indexValue)
	if err != nil {
		return err
	}

	err = b.Put(indexKey, iVal)
	if err != nil {
		return err
	}

	return nil
}

// indexBucketName returns the name of the bolt bucket where this index is stored
func indexBucketName(typeName, indexName string) []byte {
	return []byte(indexBucketPrefix + ":" + typeName + ":" + indexName)
}

// indexKeys is a slice of unique, sorted keys([]byte) that an index points to
type indexKeys [][]byte

func (v indexKeys) add(key []byte) {
	i := sort.Search(len(v), func(i int) bool {
		return bytes.Compare(v[i], key) >= 0
	})

	if i < len(v) {
		return
	}

	v = append(v, nil)
	copy(v[i+1:], v[i:])
	v[i] = key
}

func (v indexKeys) remove(key []byte) {
	i := sort.Search(len(v), func(i int) bool {
		return bytes.Compare(v[i], key) >= 0
	})

	if i < len(v) {
		copy(v[i:], v[i+1:])
		v[len(v)-1] = nil
		v = v[:len(v)-1]
	}
}
