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

const keyIndex = ""

// Index is a function that returns the indexable bytes of the passed in value
type Index func(name string, value interface{}) ([]byte, error)

// adds an item to the index
func indexAdd(storer Storer, tx *bolt.Tx, key []byte, data interface{}) error {
	indexes := storer.Indexes()

	for name, index := range indexes {
		err := indexUpdate(storer.Type(), name, index, tx, key, data, false)
		if err != nil {
			return err
		}
	}

	return nil
}

// removes an item from the index
func indexDelete(storer Storer, tx *bolt.Tx, key []byte, data interface{}) error {
	indexes := storer.Indexes()

	for name, index := range indexes {
		err := indexUpdate(storer.Type(), name, index, tx, key, data, true)
		if err != nil {
			return err
		}
	}

	return nil
}

// adds or removes a specific index on an item
func indexUpdate(typeName, indexName string, index Index, tx *bolt.Tx, key []byte, value interface{}, delete bool) error {
	indexKey, err := index(indexName, value)
	if indexKey == nil {
		return nil
	}

	indexValue := make(keyList, 0)

	if err != nil {
		return err
	}

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

// IndexExists tests if an index exists for the passed in field name
func (s *Store) IndexExists(tx *bolt.Tx, typeName, indexName string) bool {
	return (tx.Bucket(indexBucketName(typeName, indexName)) != nil)
}

// indexBucketName returns the name of the bolt bucket where this index is stored
func indexBucketName(typeName, indexName string) []byte {
	return []byte(indexBucketPrefix + ":" + typeName + ":" + indexName)
}

// keyList is a slice of unique, sorted keys([]byte) such as what an index points to
type keyList [][]byte

func (v keyList) add(key []byte) {
	i := sort.Search(len(v), func(i int) bool {
		return bytes.Compare(v[i], key) >= 0
	})

	if i < len(v) {
		// already added
		return
	}

	v = append(v, nil)
	copy(v[i+1:], v[i:])
	v[i] = key
}

func (v keyList) remove(key []byte) {
	i := sort.Search(len(v), func(i int) bool {
		return bytes.Compare(v[i], key) >= 0
	})

	if i < len(v) {
		copy(v[i:], v[i+1:])
		v[len(v)-1] = nil
		v = v[:len(v)-1]
	}
}

func (v keyList) in(key []byte) bool {
	i := sort.Search(len(v), func(i int) bool {
		return bytes.Compare(v[i], key) >= 0
	})

	return (i < len(v))
}

type indexIter struct {
	currentIndex int
	keys         [][]byte
	bucket       *bolt.Bucket
}

func newIterator(tx *bolt.Tx, typeName, indexName string, criteria []*Criterion) (iterator, error) {
	iter := &indexIter{
		currentIndex: -1,
		bucket:       tx.Bucket([]byte(typeName)),
	}

	//FIXME: Iterator should continue until it finds a record that matches the criteria
	// this is all crap

	var iBucket *bolt.Bucket

	if indexName == keyIndex {
		iBucket = tx.Bucket([]byte(typeName))
	} else {
		iBucket = tx.Bucket(indexBucketName(typeName, indexName))
		if iBucket == nil {
			return tx.Bucket([]byte(typeName)).Cursor(), nil

		}
	}

	c := iBucket.Cursor()

	for k, v := c.First(); k != nil; k, v = c.Next() {
		include, err := matchesAllCriteria(criteria, k)
		if err != nil {
			return nil, err
		}

		if include {
			if indexName == keyIndex {
				iter.keys = append(iter.keys, k)
			} else {
				// append the slice of keys stored in the index
				var keys = new(keyList)
				err := decode(v, keys)
				if err != nil {
					return nil, err
				}
				iter.keys = append(iter.keys, [][]byte(*keys)...)
			}
		}
	}

	return iter, nil
}

func (i *indexIter) get() (key []byte, value []byte) {
	if len(i.keys) == 0 {
		return nil, nil
	}

	key = i.keys[i.currentIndex]
	value = i.bucket.Get(key)
	return

}
func (i *indexIter) First() (key []byte, value []byte) {
	i.currentIndex = 0
	return i.get()
}

func (i *indexIter) Last() (key []byte, value []byte) {
	i.currentIndex = (len(i.keys) - 1)
	return i.get()
}

func (i *indexIter) Next() (key []byte, value []byte) {
	if i.currentIndex == (len(i.keys) - 1) {
		return nil, nil
	}

	i.currentIndex++
	return i.get()
}

func (i *indexIter) Prev() (key []byte, value []byte) {
	if i.currentIndex <= 0 {
		return nil, nil
	}

	i.currentIndex--
	return i.get()
}
