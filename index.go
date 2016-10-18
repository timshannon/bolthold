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

// size of iterator keys stored in memory before more are fetched
const iteratorKeyMinCacheSize = 100

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

type iterator struct {
	keyCache    [][]byte
	dataBucket  *bolt.Bucket
	indexCursor *bolt.Cursor
	nextKeys    func(bool, *bolt.Cursor) ([][]byte, error)
	prepCursor  bool
	err         error
}

// TODO: Use cursor.Seek() by looking at query criteria to skip uncessary reads and seek to the earliest potential record

func newIterator(tx *bolt.Tx, typeName string, query *Query) *iterator {
	criteria := query.fieldCriteria[query.index]

	iter := &iterator{
		dataBucket: tx.Bucket([]byte(typeName)),
		prepCursor: true,
	}

	// 3 scenarios
	//   Key field
	if query.index == Key() {
		iter.indexCursor = tx.Bucket([]byte(typeName)).Cursor()

		iter.nextKeys = func(prepCursor bool, cursor *bolt.Cursor) ([][]byte, error) {
			var nKeys [][]byte

			for len(nKeys) < iteratorKeyMinCacheSize {
				var k []byte
				if prepCursor {
					k, _ = cursor.First()
					prepCursor = false
				} else {
					k, _ = cursor.Next()
				}
				if k == nil {
					return nKeys, nil
				}

				ok, err := matchesAllCriteria(criteria, k)
				if err != nil {
					return nil, err
				}

				if ok {
					nKeys = append(nKeys, k)
				}

			}
			return nKeys, nil

		}

		return iter

	}

	iBucket := tx.Bucket(indexBucketName(typeName, query.index))
	if iBucket == nil {
		// bad index, filter through entire store
		query.badIndex = true

		iter.indexCursor = tx.Bucket([]byte(typeName)).Cursor()

		iter.nextKeys = func(prepCursor bool, cursor *bolt.Cursor) ([][]byte, error) {
			var nKeys [][]byte

			for len(nKeys) < iteratorKeyMinCacheSize {
				var k []byte
				if prepCursor {
					k, _ = cursor.First()
					prepCursor = false
				} else {
					k, _ = cursor.Next()
				}
				if k == nil {
					return nKeys, nil
				}

				nKeys = append(nKeys, k)
			}
			return nKeys, nil
		}

		return iter
	}

	//   indexed field
	iter.indexCursor = iBucket.Cursor()

	iter.nextKeys = func(prepCursor bool, cursor *bolt.Cursor) ([][]byte, error) {
		var nKeys [][]byte

		for len(nKeys) < iteratorKeyMinCacheSize {
			var k, v []byte
			if prepCursor {
				k, v = cursor.First()
				prepCursor = false
			} else {
				k, v = cursor.Next()
			}
			if k == nil {
				return nKeys, nil
			}
			ok, err := matchesAllCriteria(criteria, k)
			if err != nil {
				return nil, err
			}

			if ok {
				// append the slice of keys stored in the index
				var keys = new(keyList)
				err := decode(v, keys)
				if err != nil {
					return nil, err
				}

				nKeys = append(nKeys, [][]byte(*keys)...)
			}

		}
		return nKeys, nil

	}

	return iter

}

// Next returns the next key value that matches the iterators criteria
// If no more kv's are available the return nil, if there is an error, they return nil
// and iterator.Error() will return the error
func (i *iterator) Next() (key []byte, value []byte) {
	if i.err != nil {
		return nil, nil
	}

	if len(i.keyCache) == 0 {
		newKeys, err := i.nextKeys(i.prepCursor, i.indexCursor)
		i.prepCursor = false
		if err != nil {
			i.err = err
			return nil, nil
		}

		if len(newKeys) == 0 {
			return nil, nil
		}

		i.keyCache = append(i.keyCache, newKeys...)
	}

	nextKey := i.keyCache[0]
	i.keyCache = i.keyCache[1:]

	val := i.dataBucket.Get(nextKey)

	return nextKey, val
}

// Error returns the last error, iterator.Next() will not continue if there is an error present
func (i *iterator) Error() error {
	return i.err
}
