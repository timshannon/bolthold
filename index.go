// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package gobstore

// GobStoreIndexTag is the struct tag used to define an a field as indexable for a gobstore
const GobStoreIndexTag = "gobstoreIndex"

// Index defines an index on a gobStore
type Index struct {
	Name string
	Func IndexFunc
}

// IndexFunc is a function that returns the indexable bytes of the passed in value
type IndexFunc func(name string, value interface{}) ([]byte, error)

// createIfNotExists checks if an index exists, and if it doesn't, it creates it and populates it
func (i *Index) createIfNotExists(storer Storer) error {

}
