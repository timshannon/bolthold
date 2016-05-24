// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package gobstore

import (
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
	s.db.Close()
}
