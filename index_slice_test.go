// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold_test

import (
	"testing"

	"github.com/timshannon/bolthold"
	bolt "go.etcd.io/bbolt"
)

func TestIndexSlice(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		var testData = []ItemTest{
			ItemTest{
				Key:  0,
				Name: "John",
				Tags: []string{"red", "green", "blue"},
			},
			ItemTest{
				Key:  1,
				Name: "Bill",
				Tags: []string{"red", "purple"},
			},
			ItemTest{
				Key:  2,
				Name: "Jane",
				Tags: []string{"red", "orange"},
			},
			ItemTest{
				Key:  3,
				Name: "Brian",
				Tags: []string{"red", "purple"},
			},
		}

		for _, data := range testData {
			err := store.Insert(data.Key, data)
			if err != nil {
				t.Fatalf("Error creating data for tests: %s", err)
			}
		}

		b := store.Bolt()

		b.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("_index:ItemTest:Tags"))
			if bucket == nil {
				t.Fatalf("No index bucket found for Tags index")
			}

			indexCount := 0
			bucket.ForEach(func(k, v []byte) error {
				indexCount++
				return nil
			})

			// each tag chould be indexed individually and there are 5 different tags
			if indexCount != 5 {
				t.Fatalf("Incorrect index count. Expected %d got %d", 5, indexCount)
			}

			return nil
		})

	})
}

func Test85SliceIndex(t *testing.T) {
	type Event struct {
		Id         uint64
		Type       string   `boltholdIndex:"Type"`
		Categories []string `boltholdSliceIndex:"Categories"`
	}

	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		e1 := &Event{Id: 1, Type: "Type1", Categories: []string{"Cat 1", "Cat 2"}}
		e2 := &Event{Id: 2, Type: "Type1", Categories: []string{"Cat 3"}}

		err := store.Insert(e1.Id, e1)
		if err != nil {
			t.Fatalf("Error inserting record: %s", err)
		}
		store.Insert(e2.Id, e2)
		if err != nil {
			t.Fatalf("Error inserting record: %s", err)
		}

		var es []*Event
		err = store.Find(&es, bolthold.Where("Categories").Contains("Cat 1").Index("Categories"))
		if err != nil {
			t.Fatalf("Error querying: %s", err)
		}

		if len(es) != 1 {
			t.Fatalf("Expected 1, got %d", len(es))
		}
	})
}
