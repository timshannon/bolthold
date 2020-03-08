// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold_test

import (
	"testing"

	bh "github.com/timshannon/bolthold"
	bolt "go.etcd.io/bbolt"
)

func TestIndexSlice(t *testing.T) {
	testWrap(t, func(store *bh.Store, t *testing.T) {
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
			ok(t, store.Insert(data.Key, data))
		}

		b := store.Bolt()

		ok(t, b.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("_index:ItemTest:Tags"))
			assert(t, bucket != nil, "No index bucket found for Tags index")

			indexCount := 0
			bucket.ForEach(func(k, v []byte) error {
				indexCount++
				return nil
			})

			// each tag chould be indexed individually and there are 5 different tags
			equals(t, indexCount, 5)
			return nil
		}))

	})
}

func Test85SliceIndex(t *testing.T) {
	type Event struct {
		Id         uint64
		Type       string   `boltholdIndex:"Type"`
		Categories []string `boltholdSliceIndex:"Categories"`
	}

	testWrap(t, func(store *bh.Store, t *testing.T) {
		e1 := &Event{Id: 1, Type: "Type1", Categories: []string{"Cat 1", "Cat 2"}}
		e2 := &Event{Id: 2, Type: "Type1", Categories: []string{"Cat 3"}}

		ok(t, store.Insert(e1.Id, e1))
		ok(t, store.Insert(e2.Id, e2))

		var es []*Event
		ok(t, store.Find(&es, bh.Where("Categories").Contains("Cat 1").Index("Categories")))
		equals(t, len(es), 1)
	})
}

func Test87SliceIndex(t *testing.T) {
	type Event struct {
		Id         uint64
		Type       string   `boltholdIndex:"Type"`
		Categories []string `boltholdSliceIndex:"Categories"`
	}

	testWrap(t, func(store *bh.Store, t *testing.T) {
		e1 := &Event{Id: 1, Type: "Type1", Categories: []string{"Cat 1", "Cat 2"}}
		e2 := &Event{Id: 2, Type: "Type1", Categories: []string{"Cat 3"}}

		ok(t, store.Insert(e1.Id, e1))
		ok(t, store.Insert(e2.Id, e2))
		var es []*Event
		ok(t, store.Find(&es, bh.Where("Categories").ContainsAny("Cat 1").Index("Categories")))
		equals(t, len(es), 1)
	})
}

func TestSliceIndexWithPointers(t *testing.T) {
	type Event struct {
		Id         uint64
		Type       string    `boltholdIndex:"Type"`
		Categories []*string `boltholdSliceIndex:"Categories"`
	}

	testWrap(t, func(store *bh.Store, t *testing.T) {
		cat1 := "Cat 1"
		cat2 := "Cat 2"
		cat3 := "Cat 3"

		e1 := &Event{Id: 1, Type: "Type1", Categories: []*string{&cat1, &cat2}}
		e2 := &Event{Id: 2, Type: "Type1", Categories: []*string{&cat3}}

		ok(t, store.Insert(e1.Id, e1))
		ok(t, store.Insert(e2.Id, e2))

		var es []*Event
		ok(t, store.Find(&es, bh.Where("Categories").ContainsAll("Cat 1").Index("Categories")))
		equals(t, len(es), 1)
	})
}

func Test90AnonIndex(t *testing.T) {
	type (
		Profile struct {
			Username string `boltholdIndex:"Username" json:"username"`
			Password string `json:"password"`
		}

		User struct {
			Profile

			ID   string
			Name string
		}
	)

	testWrap(t, func(store *bh.Store, t *testing.T) {
		ok(t, store.Insert(1, &User{
			Profile: Profile{
				Username: "test",
				Password: "test",
			},
			ID:   "1234",
			Name: "Tester",
		}))

		b := store.Bolt()

		ok(t, b.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(indexName("User", "Username"))
			assert(t, bucket != nil, "No index bucket found")

			indexCount := 0
			bucket.ForEach(func(k, v []byte) error {
				indexCount++
				return nil
			})

			equals(t, indexCount, 1)
			return nil
		}))

	})
}

func Test90AnonIndexPointer(t *testing.T) {
	type (
		Profile struct {
			Username string `boltholdIndex:"Username" json:"username"`
			Password string `json:"password"`
		}

		User struct {
			*Profile

			ID   string
			Name string
		}
	)

	testWrap(t, func(store *bh.Store, t *testing.T) {
		ok(t, store.Insert(1, &User{
			Profile: &Profile{
				Username: "test",
				Password: "test",
			},
			ID:   "1234",
			Name: "Tester",
		}))

		b := store.Bolt()

		ok(t, b.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(indexName("User", "Username"))
			assert(t, bucket != nil, "No index bucket found")

			indexCount := 0
			bucket.ForEach(func(k, v []byte) error {
				indexCount++
				return nil
			})

			equals(t, indexCount, 1)
			return nil
		}))

	})
}

func Test94NilAnonIndexPointer(t *testing.T) {
	type (
		Profile struct {
			Username string `boltholdIndex:"Username" json:"username"`
			Password string `json:"password"`
		}

		User struct {
			*Profile

			ID   string
			Name string
		}
	)

	testWrap(t, func(store *bh.Store, t *testing.T) {
		ok(t, store.Insert(1, &User{
			ID:   "1234",
			Name: "Tester",
		}))

		b := store.Bolt()

		ok(t, b.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(indexName("User", "Username"))
			assert(t, bucket == nil, "Found index where none should've been added")
			return nil
		}))

	})
}
