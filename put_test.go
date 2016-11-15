// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/timshannon/bolthold"
)

func TestInsert(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:    "Test Name",
			Created: time.Now(),
		}

		err := store.Insert(key, data)
		if err != nil {
			t.Fatalf("Error inserting data for test: %s", err)
		}

		result := &ItemTest{}

		err = store.Get(key, result)
		if err != nil {
			t.Fatalf("Error getting data from bolthold: %s", err)
		}

		if !data.equal(result) {
			t.Fatalf("Got %s wanted %s.", result, data)
		}

		// test duplicate insert
		err = store.Insert(key, &ItemTest{
			Name:    "Test Name",
			Created: time.Now(),
		})

		if err != bolthold.ErrKeyExists {
			t.Fatalf("Insert didn't fail! Expected %s got %s", bolthold.ErrKeyExists, err)
		}

	})
}

func TestInsertReadTxn(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:    "Test Name",
			Created: time.Now(),
		}

		err := store.Bolt().View(func(tx *bolt.Tx) error {
			return store.TxInsert(tx, key, data)
		})

		if err == nil {
			t.Fatalf("Inserting into a read only transaction didn't fail!")
		}

	})
}

func TestUpdate(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:    "Test Name",
			Created: time.Now(),
		}

		err := store.Update(key, data)
		if err != bolthold.ErrNotFound {
			t.Fatalf("Update without insert didn't fail! Expected %s got %s", bolthold.ErrNotFound, err)
		}

		err = store.Insert(key, data)
		if err != nil {
			t.Fatalf("Error creating data for update test: %s", err)
		}

		result := &ItemTest{}

		err = store.Get(key, result)
		if err != nil {
			t.Fatalf("Error getting data from bolthold: %s", err)
		}

		if !data.equal(result) {
			t.Fatalf("Got %s wanted %s.", result, data)
		}

		update := &ItemTest{
			Name:    "Test Name",
			Created: time.Now(),
		}

		// test duplicate insert
		err = store.Update(key, update)

		if err != nil {
			t.Fatalf("Error updating data: %s", err)
		}

		err = store.Get(key, result)
		if err != nil {
			t.Fatalf("Error getting data from bolthold: %s", err)
		}

		if !result.equal(update) {
			t.Fatalf("Update didn't complete.  Expected %s, got %s", update, result)
		}

	})
}

func TestUpdateReadTxn(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:    "Test Name",
			Created: time.Now(),
		}

		err := store.Bolt().View(func(tx *bolt.Tx) error {
			return store.TxUpdate(tx, key, data)
		})

		if err == nil {
			t.Fatalf("Updating into a read only transaction didn't fail!")
		}

	})
}

func TestUpsert(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:    "Test Name",
			Created: time.Now(),
		}

		err := store.Upsert(key, data)
		if err != nil {
			t.Fatalf("Error upserting data: %s", err)
		}

		result := &ItemTest{}

		err = store.Get(key, result)
		if err != nil {
			t.Fatalf("Error getting data from bolthold: %s", err)
		}

		if !data.equal(result) {
			t.Fatalf("Got %s wanted %s.", result, data)
		}

		update := &ItemTest{
			Name:    "Test Name",
			Created: time.Now(),
		}

		// test duplicate insert
		err = store.Upsert(key, update)

		if err != nil {
			t.Fatalf("Error updating data: %s", err)
		}

		err = store.Get(key, result)
		if err != nil {
			t.Fatalf("Error getting data from bolthold: %s", err)
		}

		if !result.equal(update) {
			t.Fatalf("Upsert didn't complete.  Expected %s, got %s", update, result)
		}
	})
}

func TestUpsertReadTxn(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:    "Test Name",
			Created: time.Now(),
		}

		err := store.Bolt().View(func(tx *bolt.Tx) error {
			return store.TxUpsert(tx, key, data)
		})

		if err == nil {
			t.Fatalf("Updating into a read only transaction didn't fail!")
		}

	})
}

func TestUpdateMatching(t *testing.T) {
	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			testWrap(t, func(store *bolthold.Store, t *testing.T) {

				insertTestData(t, store)

				err := store.UpdateMatching(&ItemTest{}, tst.query, func(record interface{}) error {
					update, ok := record.(*ItemTest)
					if !ok {
						return fmt.Errorf("Record isn't the correct type!  Wanted Itemtest, got %T", record)
					}

					update.UpdateField = "updated"

					return nil
				})

				if err != nil {
					t.Fatalf("Error updating data from bolthold: %s", err)
				}

				var result []ItemTest
				err = store.Find(&result, bolthold.Where("UpdateField").Eq("updated"))
				if err != nil {
					t.Fatalf("Error finding result after update from bolthold: %s", err)
				}

				if len(result) != len(tst.result) {
					if testing.Verbose() {
						t.Fatalf("Find result count after update is %d wanted %d.  Results: %v",
							len(result), len(tst.result), result)
					}
					t.Fatalf("Find result count after update is %d wanted %d.", len(result),
						len(tst.result))
				}

				for i := range result {
					found := false
					for k := range tst.result {
						if result[i].key() == testData[tst.result[k]].key() &&
							result[i].UpdateField == "updated" {
							found = true
							break
						}
					}

					if !found {
						if testing.Verbose() {
							t.Fatalf("Could not find %v in the update result set! Full results: %v",
								result[i], result)
						}
						t.Fatalf("Could not find %v in the updated result set!", result[i])
					}
				}

			})

		})
	}
}
