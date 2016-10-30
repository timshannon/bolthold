// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold_test

import (
	"testing"
	"time"

	"github.com/timshannon/bolthold"
)

func TestDelete(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:    "Test Name",
			Created: time.Now(),
		}

		err := store.Insert(key, data)
		if err != nil {
			t.Fatalf("Error inserting data for delete test: %s", err)
		}

		result := &ItemTest{}

		err = store.Delete(key, result)
		if err != nil {
			t.Fatalf("Error deleting data from bolthold: %s", err)
		}

		err = store.Get(key, result)
		if err != bolthold.ErrNotFound {
			t.Fatalf("Data was not deleted from bolthold")
		}

	})
}

func TestDeleteMatching(t *testing.T) {
	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			testWrap(t, func(store *bolthold.Store, t *testing.T) {

				insertTestData(t, store)

				err := store.DeleteMatching(&ItemTest{}, tst.query)
				if err != nil {
					t.Fatalf("Error deleting data from bolthold: %s", err)
				}

				var result []ItemTest
				err = store.Find(&result, nil)
				if err != nil {
					t.Fatalf("Error finding result after delete from bolthold: %s", err)
				}

				if len(result) != (len(testData) - len(tst.result)) {
					if testing.Verbose() {
						t.Fatalf("Delete result count is %d wanted %d.  Results: %v", len(result),
							(len(testData) - len(tst.result)), result)
					}
					t.Fatalf("Delete result count is %d wanted %d.", len(result),
						(len(testData) - len(tst.result)))

				}

				for i := range result {
					found := false
					for k := range tst.result {
						if result[i].equal(&testData[tst.result[k]]) {
							found = true
							break
						}
					}

					if found {
						if testing.Verbose() {
							t.Fatalf("Found %v in the result set when it should've been deleted! Full results: %v", result[i], result)
						}
						t.Fatalf("Found %v in the result set when it should've been deleted! Full results: %v", result[i])
					}
				}

			})

		})
	}
}

func TestDeleteOnUnknownType(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		var x BadType
		err := store.DeleteMatching(x, bolthold.Where("BadName").Eq("blah"))
		if err != nil {
			t.Fatalf("Error finding data from bolthold: %s", err)
		}

		var result []ItemTest
		err = store.Find(&result, nil)
		if err != nil {
			t.Fatalf("Error finding result after delete from bolthold: %s", err)
		}

		if len(result) != len(testData) {
			t.Fatalf("Find result count after delete is %d wanted %d.", len(result), len(testData))
		}
	})
}

func TestDeleteWithNilValue(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)

		var result ItemTest
		err := store.DeleteMatching(&result, bolthold.Where("Name").Eq(nil))
		if err == nil {
			t.Fatalf("Comparing with nil did NOT return an error!")
		}

		if _, ok := err.(*bolthold.ErrTypeMismatch); !ok {
			t.Fatalf("Comparing with nil did NOT return the correct error.  Got %v", err)
		}
	})
}
