// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold_test

import (
	"fmt"
	"testing"

	"github.com/timshannon/bolthold"
)

var sortTests = []test{
	test{
		name:   "Sort By Name",
		query:  bolthold.Where("Category").Eq("vehicle").SortBy("Name"),
		result: []int{0, 11, 1, 3, 6},
	},
	test{
		name:   "Sort By Name Reversed",
		query:  bolthold.Where("Category").Eq("vehicle").SortBy("Name").Reverse(),
		result: []int{6, 3, 1, 11, 0},
	},
	test{
		name:   "Sort By Multiple Fields",
		query:  bolthold.Where("ID").In(8, 3, 13).SortBy("Category", "Name"),
		result: []int{13, 15, 4, 3},
	},
	test{
		name:   "Sort By Multiple Fields Reversed",
		query:  bolthold.Where("ID").In(8, 3, 13).SortBy("Category", "Name").Reverse(),
		result: []int{3, 4, 15, 13},
	},
	test{
		name:   "Sort By Duplicate Field Names",
		query:  bolthold.Where("ID").In(8, 3, 13).SortBy("Category", "Name", "Category"),
		result: []int{13, 15, 4, 3},
	},
}

func TestSortedFind(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)

		for _, tst := range sortTests {
			t.Run(tst.name, func(t *testing.T) {
				var result []ItemTest
				err := store.Find(&result, tst.query)
				if err != nil {
					t.Fatalf("Error finding sort data from bolthold: %s", err)
				}
				if len(result) != len(tst.result) {
					if testing.Verbose() {
						t.Fatalf("Sorted Find result count is %d wanted %d.  Results: %v", len(result),
							len(tst.result), result)
					}
					t.Fatalf("Sorted Find result count is %d wanted %d.", len(result), len(tst.result))
				}

				for i := range result {
					if !result[i].equal(&testData[tst.result[i]]) {
						t.Fatalf("Expected index %d to be %v, Got %v", i, &testData[tst.result[i]], result[i])
					}
				}
			})
		}
	})
}

func TestSortedUpdateMatching(t *testing.T) {
	for _, tst := range sortTests {
		t.Run(tst.name, func(t *testing.T) {
			testWrap(t, func(store *bolthold.Store, t *testing.T) {

				insertTestData(t, store)

				err := store.UpdateMatching(&ItemTest{}, tst.query, func(record interface{}) error {
					update, ok := record.(*ItemTest)
					if !ok {
						return fmt.Errorf("Record isn't the correct type!  Wanted Itemtest, got %T", record)
					}

					update.UpdateField = "updated"
					update.UpdateIndex = "updated index"

					return nil
				})

				if err != nil {
					t.Fatalf("Error updating data from bolthold: %s", err)
				}

				var result []ItemTest
				err = store.Find(&result, bolthold.Where("UpdateIndex").Eq("updated index").And("UpdateField").Eq("updated"))
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
						if result[i].Key == testData[tst.result[k]].Key &&
							result[i].UpdateField == "updated" &&
							result[i].UpdateIndex == "updated index" {
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

func TestSortedDeleteMatching(t *testing.T) {
	for _, tst := range sortTests {
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
						t.Fatalf("Found %v in the result set when it should've been deleted!", result[i])
					}
				}

			})

		})
	}
}

func TestSortOnKey(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Running Sort on Key field did not panic!")
			}
		}()

		var result []ItemTest
		_ = store.Find(&result, bolthold.Where("Name").Eq("blah").SortBy(bolthold.Key))
	})
}
