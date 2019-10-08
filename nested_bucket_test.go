// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/timshannon/bolthold"
	bolt "go.etcd.io/bbolt"
)

func testWrapWithBucket(t *testing.T, tests func(store *bolthold.Store, bucket *bolt.Bucket, t *testing.T)) {
	filename := tempfile()
	store, err := bolthold.Open(filename, 0666, nil)
	if err != nil {
		t.Fatalf("Error opening %s: %s", filename, err)
	}

	if store == nil {
		t.Fatalf("store is null!")
	}

	defer store.Close()
	defer os.Remove(filename)

	var bucket *bolt.Bucket
	err = store.Bolt().Update(func(tx *bolt.Tx) error {
		bucket, err = tx.CreateBucketIfNotExists([]byte("test bucket parent"))
		if err != nil {
			return err
		}
		tests(store, bucket, t)
		return nil
	})

	if err != nil {
		t.Fatalf("Error creating bucket %s: %s", filename, err)
	}
}

func TestGetFromBucket(t *testing.T) {
	testWrapWithBucket(t, func(store *bolthold.Store, bucket *bolt.Bucket, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:    "Test Name",
			Created: time.Now(),
		}
		err := store.InsertIntoBucket(bucket, key, data)
		if err != nil {
			t.Fatalf("Error creating data for get test: %s", err)
		}

		result := &ItemTest{}

		err = store.Get(key, result)
		if err != bolthold.ErrNotFound {
			t.Fatalf("Expected to not find record")
		}

		err = store.GetFromBucket(bucket, key, result)
		if err != nil {
			t.Fatalf("Error getting data from bolthold bucket: %s", err)
		}

		if !data.equal(result) {
			t.Fatalf("Got %v wanted %v.", result, data)
		}
	})
}

func insertBucketTestData(t *testing.T, store *bolthold.Store, bucket *bolt.Bucket) {
	for i := range testData {
		err := store.InsertIntoBucket(bucket, testData[i].Key, testData[i])
		if err != nil {
			t.Fatalf("Error inserting test data for find test: %s", err)
		}
	}
}

func TestFindInBucket(t *testing.T) {
	testWrapWithBucket(t, func(store *bolthold.Store, bucket *bolt.Bucket, t *testing.T) {
		insertBucketTestData(t, store, bucket)
		for _, tst := range testResults {
			t.Run(tst.name, func(t *testing.T) {
				var result []ItemTest
				err := store.FindInBucket(bucket, &result, tst.query)
				if err != nil {
					t.Fatalf("Error finding data from bolthold: %s", err)
				}
				if len(result) != len(tst.result) {
					if testing.Verbose() {
						t.Fatalf("Find result count is %d wanted %d.  Results: %v", len(result),
							len(tst.result), result)
					}
					t.Fatalf("Find result count is %d wanted %d.", len(result), len(tst.result))
				}

				for i := range result {
					found := false
					for k := range tst.result {
						if result[i].equal(&testData[tst.result[k]]) {
							found = true
							break
						}
					}

					if !found {
						if testing.Verbose() {
							t.Fatalf("%v should not be in the result set! Full results: %v",
								result[i], result)
						}
						t.Fatalf("%v should not be in the result set!", result[i])
					}
				}
			})
		}
	})
}

func TestBucketCount(t *testing.T) {
	testWrapWithBucket(t, func(store *bolthold.Store, bucket *bolt.Bucket, t *testing.T) {
		insertBucketTestData(t, store, bucket)
		for _, tst := range testResults {
			t.Run(tst.name, func(t *testing.T) {
				count, err := store.CountInBucket(bucket, ItemTest{}, tst.query)
				if err != nil {
					t.Fatalf("Error counting data from bolthold: %s", err)
				}

				if count != len(tst.result) {
					t.Fatalf("Count result is %d wanted %d.", count, len(tst.result))
				}
			})
		}
	})
}

func TestFindOneInBucket(t *testing.T) {
	testWrapWithBucket(t, func(store *bolthold.Store, bucket *bolt.Bucket, t *testing.T) {
		insertBucketTestData(t, store, bucket)
		for _, tst := range testResults {
			t.Run(tst.name, func(t *testing.T) {
				result := &ItemTest{}
				err := store.FindOneInBucket(bucket, result, tst.query)
				if len(tst.result) == 0 && err == bolthold.ErrNotFound {
					return
				}

				if err != nil {
					t.Fatalf("Error finding one data from bolthold: %s", err)
				}

				if !result.equal(&testData[tst.result[0]]) {
					t.Fatalf("Result doesnt match the first record in the testing result set. "+
						"Expected key of %d got %d", &testData[tst.result[0]].Key, result.Key)
				}
			})
		}
	})
}

func TestInsertBucket(t *testing.T) {
	testWrapWithBucket(t, func(store *bolthold.Store, bucket *bolt.Bucket, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:     "Test Name",
			Category: "Test Category",
			Created:  time.Now(),
		}

		err := store.InsertIntoBucket(bucket, key, data)
		if err != nil {
			t.Fatalf("Error inserting data for test: %s", err)
		}

		result := &ItemTest{}

		err = store.GetFromBucket(bucket, key, result)
		if err != nil {
			t.Fatalf("Error getting data from bolthold: %s", err)
		}

		if !data.equal(result) {
			t.Fatalf("Got %v wanted %v.", result, data)
		}

		// test duplicate insert
		err = store.InsertIntoBucket(bucket, key, &ItemTest{
			Name:    "Test Name",
			Created: time.Now(),
		})

		if err != bolthold.ErrKeyExists {
			t.Fatalf("Insert didn't fail! Expected %s got %s", bolthold.ErrKeyExists, err)
		}
	})
}

func TestUpdateBucket(t *testing.T) {
	testWrapWithBucket(t, func(store *bolthold.Store, bucket *bolt.Bucket, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:     "Test Name",
			Category: "Test Category",
			Created:  time.Now(),
		}

		err := store.UpdateBucket(bucket, key, data)
		if err != bolthold.ErrNotFound {
			t.Fatalf("Update without insert didn't fail! Expected %s got %s", bolthold.ErrNotFound, err)
		}

		err = store.InsertIntoBucket(bucket, key, data)
		if err != nil {
			t.Fatalf("Error creating data for update test: %s", err)
		}

		result := &ItemTest{}

		err = store.GetFromBucket(bucket, key, result)
		if err != nil {
			t.Fatalf("Error getting data from bolthold: %s", err)
		}

		if !data.equal(result) {
			t.Fatalf("Got %v wanted %v.", result, data)
		}

		update := &ItemTest{
			Name:     "Test Name Updated",
			Category: "Test Category Updated",
			Created:  time.Now(),
		}

		// test duplicate insert
		err = store.UpdateBucket(bucket, key, update)

		if err != nil {
			t.Fatalf("Error updating data: %s", err)
		}

		err = store.GetFromBucket(bucket, key, result)
		if err != nil {
			t.Fatalf("Error getting data from bolthold: %s", err)
		}

		if !result.equal(update) {
			t.Fatalf("Update didn't complete.  Expected %v, got %v", update, result)
		}
	})
}

func TestUpsertBucket(t *testing.T) {
	testWrapWithBucket(t, func(store *bolthold.Store, bucket *bolt.Bucket, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:     "Test Name",
			Category: "Test Category",
			Created:  time.Now(),
		}

		err := store.UpsertBucket(bucket, key, data)
		if err != nil {
			t.Fatalf("Error upserting data: %s", err)
		}

		result := &ItemTest{}

		err = store.GetFromBucket(bucket, key, result)
		if err != nil {
			t.Fatalf("Error getting data from bolthold: %s", err)
		}

		if !data.equal(result) {
			t.Fatalf("Got %v wanted %v.", result, data)
		}

		update := &ItemTest{
			Name:     "Test Name Updated",
			Category: "Test Category Updated",
			Created:  time.Now(),
		}

		// test duplicate insert
		err = store.UpsertBucket(bucket, key, update)

		if err != nil {
			t.Fatalf("Error updating data: %s", err)
		}

		err = store.GetFromBucket(bucket, key, result)
		if err != nil {
			t.Fatalf("Error getting data from bolthold: %s", err)
		}

		if !result.equal(update) {
			t.Fatalf("Upsert didn't complete.  Expected %v, got %v", update, result)
		}
	})
}

func TestUpdateMatchingBucket(t *testing.T) {
	for _, tst := range testResults {
		t.Run(tst.name, func(t *testing.T) {
			testWrapWithBucket(t, func(store *bolthold.Store, bucket *bolt.Bucket, t *testing.T) {
				insertBucketTestData(t, store, bucket)

				err := store.UpdateMatchingInBucket(bucket, &ItemTest{}, tst.query,
					func(record interface{}) error {
						update, ok := record.(*ItemTest)
						if !ok {
							return fmt.Errorf("Record isn't the correct type!  "+
								"Wanted Itemtest, got %T", record)
						}

						update.UpdateField = "updated"
						update.UpdateIndex = "updated index"

						return nil
					})

				if err != nil {
					t.Fatalf("Error updating data from bolthold: %s", err)
				}

				var result []ItemTest
				err = store.FindInBucket(bucket, &result, bolthold.Where("UpdateIndex").
					Eq("updated index").And("UpdateField").Eq("updated"))
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
							t.Fatalf("Could not find %v in the update result set! "+
								"Full results: %v", result[i], result)
						}
						t.Fatalf("Could not find %v in the updated result set!", result[i])
					}
				}

			})

		})
	}
}

func TestDeleteBucket(t *testing.T) {
	testWrapWithBucket(t, func(store *bolthold.Store, bucket *bolt.Bucket, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:    "Test Name",
			Created: time.Now(),
		}

		err := store.InsertIntoBucket(bucket, key, data)
		if err != nil {
			t.Fatalf("Error inserting data for delete test: %s", err)
		}

		result := &ItemTest{}

		err = store.DeleteFromBucket(bucket, key, result)
		if err != nil {
			t.Fatalf("Error deleting data from bolthold: %s", err)
		}

		err = store.GetFromBucket(bucket, key, result)
		if err != bolthold.ErrNotFound {
			t.Fatalf("Data was not deleted from bolthold")
		}

	})
}

func TestDeleteMatchingBucket(t *testing.T) {
	for _, tst := range testResults {
		t.Run(tst.name, func(t *testing.T) {
			testWrapWithBucket(t, func(store *bolthold.Store, bucket *bolt.Bucket, t *testing.T) {

				insertBucketTestData(t, store, bucket)

				err := store.DeleteMatchingFromBucket(bucket, &ItemTest{}, tst.query)
				if err != nil {
					t.Fatalf("Error deleting data from bolthold: %s", err)
				}

				var result []ItemTest
				err = store.FindInBucket(bucket, &result, nil)
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
							t.Fatalf("Found %v in the result set when it should've "+
								"been deleted! Full results: %v", result[i], result)
						}
						t.Fatalf("Found %v in the result set when it should've been deleted!",
							result[i])
					}
				}
			})
		})
	}
}
