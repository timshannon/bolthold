// Copyright 2019 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold_test

import (
	"fmt"
	"testing"

	"github.com/timshannon/bolthold"
	bolt "go.etcd.io/bbolt"
)

func TestForEach(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		for _, tst := range testResults {
			t.Run(tst.name, func(t *testing.T) {
				count := 0
				err := store.ForEach(tst.query, func(record *ItemTest) error {
					count++

					found := false
					for i := range tst.result {
						if record.equal(&testData[tst.result[i]]) {
							found = true
							break
						}
					}

					if !found {
						if testing.Verbose() {
							return fmt.Errorf("%v was not found in the result set! Full results: %v",
								record, tst.result)
						}
						return fmt.Errorf("%v was not found in the result set!", record)
					}

					return nil
				})
				if count != len(tst.result) {
					t.Fatalf("ForEach count is %d wanted %d.", count, len(tst.result))
				}
				if err != nil {
					t.Fatalf("Error during ForEach iteration: %s", err)
				}
			})
		}
	})
}

func TestForEachInBucket(t *testing.T) {
	testWrapWithBucket(t, func(store *bolthold.Store, bucket *bolt.Bucket, t *testing.T) {
		insertBucketTestData(t, store, bucket)
		for _, tst := range testResults {
			t.Run(tst.name, func(t *testing.T) {
				count := 0
				err := store.ForEachInBucket(bucket, tst.query, func(record *ItemTest) error {
					count++

					found := false
					for i := range tst.result {
						if record.equal(&testData[tst.result[i]]) {
							found = true
							break
						}
					}

					if !found {
						if testing.Verbose() {
							return fmt.Errorf("%v was not found in the result set! Full results: %v",
								record, tst.result)
						}
						return fmt.Errorf("%v was not found in the result set!", record)
					}

					return nil
				})
				if count != len(tst.result) {
					t.Fatalf("ForEach count is %d wanted %d.", count, len(tst.result))
				}
				if err != nil {
					t.Fatalf("Error during ForEach iteration: %s", err)
				}
			})
		}
	})
}
