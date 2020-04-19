// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold_test

import (
	"testing"
	"time"

	"github.com/timshannon/bolthold"
)

func TestGet(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:    "Test Name",
			Created: time.Now(),
		}
		err := store.Insert(key, data)
		if err != nil {
			t.Fatalf("Error creating data for get test: %s", err)
		}

		result := &ItemTest{}

		err = store.Get(key, result)
		if err != nil {
			t.Fatalf("Error getting data from bolthold: %s", err)
		}

		if !data.equal(result) {
			t.Fatalf("Got %v wanted %v.", result, data)
		}
	})
}

func TestGetKeyStructTag(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		type KeyTest struct {
			Key   int `boltholdKey:"Key"`
			Value string
		}

		key := 3

		err := store.Insert(key, &KeyTest{
			Value: "test value",
		})

		if err != nil {
			t.Fatalf("Error inserting KeyTest struct for Key struct tag testing. Error: %s", err)
		}

		var result KeyTest
		err = store.Get(key, &result)

		if err != nil {
			t.Fatalf("Error running Get in TestKeyStructTag. ERROR: %s", err)
		}

		if result.Key != key {
			t.Fatalf("Key struct tag was not set correctly.  Expected %d, got %d", key, result.Key)
		}
	})
}

func TestGetKeyStructTagIntoPtr(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		type KeyTest struct {
			Key   *int `boltholdKey:"Key"`
			Value string
		}

		key := 3

		err := store.Insert(&key, &KeyTest{
			Value: "test value",
		})

		if err != nil {
			t.Fatalf("Error inserting KeyTest struct for Key struct tag testing. Error: %s", err)
		}

		var result KeyTest

		err = store.Get(key, &result)
		if err != nil {
			t.Fatalf("Error running Get in TestKeyStructTag. ERROR: %s", err)
		}

		if result.Key == nil || *result.Key != key {
			t.Fatalf("Key struct tag was not set correctly.  Expected %d, got %d", key, result.Key)
		}
	})
}

func TestIssue103(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		type Counterer struct {
			State uint
		}

		count := new(Counterer)

		count.State++
		ok(t, store.Upsert("count", count))

		ok(t, store.Get("count", &count))

	})
}
