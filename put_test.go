// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package gobstore_test

import (
	"testing"
	"time"

	"github.com/timshannon/gobstore"
)

func TestInsert(t *testing.T) {
	testWrap(t, func(store *gobstore.Store, t *testing.T) {
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
			t.Fatalf("Error getting data from gobstore: %s", err)
		}

		if !data.equal(result) {
			t.Fatalf("Got %s wanted %s.", result, data)
		}

		// test duplicate insert
		err = store.Insert(key, &ItemTest{
			Name:    "Test Name",
			Created: time.Now(),
		})

		if err != gobstore.ErrKeyExists {
			t.Fatalf("Insert didn't fail! Expected %s got %s", gobstore.ErrKeyExists, err)
		}

	})
}

func TestUpdate(t *testing.T) {
	testWrap(t, func(store *gobstore.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:    "Test Name",
			Created: time.Now(),
		}

		err := store.Update(key, data)
		if err != gobstore.ErrNotFound {
			t.Fatalf("Update without insert didn't fail! Expected %s got %s", gobstore.ErrNotFound, err)
		}

		err = store.Insert(key, data)
		if err != nil {
			t.Fatalf("Error creating data for update test: %s", err)
		}

		result := &ItemTest{}

		err = store.Get(key, result)
		if err != nil {
			t.Fatalf("Error getting data from gobstore: %s", err)
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
			t.Fatalf("Error getting data from gobstore: %s", err)
		}

		if !result.equal(update) {
			t.Fatalf("Update didn't complete.  Expected %s, got %s", update, result)
		}

	})
}

func TestUpsert(t *testing.T) {
	testWrap(t, func(store *gobstore.Store, t *testing.T) {
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
			t.Fatalf("Error getting data from gobstore: %s", err)
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
			t.Fatalf("Error getting data from gobstore: %s", err)
		}

		if !result.equal(update) {
			t.Fatalf("Upsert didn't complete.  Expected %s, got %s", update, result)
		}
	})
}
