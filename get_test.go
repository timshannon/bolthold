// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package gobstore_test

import (
	"testing"
	"time"

	"github.com/timshannon/gobstore"
)

func TestGet(t *testing.T) {
	testWrap(t, func(store *gobstore.Store, t *testing.T) {
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
			t.Fatalf("Error getting data from gobstore: %s", err)
		}

		if !data.equal(result) {
			t.Fatalf("Got %s wanted %s.", result, data)
		}
	})
}
