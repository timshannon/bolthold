// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/timshannon/bolthold"
)

type ItemTest struct {
	ID       int
	Name     string
	Category string `boltholdIndex:"Category"`
	Created  time.Time
}

func (i *ItemTest) key() string {
	return strconv.Itoa(i.ID) + "_" + i.Name
}

func (i *ItemTest) equal(other *ItemTest) bool {
	if i.ID != other.ID {
		return false
	}

	if i.Name != other.Name {
		return false
	}

	if i.Category != other.Category {
		return false
	}

	if !i.Created.Equal(other.Created) {
		return false
	}

	return true
}

var testData = []ItemTest{
	ItemTest{
		ID:       0,
		Name:     "car",
		Category: "vehicle",
		Created:  time.Now().AddDate(-1, 0, 0),
	},
	ItemTest{
		ID:       1,
		Name:     "truck",
		Category: "vehicle",
		Created:  time.Now().AddDate(0, 30, 0),
	},
	ItemTest{
		ID:       2,
		Name:     "seal",
		Category: "animal",
		Created:  time.Now().AddDate(-1, 0, 0),
	},
	ItemTest{
		ID:       3,
		Name:     "van",
		Category: "vehicle",
		Created:  time.Now().AddDate(0, 30, 0),
	},
	ItemTest{
		ID:       8,
		Name:     "pizza",
		Category: "food",
		Created:  time.Now(),
	},
	ItemTest{
		ID:       1,
		Name:     "crow",
		Category: "animal",
		Created:  time.Now(),
	},
	ItemTest{
		ID:       5,
		Name:     "van",
		Category: "vehicle",
		Created:  time.Now(),
	},
	ItemTest{
		ID:       5,
		Name:     "pizza",
		Category: "food",
		Created:  time.Now(),
	},
	ItemTest{
		ID:       6,
		Name:     "lion",
		Category: "animal",
		Created:  time.Now().AddDate(3, 0, 0),
	},
	ItemTest{
		ID:       7,
		Name:     "bear",
		Category: "animal",
		Created:  time.Now().AddDate(3, 0, 0),
	},
	ItemTest{
		ID:       9,
		Name:     "tacos",
		Category: "food",
		Created:  time.Now().AddDate(-3, 0, 0),
	},
	ItemTest{
		ID:       10,
		Name:     "golf cart",
		Category: "vehicle",
		Created:  time.Now().AddDate(0, 0, 30),
	},
	ItemTest{
		ID:       11,
		Name:     "oatmeal",
		Category: "food",
		Created:  time.Now().AddDate(0, 0, -30),
	},
}

func insertTestData(t *testing.T, store *bolthold.Store) {
	for i := range testData {
		err := store.Insert(testData[i].key(), testData[i])
		if err != nil {
			t.Fatalf("Error insertering test data for find test: %s", err)
		}
	}
}

func TestFindEqualKey(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)

		want := testData[4]

		var result []ItemTest

		err := store.Find(&result, bolthold.Where(bolthold.Key()).Eq(want.key()))

		if err != nil {
			t.Fatalf("Error finding data from bolthold: %s", err)
		}

		if len(result) != 1 {
			t.Fatalf("Find result count is %d wanted %d", len(result), 1)
		}

		if !result[0].equal(&want) {
			t.Fatalf("Got %v wanted %v.", result[0], want)
		}
	})
}

func TestFindEqualFieldNoIndex(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)

		want := testData[1]

		var result []ItemTest

		err := store.Find(&result, bolthold.Where("Name").Eq(want.Name))

		if err != nil {
			t.Fatalf("Error finding data from bolthold: %s", err)
		}

		if len(result) != 1 {
			t.Fatalf("Find result count is %d wanted %d", len(result), 1)
		}

		if !result[0].equal(&want) {
			t.Fatalf("Got %v wanted %v.", result[0], want)
		}

	})
}

func TestFindEqualFieldIndex(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)

		var result []ItemTest

		cat := "vehicle"

		err := store.Find(&result, bolthold.Where("Category").Eq(cat))

		if err != nil {
			t.Fatalf("Error finding data from bolthold: %s", err)
		}

		if len(result) != 5 {
			t.Fatalf("Find result count is %d wanted %d", len(result), 5)
		}

		for i := range result {
			if result[i].Category != cat {
				t.Fatalf("Result %s is not of the category %s", result[i].key(), cat)
			}
		}
	})
}
