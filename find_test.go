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
	ItemTest{ //0
		ID:       0,
		Name:     "car",
		Category: "vehicle",
		Created:  time.Now().AddDate(-1, 0, 0),
	},
	ItemTest{ //1
		ID:       1,
		Name:     "truck",
		Category: "vehicle",
		Created:  time.Now().AddDate(0, 30, 0),
	},
	ItemTest{ //2
		ID:       2,
		Name:     "seal",
		Category: "animal",
		Created:  time.Now().AddDate(-1, 0, 0),
	},
	ItemTest{ //3
		ID:       3,
		Name:     "van",
		Category: "vehicle",
		Created:  time.Now().AddDate(0, 30, 0),
	},
	ItemTest{ //4
		ID:       8,
		Name:     "pizza",
		Category: "food",
		Created:  time.Now(),
	},
	ItemTest{ //5
		ID:       1,
		Name:     "crow",
		Category: "animal",
		Created:  time.Now(),
	},
	ItemTest{ //6
		ID:       5,
		Name:     "van",
		Category: "vehicle",
		Created:  time.Now(),
	},
	ItemTest{ //7
		ID:       5,
		Name:     "pizza",
		Category: "food",
		Created:  time.Now(),
	},
	ItemTest{ //8
		ID:       6,
		Name:     "lion",
		Category: "animal",
		Created:  time.Now().AddDate(3, 0, 0),
	},
	ItemTest{ //9
		ID:       7,
		Name:     "bear",
		Category: "animal",
		Created:  time.Now().AddDate(3, 0, 0),
	},
	ItemTest{ //10
		ID:       9,
		Name:     "tacos",
		Category: "food",
		Created:  time.Now().AddDate(-3, 0, 0),
	},
	ItemTest{ //11
		ID:       10,
		Name:     "golf cart",
		Category: "vehicle",
		Created:  time.Now().AddDate(0, 0, 30),
	},
	ItemTest{ //12
		ID:       11,
		Name:     "oatmeal",
		Category: "food",
		Created:  time.Now().AddDate(0, 0, -30),
	},
	ItemTest{ //13
		ID:       8,
		Name:     "mouse",
		Category: "animal",
		Created:  time.Now(),
	},
	ItemTest{ //14
		ID:       12,
		Name:     "fish",
		Category: "animal",
		Created:  time.Now(),
	},
	ItemTest{ //15
		ID:       13,
		Name:     "fish",
		Category: "food",
		Created:  time.Now(),
	},
	ItemTest{ //16
		ID:       9,
		Name:     "zebra",
		Category: "animal",
		Created:  time.Now(),
	},
}

type test struct {
	name   string
	query  *bolthold.Query
	result []int // indices of test data to be found
}

var tests = []test{
	test{
		name:   "Equal Key",
		query:  bolthold.Where(bolthold.Key()).Eq(testData[4].key()),
		result: []int{4},
	},
	test{
		name:   "Equal Field Without Index",
		query:  bolthold.Where("Name").Eq(testData[1].Name),
		result: []int{1},
	},
	test{
		name:   "Equal Field With Index",
		query:  bolthold.Where("Category").Eq("vehicle"),
		result: []int{0, 1, 3, 6, 11},
	},
	test{
		name:   "Greater Than Key",
		query:  bolthold.Where(bolthold.Key()).Gt(testData[10].key()),
		result: []int{16},
	},
	test{
		name:   "Greater Than Field Without Index",
		query:  bolthold.Where("ID").Gt(10),
		result: []int{12, 14, 15},
	},
	test{
		name:   "Greater Than Field With Index",
		query:  bolthold.Where("Category").Gt("food"),
		result: []int{0, 1, 3, 6, 11},
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

func TestFind(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)

		for _, tst := range tests {
			t.Run(tst.name, func(t *testing.T) {
				var result []ItemTest

				err := store.Find(&result, tst.query)
				if err != nil {
					t.Fatalf("Error finding data from bolthold: %s", err)
				}
				if len(result) != len(tst.result) {
					t.Fatalf("Find result count is %d wanted %d.  Results: %v", len(result), len(tst.result), result)
				}

				for i := range result {
					found := false
					for k := range tst.result {
						if result[i].equal(&testData[tst.result[k]]) {
							found = true
						}
					}

					if !found {
						t.Fatalf("Could not find %v in the result set! Full results: %v", result[i], result)
					}
				}
			})
		}
	})
}
