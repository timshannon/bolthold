// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold_test

import (
	"fmt"
	"testing"

	"github.com/timshannon/bolthold"
)

func TestFindAggregateGroup(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)

		result, err := store.FindAggregate(&ItemTest{}, nil, "Category")

		if err != nil {
			t.Fatalf("Error finding aggregate data from bolthold: %s", err)
		}

		if len(result) != 3 {
			t.Fatalf("Wrong number of groupings.  Wanted %d got %d", 3, len(result))
		}

		for i := range result {
			var items []ItemTest
			var group string

			result[i].Reduction(&items)
			result[i].Group(&group)

			for j := range items {
				if items[j].Category != group {
					t.Fatalf("Reduction item is not in the proper grouping.  Wanted %s, Got %s",
						group, items[j].Category)
				}
				fmt.Println(items[i])
			}
		}

		//test min
		for i := range result {
			min := &ItemTest{}
			max := &ItemTest{}
			var group string

			result[i].Group(&group)

			result[i].Min("ID", min)
			result[i].Max("ID", max)

			fmt.Printf("Max %s: %d\n", group, max.Key)
			fmt.Printf("Min %s: %d\n", group, min.Key)

			switch group {
			case "animal":
				if !min.equal(&testData[5]) {
					t.Fatalf("Expected min value of %v Got %v", testData[5], min)
				}
			case "food":
				if !min.equal(&testData[7]) {
					t.Fatalf("Expected min value of %v Got %v", testData[7], min)
				}

			case "vehicle":
				if !min.equal(&testData[1]) {
					t.Fatalf("Expected min value of %v Got %v", testData[1], min)
				}
			}
		}
		// test max
		// test avg
		// test count

	})
}
