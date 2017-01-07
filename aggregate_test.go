// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold_test

import (
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
			}
		}

		//test min / max / count
		for i := range result {
			min := &ItemTest{}
			max := &ItemTest{}

			var group string

			result[i].Group(&group)

			result[i].Min("ID", min)
			result[i].Max("ID", max)
			avg := result[i].Avg("ID")
			sum := result[i].Sum("ID")

			switch group {
			case "animal":
				if !min.equal(&testData[2]) {
					t.Fatalf("Expected animal min value of %v Got %v", testData[2], min)
				}
				if !max.equal(&testData[14]) {
					t.Fatalf("Expected animal max value of %v Got %v", testData[14], max)
				}

				if result[i].Count() != 7 {
					t.Fatalf("Expected animal count of %d got %d", 7, result[i].Count())
				}

				if avg != 6.142857142857143 {
					t.Fatalf("Expected animal AVG of %v got %v", 6.142857142857143, avg)
				}

				if sum != 43 {
					t.Fatalf("Expected animal SUM of %v got %v", 43, sum)
				}

			case "food":
				if !min.equal(&testData[7]) {
					t.Fatalf("Expected food min value of %v Got %v", testData[7], min)
				}
				if !max.equal(&testData[15]) {
					t.Fatalf("Expected food max value of %v Got %v", testData[15], max)
				}

				if result[i].Count() != 5 {
					t.Fatalf("Expected food count of %d got %d", 5, result[i].Count())
				}

				if avg != 9.2 {
					t.Fatalf("Expected food AVG of %v got %v", 9.2, avg)
				}

				if sum != 46 {
					t.Fatalf("Expected food SUM of %v got %v", 46, sum)
				}

			case "vehicle":
				if !min.equal(&testData[0]) {
					t.Fatalf("Expected vehicle min value of %v Got %v", testData[0], min)
				}
				if !max.equal(&testData[11]) {
					t.Fatalf("Expected vehicle max value of %v Got %v", testData[11], max)
				}

				if result[i].Count() != 5 {
					t.Fatalf("Expected vehicle count of %d got %d", 5, result[i].Count())
				}

				if avg != 3.8 {
					t.Fatalf("Expected vehicle AVG of %v got %v", 3.8, avg)
				}

				if sum != 19 {
					t.Fatalf("Expected vehicle SUM of %v got %v", 19, sum)
				}

			}
		}
		// test avg
		// test count

	})
}

//TODO: Test SubAggregate queries
