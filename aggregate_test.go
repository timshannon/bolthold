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
			default:
				t.Fatalf(fmt.Sprintf("Unaccounted for grouping: %s", group))
			}
		}

	})
}

func TestFindAggregateMultipleGrouping(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)

		result, err := store.FindAggregate(&ItemTest{}, nil, "Category", "Color")

		if err != nil {
			t.Fatalf("Error finding aggregate data from bolthold: %s", err)
		}

		if len(result) != 7 {
			t.Fatalf("Wrong number of groupings.  Wanted %d got %d", 7, len(result))
		}

		for i := range result {
			var items []*ItemTest
			var category string
			var color string

			result[i].Reduction(&items)
			result[i].Group(&category, &color)

			for j := range items {
				if items[j].Category != category || items[j].Color != color {
					t.Fatalf("Reduction item is not in the proper grouping.  Wanted %s - %s, Got %s - %s",
						category, color, items[j].Category, items[j].Color)
				}
			}
		}

		//test min / max / count
		for i := range result {
			min := &ItemTest{}
			max := &ItemTest{}

			var category string
			var color string

			result[i].Group(&category, &color)

			result[i].Min("ID", min)
			result[i].Max("ID", max)
			avg := result[i].Avg("ID")
			sum := result[i].Sum("ID")

			group := category + "-" + color

			switch group {
			case "animal-":
				if !min.equal(&testData[2]) {
					t.Fatalf("Expected %s min value of %v Got %v", group, testData[2], min)
				}
				if !max.equal(&testData[14]) {
					t.Fatalf("Expected %s max value of %v Got %v", group, testData[14], max)
				}

				if result[i].Count() != 6 {
					t.Fatalf("Expected %s count of %d got %d", group, 6, result[i].Count())
				}

				if avg != 7 {
					t.Fatalf("Expected %s AVG of %v got %v", group, 7, avg)
				}

				if sum != 42 {
					t.Fatalf("Expected %s SUM of %v got %v", group, 42, sum)
				}
			case "animal-blue":
				if !min.equal(&testData[5]) {
					t.Fatalf("Expected %s min value of %v Got %v", group, testData[5], min)
				}
				if !max.equal(&testData[5]) {
					t.Fatalf("Expected %s max value of %v Got %v", group, testData[5], max)
				}

				if result[i].Count() != 1 {
					t.Fatalf("Expected %s count of %d got %d", group, 1, result[i].Count())
				}

				if avg != 1 {
					t.Fatalf("Expected %s AVG of %v got %v", group, 1, avg)
				}

				if sum != 1 {
					t.Fatalf("Expected %s SUM of %v got %v", group, 1, sum)
				}
			case "food-":
				if !min.equal(&testData[7]) {
					t.Fatalf("Expected %s min value of %v Got %v", group, testData[7], min)
				}
				if !max.equal(&testData[15]) {
					t.Fatalf("Expected %s max value of %v Got %v", group, testData[15], max)
				}

				if result[i].Count() != 4 {
					t.Fatalf("Expected %s count of %d got %d", group, 4, result[i].Count())
				}

				if avg != 9.25 {
					t.Fatalf("Expected %s AVG of %v got %v", group, 9.25, avg)
				}

				if sum != 37 {
					t.Fatalf("Expected %s SUM of %v got %v", group, 37, sum)
				}
			case "food-orange":
				if !min.equal(&testData[10]) {
					t.Fatalf("Expected %s min value of %v Got %v", group, testData[10], min)
				}
				if !max.equal(&testData[10]) {
					t.Fatalf("Expected %s max value of %v Got %v", group, testData[10], max)
				}

				if result[i].Count() != 1 {
					t.Fatalf("Expected %s count of %d got %d", group, 1, result[i].Count())
				}

				if avg != 9 {
					t.Fatalf("Expected %s AVG of %v got %v", group, 9, avg)
				}

				if sum != 9 {
					t.Fatalf("Expected %s SUM of %v got %v", group, 9, sum)
				}
			case "vehicle-":
				if !min.equal(&testData[0]) {
					t.Fatalf("Expected %s min value of %v Got %v", group, testData[0], min)
				}
				if !max.equal(&testData[3]) {
					t.Fatalf("Expected %s max value of %v Got %v", group, testData[3], max)
				}

				if result[i].Count() != 3 {
					t.Fatalf("Expected %s count of %d got %d", group, 3, result[i].Count())
				}

				if avg != 1.3333333333333333 {
					t.Fatalf("Expected %s AVG of %v got %v", group, 1.3333333333333333, avg)
				}

				if sum != 4 {
					t.Fatalf("Expected %s SUM of %v got %v", group, 4, sum)
				}
			case "vehicle-orange":
				if !min.equal(&testData[6]) {
					t.Fatalf("Expected %s min value of %v Got %v", group, testData[6], min)
				}
				if !max.equal(&testData[6]) {
					t.Fatalf("Expected %s max value of %v Got %v", group, testData[6], max)
				}

				if result[i].Count() != 1 {
					t.Fatalf("Expected %s count of %d got %d", group, 1, result[i].Count())
				}

				if avg != 5 {
					t.Fatalf("Expected %s AVG of %v got %v", group, 5, avg)
				}

				if sum != 5 {
					t.Fatalf("Expected %s SUM of %v got %v", group, 5, sum)
				}
			case "vehicle-pink":
				if !min.equal(&testData[11]) {
					t.Fatalf("Expected %s min value of %v Got %v", group, testData[11], min)
				}
				if !max.equal(&testData[11]) {
					t.Fatalf("Expected %s max value of %v Got %v", group, testData[11], max)
				}

				if result[i].Count() != 1 {
					t.Fatalf("Expected %s count of %d got %d", group, 1, result[i].Count())
				}

				if avg != 10 {
					t.Fatalf("Expected %s AVG of %v got %v", group, 10, avg)
				}

				if sum != 10 {
					t.Fatalf("Expected %s SUM of %v got %v", group, 10, sum)
				}
			default:
				t.Fatalf(fmt.Sprintf("Unaccounted for grouping: %s", group))

			}
		}
	})
}

func TestFindAggregateGroupPointerPanic(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Running group without a pointer did not panic!")
			}
		}()

		result, _ := store.FindAggregate(&ItemTest{}, nil, "Category")

		group := ""

		result[0].Group(group)
	})
}

func TestFindAggregateGroupLenPanic(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Running group with wrong number of groupings did not panic!")
			}
		}()

		result, _ := store.FindAggregate(&ItemTest{}, nil, "Category")

		group := ""
		group2 := ""

		result[0].Group(&group, &group2)
	})
}

func TestFindAggregateReductionPointerPanic(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Running Reduction without a pointer did not panic!")
			}
		}()

		result, _ := store.FindAggregate(&ItemTest{}, nil, "Category")

		var items []ItemTest

		result[0].Reduction(items)
	})
}

func TestFindAggregateSortInvalidFieldPanic(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Running Sort on a non-existent field did not panic!")
			}
		}()

		result, _ := store.FindAggregate(&ItemTest{}, nil, "Category")

		result[0].Sort("BadField")
	})
}

func TestFindAggregateSortLowerFieldPanic(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Running Sort on a lower case field did not panic!")
			}
		}()

		result, _ := store.FindAggregate(&ItemTest{}, nil, "Category")

		result[0].Sort("category")
	})
}

func TestFindAggregateMaxPointerPanic(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Running Max without a pointer did not panic!")
			}
		}()

		result, _ := store.FindAggregate(&ItemTest{}, nil, "Category")

		var items ItemTest

		result[0].Max("Category", items)
	})
}

func TestFindAggregateMaxPointerNilPanic(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Running Max on a nil pointer did not panic!")
			}
		}()

		result, _ := store.FindAggregate(&ItemTest{}, nil, "Category")

		var items *ItemTest

		result[0].Max("Category", items)
	})
}

func TestFindAggregateMinPointerPanic(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Running Min without a pointer did not panic!")
			}
		}()

		result, _ := store.FindAggregate(&ItemTest{}, nil, "Category")

		var items ItemTest

		result[0].Min("Category", items)
	})
}

func TestFindAggregateMinPointerNilPanic(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Running Min on a nil pointer did not panic!")
			}
		}()

		result, _ := store.FindAggregate(&ItemTest{}, nil, "Category")

		var items *ItemTest

		result[0].Min("Category", items)
	})
}

func TestFindAggregateBadSumFieldPanic(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Running Sum on a bad field did not panic!")
			}
		}()

		result, _ := store.FindAggregate(&ItemTest{}, nil, "Category")

		result[0].Sum("BadField")
	})
}
