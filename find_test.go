// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold_test

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/timshannon/bolthold"
	bh "github.com/timshannon/bolthold"
)

type ItemTest struct {
	Key         int
	ID          int
	Name        string
	Category    string `boltholdIndex:"Category"`
	Created     time.Time
	Tags        []string `boltholdSliceIndex:"Tags"`
	Color       string
	Fruit       string
	UpdateField string
	UpdateIndex string `boltholdIndex:"UpdateIndex"`
	MapVal      map[string]string
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
	{
		Key:      0,
		ID:       0,
		Name:     "car",
		Category: "vehicle",
		Created:  time.Now().AddDate(-1, 0, 0),
	},
	{
		Key:      1,
		ID:       1,
		Name:     "truck",
		Category: "vehicle",
		Created:  time.Now().AddDate(0, 30, 0),
	},
	{
		Key:      2,
		Name:     "seal",
		Category: "animal",
		Created:  time.Now().AddDate(-1, 0, 0),
	},
	{
		Key:      3,
		ID:       3,
		Name:     "van",
		Category: "vehicle",
		Created:  time.Now().AddDate(0, 30, 0),
	},
	{
		Key:      4,
		ID:       8,
		Name:     "pizza",
		Category: "food",
		Created:  time.Now(),
		Tags:     []string{"cooked", "takeout"},
	},
	{
		Key:      5,
		ID:       1,
		Name:     "crow",
		Category: "animal",
		Created:  time.Now(),
		Color:    "blue",
		Fruit:    "orange",
	},
	{
		Key:      6,
		ID:       5,
		Name:     "van",
		Category: "vehicle",
		Created:  time.Now(),
		Color:    "orange",
		Fruit:    "orange",
	},
	{
		Key:      7,
		ID:       5,
		Name:     "pizza",
		Category: "food",
		Created:  time.Now(),
		Tags:     []string{"cooked", "takeout"},
	},
	{
		Key:      8,
		ID:       6,
		Name:     "lion",
		Category: "animal",
		Created:  time.Now().AddDate(3, 0, 0),
	},
	{
		Key:      9,
		ID:       7,
		Name:     "bear",
		Category: "animal",
		Created:  time.Now().AddDate(3, 0, 0),
	},
	{
		Key:      10,
		ID:       9,
		Name:     "tacos",
		Category: "food",
		Created:  time.Now().AddDate(-3, 0, 0),
		Tags:     []string{"cooked", "takeout"},
		Color:    "orange",
	},
	{
		Key:      11,
		ID:       10,
		Name:     "golf cart",
		Category: "vehicle",
		Created:  time.Now().AddDate(0, 0, 30),
		Color:    "pink",
		Fruit:    "apple",
	},
	{
		Key:      12,
		ID:       11,
		Name:     "oatmeal",
		Category: "food",
		Created:  time.Now().AddDate(0, 0, -30),
		Tags:     []string{"cooked", "healthy"},
	},
	{
		Key:      13,
		ID:       8,
		Name:     "mouse",
		Category: "animal",
		Created:  time.Now(),
	},
	{
		Key:      14,
		ID:       12,
		Name:     "fish",
		Category: "animal",
		Created:  time.Now().AddDate(0, 0, -1),
	},
	{
		Key:      15,
		ID:       13,
		Name:     "fish",
		Category: "food",
		Created:  time.Now(),
		Tags:     []string{"cooked", "healthy"},
	},
	{
		Key:      16,
		ID:       9,
		Name:     "zebra",
		Category: "animal",
		Created:  time.Now(),
		MapVal: map[string]string{
			"test": "testval",
		},
	},
}

type test struct {
	name   string
	query  *bolthold.Query
	result []int // indices of test data to be found
}

var testResults = []test{
	{
		name:   "Equal Key",
		query:  bolthold.Where(bolthold.Key).Eq(testData[4].Key),
		result: []int{4},
	},
	{
		name:   "Equal Field Without Index",
		query:  bolthold.Where("Name").Eq(testData[1].Name),
		result: []int{1},
	},
	{
		name:   "Equal Field With Index",
		query:  bolthold.Where("Category").Eq("vehicle"),
		result: []int{0, 1, 3, 6, 11},
	},
	{
		name:   "Not Equal Key",
		query:  bolthold.Where(bolthold.Key).Ne(testData[4].Key),
		result: []int{0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	},
	{
		name:   "Not Equal Field Without Index",
		query:  bolthold.Where("Name").Ne(testData[1].Name),
		result: []int{0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	},
	{
		name:   "Not Equal Field With Index",
		query:  bolthold.Where("Category").Ne("vehicle"),
		result: []int{2, 4, 5, 7, 8, 9, 10, 12, 13, 14, 15, 16},
	},
	{
		name:   "Greater Than Key",
		query:  bolthold.Where(bolthold.Key).Gt(testData[10].Key),
		result: []int{11, 12, 13, 14, 15, 16},
	},
	{
		name:   "Greater Than Field Without Index",
		query:  bolthold.Where("ID").Gt(10),
		result: []int{12, 14, 15},
	},
	{
		name:   "Greater Than Field With Index",
		query:  bolthold.Where("Category").Gt("food"),
		result: []int{0, 1, 3, 6, 11},
	},
	{
		name:   "Less Than Key",
		query:  bolthold.Where(bolthold.Key).Lt(testData[0].Key),
		result: []int{},
	},
	{
		name:   "Less Than Field Without Index",
		query:  bolthold.Where("ID").Lt(5),
		result: []int{0, 1, 2, 3, 5},
	},
	{
		name:   "Less Than Field With Index",
		query:  bolthold.Where("Category").Lt("food"),
		result: []int{2, 5, 8, 9, 13, 14, 16},
	},
	{
		name:   "Less Than or Equal To Key",
		query:  bolthold.Where(bolthold.Key).Le(testData[0].Key),
		result: []int{0},
	},
	{
		name:   "Less Than or Equal To Field Without Index",
		query:  bolthold.Where("ID").Le(5),
		result: []int{0, 1, 2, 3, 5, 6, 7},
	},
	{
		name:   "Less Than Field With Index",
		query:  bolthold.Where("Category").Le("food"),
		result: []int{2, 5, 8, 9, 13, 14, 16, 4, 7, 10, 12, 15},
	},
	{
		name:   "Greater Than or Equal To Key",
		query:  bolthold.Where(bolthold.Key).Ge(testData[10].Key),
		result: []int{10, 11, 12, 13, 14, 15, 16},
	},
	{
		name:   "Greater Than or Equal To Field Without Index",
		query:  bolthold.Where("ID").Ge(10),
		result: []int{11, 12, 14, 15},
	},
	{
		name:   "Greater Than or Equal To Field With Index",
		query:  bolthold.Where("Category").Ge("food"),
		result: []int{0, 1, 3, 6, 11, 4, 7, 10, 12, 15},
	},
	{
		name:   "In",
		query:  bolthold.Where("ID").In(5, 8, 3),
		result: []int{3, 6, 7, 4, 13},
	},
	{
		name:   "In on data from other index",
		query:  bolthold.Where("ID").In(5, 8, 3).Index("Category"),
		result: []int{4, 3, 6, 7, 13},
	},
	{
		name:   "In on index",
		query:  bolthold.Where("Category").In("food", "animal").Index("Category"),
		result: []int{4, 2, 5, 7, 8, 9, 10, 12, 13, 14, 15, 16},
	},
	{
		name:   "Regular Expression",
		query:  bolthold.Where("Name").RegExp(regexp.MustCompile("ea")),
		result: []int{2, 9, 12},
	},
	{
		name: "Function Field",
		query: bolthold.Where("Name").MatchFunc(func(ra *bolthold.RecordAccess) (bool, error) {
			field := ra.Field()
			_, ok := field.(string)
			if !ok {
				return false, fmt.Errorf("field not a string, it's a %T", field)
			}

			return strings.HasPrefix(field.(string), "oat"), nil
		}),
		result: []int{12},
	},
	{
		name: "Function Record",
		query: bolthold.Where("ID").MatchFunc(func(ra *bolthold.RecordAccess) (bool, error) {
			record := ra.Record()
			_, ok := record.(*ItemTest)
			if !ok {
				return false, fmt.Errorf("record not an ItemTest, it's a %T", record)
			}

			return strings.HasPrefix(record.(*ItemTest).Name, "oat"), nil
		}),
		result: []int{12},
	},
	{
		name: "Function Subquery",
		query: bolthold.Where("Name").MatchFunc(func(ra *bolthold.RecordAccess) (bool, error) {
			// find where name exists in more than one category
			record, ok := ra.Record().(*ItemTest)
			if !ok {
				return false, fmt.Errorf("Record is not ItemTest, it's a %T", ra.Record())
			}

			var result []ItemTest

			err := ra.SubQuery(&result,
				bolthold.Where("Name").Eq(record.Name).And("Category").Ne(record.Category))
			if err != nil {
				return false, err
			}

			if len(result) > 0 {
				return true, nil
			}

			return false, nil
		}),
		result: []int{14, 15},
	},
	{
		name:   "Time Comparison",
		query:  bolthold.Where("Created").Gt(time.Now()),
		result: []int{1, 3, 8, 9, 11},
	},
	{
		name:   "Chained And Query with non-index lead",
		query:  bolthold.Where("Created").Gt(time.Now()).And("Category").Eq("vehicle"),
		result: []int{1, 3, 11},
	},
	{
		name:   "Multiple Chained And Queries with non-index lead",
		query:  bolthold.Where("Created").Gt(time.Now()).And("Category").Eq("vehicle").And("ID").Ge(10),
		result: []int{11},
	},
	{
		name:   "Chained And Query with leading Index", // also different order same criteria
		query:  bolthold.Where("Category").Eq("vehicle").And("ID").Ge(10).And("Created").Gt(time.Now()),
		result: []int{11},
	},
	{
		name:   "Chained Or Query with leading index",
		query:  bolthold.Where("Category").Eq("vehicle").Or(bolthold.Where("Category").Eq("animal")),
		result: []int{0, 1, 3, 6, 11, 2, 5, 8, 9, 13, 14, 16},
	},
	{
		name:   "Chained Or Query with unioned data",
		query:  bolthold.Where("Category").Eq("animal").Or(bolthold.Where("Name").Eq("fish")),
		result: []int{2, 5, 8, 9, 13, 14, 16, 15},
	},
	{
		name: "Multiple Chained And + Or Query ",
		query: bolthold.Where("Category").Eq("animal").And("Created").Gt(time.Now()).
			Or(bolthold.Where("Name").Eq("fish").And("ID").Ge(13)),
		result: []int{8, 9, 15},
	},
	{
		name:   "Nil Query",
		query:  nil,
		result: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	},
	{
		name:   "Empty Query",
		query:  &bolthold.Query{},
		result: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	},
	{
		name:   "Empty Limit Query",
		query:  (&bolthold.Query{}).Limit(10),
		result: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
	},
	{
		name:   "Empty Skip Query",
		query:  (&bolthold.Query{}).Skip(10),
		result: []int{10, 11, 12, 13, 14, 15, 16},
	},
	{
		name:   "Empty And Query",
		query:  (&bolthold.Query{}).And("ID").Lt(5),
		result: []int{0, 1, 2, 3, 5},
	},
	{
		name:   "Nil Comparison",
		query:  bolthold.Where("Tags").IsNil(),
		result: []int{0, 1, 2, 3, 5, 6, 8, 9, 11, 13, 14, 16},
	},
	{
		name:   "Self-Field comparison",
		query:  bolthold.Where("Color").Eq(bolthold.Field("Fruit")).And("Fruit").Ne(""),
		result: []int{6},
	},
	{
		name:   "Test Key in secondary",
		query:  bolthold.Where("Category").Eq("food").And(bolthold.Key).Eq(testData[4].Key),
		result: []int{4},
	},
	{
		name:   "Skip",
		query:  bolthold.Where(bolthold.Key).Gt(testData[10].Key).Skip(3),
		result: []int{14, 15, 16},
	},
	{
		name:   "Skip Past Len",
		query:  bolthold.Where(bolthold.Key).Gt(testData[10].Key).Skip(9),
		result: []int{},
	},
	{
		name:   "Skip with Or query",
		query:  bolthold.Where("Category").Eq("vehicle").Or(bolthold.Where("Category").Eq("animal")).Skip(4),
		result: []int{11, 2, 5, 8, 9, 13, 14, 16},
	},
	{
		name:   "Skip with Or query, that crosses or boundary",
		query:  bolthold.Where("Category").Eq("vehicle").Or(bolthold.Where("Category").Eq("animal")).Skip(8),
		result: []int{16, 9, 13, 14},
	},
	{
		name:   "Limit",
		query:  bolthold.Where(bolthold.Key).Gt(testData[10].Key).Limit(5),
		result: []int{11, 12, 13, 14, 15},
	},
	{
		name: "Issue #8 - Function Field on index",
		query: bolthold.Where("Category").MatchFunc(func(ra *bolthold.RecordAccess) (bool, error) {
			field := ra.Field()
			_, ok := field.(string)
			if !ok {
				return false, fmt.Errorf("field not a string, it's a %T", field)
			}

			return !strings.HasPrefix(field.(string), "veh"), nil
		}),
		result: []int{2, 4, 5, 7, 8, 9, 10, 12, 13, 14, 15, 16},
	},
	{
		name: "Issue #8 - Function Field on a specific index",
		query: bolthold.Where("Category").MatchFunc(func(ra *bolthold.RecordAccess) (bool, error) {
			field := ra.Field()
			_, ok := field.(string)
			if !ok {
				return false, fmt.Errorf("field not a string, it's a %T", field)
			}

			return !strings.HasPrefix(field.(string), "veh"), nil
		}).Index("Category"),
		result: []int{2, 4, 5, 7, 8, 9, 10, 12, 13, 14, 15, 16},
	},
	{
		name: "Find item with max ID in each category - sub aggregate query",
		query: bolthold.Where("ID").MatchFunc(func(ra *bolthold.RecordAccess) (bool, error) {
			grp, err := ra.SubAggregateQuery(bolthold.Where("Category").
				Eq(ra.Record().(*ItemTest).Category), "Category")
			if err != nil {
				return false, err
			}

			max := &ItemTest{}

			grp[0].Max("ID", max)
			return ra.Field().(int) == max.ID, nil
		}),
		result: []int{11, 14, 15},
	},
	{
		name:   "Indexed in",
		query:  bolthold.Where("Category").In("animal", "vehicle"),
		result: []int{0, 1, 2, 3, 5, 6, 8, 9, 11, 13, 14, 16},
	},
	{
		name:   "Equal Field With Specific Index",
		query:  bolthold.Where("Category").Eq("vehicle").Index("Category"),
		result: []int{0, 1, 3, 6, 11},
	},
	{
		name:   "Key test after lead field",
		query:  bolthold.Where("Category").Eq("food").And(bolthold.Key).Gt(testData[10].Key),
		result: []int{12, 15},
	},
	{
		name:   "Key test after lead index",
		query:  bolthold.Where("Category").Eq("food").Index("Category").And(bolthold.Key).Gt(testData[10].Key),
		result: []int{12, 15},
	},
	{
		name:   "Not In",
		query:  bolthold.Where("Category").Not().In("food", "animal"),
		result: []int{0, 1, 3, 6, 11},
	},
	{
		name:   "Not IsNil",
		query:  bolthold.Where("Tags").Not().IsNil(),
		result: []int{4, 7, 10, 12, 15},
	},
	{
		name:   "Multiple Not criteria",
		query:  bolthold.Where("Tags").Not().IsNil().And("Name").Not().RegExp(regexp.MustCompile("ea")),
		result: []int{4, 7, 10, 15},
	},
	{
		name:   "Not Equal Key with not modifier",
		query:  bolthold.Where(bolthold.Key).Not().Eq(testData[4].Key),
		result: []int{0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	},
	{
		name:   "Double Negative", // don't do this, it's confusing
		query:  bolthold.Where(bolthold.Key).Not().Not().Eq(testData[4].Key),
		result: []int{4},
	},
	{
		name: "Not on Index",
		query: bolthold.Where("Category").Not().Eq("food").And("Category").Not().Eq("animal").
			Index("Category"),
		result: []int{0, 1, 3, 6, 11},
	},
	{
		name: "Function Field without recordAccess",
		query: bolthold.Where("Name").MatchFunc(func(name string) (bool, error) {
			return strings.HasPrefix(name, "oat"), nil
		}),
		result: []int{12},
	},
	{
		name: "Function Record without record access",
		query: bolthold.Where("ID").MatchFunc(func(record *ItemTest) (bool, error) {
			return strings.HasPrefix(record.Name, "oat"), nil
		}),
		result: []int{12},
	},
	{
		name: "Issue #8 - Function Field on a specific index without record access",
		query: bolthold.Where("Category").MatchFunc(func(category string) (bool, error) {
			return !strings.HasPrefix(category, "veh"), nil
		}).Index("Category"),
		result: []int{2, 4, 5, 7, 8, 9, 10, 12, 13, 14, 15, 16},
	},
	{
		name:   "Contains",
		query:  bolthold.Where("Tags").Contains("takeout"),
		result: []int{4, 7, 10},
	},
	{
		name:   "Contains Any",
		query:  bolthold.Where("Tags").ContainsAny("takeout", "healthy"),
		result: []int{4, 7, 10, 12, 15},
	},
	{
		name:   "Contains All",
		query:  bolthold.Where("Tags").ContainsAll("takeout", "healthy"),
		result: []int{},
	},
	{
		name:   "Contains All #2",
		query:  bolthold.Where("Tags").ContainsAll("cooked", "healthy"),
		result: []int{12, 15},
	},
	{
		name:   "bh.Slice",
		query:  bolthold.Where("Tags").ContainsAll(bolthold.Slice([]string{"cooked", "healthy"})...),
		result: []int{12, 15},
	},
	{
		name:   "Not In bh.Slice",
		query:  bolthold.Where("Category").Not().In(bolthold.Slice([]string{"food", "animal"})...),
		result: []int{0, 1, 3, 6, 11},
	},
	{
		name:   "Contains on non-slice",
		query:  bolthold.Where("Category").Contains("cooked"),
		result: []int{},
	},
	{
		name:   "Map Has Key",
		query:  bolthold.Where("MapVal").HasKey("test"),
		result: []int{16},
	},
	{
		name:   "Map Has Key 2",
		query:  bolthold.Where("MapVal").HasKey("other"),
		result: []int{},
	},
}

func insertTestData(t *testing.T, store *bolthold.Store) {
	for i := range testData {
		ok(t, store.Insert(testData[i].Key, testData[i]))
	}
}

func TestFind(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		for _, tst := range testResults {
			t.Run(tst.name, func(t *testing.T) {
				var result []ItemTest
				err := store.Find(&result, tst.query)
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

type BadType struct{}

func TestFindOnUnknownType(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		var result []BadType
		err := store.Find(&result, bolthold.Where("BadName").Eq("blah"))
		if err != nil {
			t.Fatalf("Error finding data from bolthold: %s", err)
		}
		if len(result) != 0 {
			t.Fatalf("Find result count is %d wanted %d.  Results: %v", len(result), 0, result)
		}
	})
}

func TestFindWithNilValue(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)

		var result []ItemTest
		err := store.Find(&result, bolthold.Where("Name").Eq(nil))
		if err == nil {
			t.Fatalf("Comparing with nil did NOT return an error!")
		}

		if _, ok := err.(*bolthold.ErrTypeMismatch); !ok {
			t.Fatalf("Comparing with nil did NOT return the correct error.  Got %v", err)
		}
	})
}

func TestFindWithNonSlicePtr(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Running Find with non-slice pointer did not panic!")
			}
		}()
		var result []ItemTest
		_ = store.Find(result, bolthold.Where("Name").Eq("blah"))
	})
}

func TestQueryWhereNamePanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("Querying with a lower case field did not cause a panic!")
		}
	}()

	_ = bolthold.Where("lower").Eq("test")
}

func TestQueryAndNamePanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("Querying with a lower case field did not cause a panic!")
		}
	}()

	_ = bolthold.Where("Upper").Eq("test").And("lower").Eq("test")
}

func TestFindOnInvalidFieldName(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		var result []ItemTest

		err := store.Find(&result, bolthold.Where("BadFieldName").Eq("test"))
		if err == nil {
			t.Fatalf("Find query against a bad field name didn't return an error!")
		}

	})
}

func TestFindOnInvalidIndex(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		var result []ItemTest

		err := store.Find(&result, bolthold.Where("Name").Eq("test").Index("BadIndex"))
		if err == nil {
			t.Fatalf("Find query against a bad index name didn't return an error!")
		}

	})
}

func TestFindOnEmptyBucketWithIndex(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		// DO NOT INSERT DATA
		var result []ItemTest

		err := store.Find(&result, bolthold.Where("Category").Eq("animal").Index("Category"))
		if err != nil {
			t.Fatalf("Find query against a valid index name but an empty data bucket return an error!")
		}
		if len(result) > 0 {
			t.Fatalf("Find query against an empty bucket returned results!")
		}
	})
}

func TestQueryStringPrint(t *testing.T) {
	q := bolthold.Where("FirstField").Eq("first value").And("SecondField").Gt("Second Value").And("ThirdField").
		Lt("Third Value").And("FourthField").Ge("FourthValue").And("FifthField").Le("FifthValue").And("SixthField").
		Ne("Sixth Value").Or(bolthold.Where("FirstField").In("val1", "val2", "val3").And("SecondField").IsNil().
		And("ThirdField").RegExp(regexp.MustCompile("test")).Index("IndexName").And("FirstField").
		MatchFunc(func(ra *bolthold.RecordAccess) (bool, error) {
			return true, nil
		})).
		And("FirstField").Not().Eq("negative").
		And("FirstField").Contains("value").
		And("SecondField").ContainsAny("val1", "val2", "val3").
		And("ThirdField").ContainsAll("val1", "val2", "val3")

	contains := []string{
		"FirstField == first value",
		"SecondField > Second Value",
		"ThirdField < Third Value",
		"FourthField >= FourthValue",
		"FifthField <= FifthValue",
		"SixthField != Sixth Value",
		"FirstField in [val1 val2 val3]",
		"FirstField matches the function",
		"SecondField is nil",
		"ThirdField matches the regular expression test",
		"Using Index [IndexName]",
		"FirstField NOT == negative",
		"FirstField contains value",
		"SecondField contains any of [val1 val2 val3]",
		"ThirdField contains all of [val1 val2 val3]",
	}

	// map order isn't guaranteed, check if all needed lines exist

	tst := q.String()

	tstLines := strings.Split(tst, "\n")

	for i := range contains {
		found := false
		for k := range tstLines {
			if strings.Contains(tstLines[k], contains[i]) {
				found = true
				break
			}
		}

		if !found {
			t.Fatalf("Line %s was not found in the result \n%s", contains[i], tst)
		}

	}

}

func TestSkip(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		var result []ItemTest

		q := bolthold.Where("Category").Eq("animal").Or(bolthold.Where("Name").Eq("fish"))

		err := store.Find(&result, q)

		if err != nil {
			t.Fatalf("Error retrieving data for skip test.")
		}

		var skipResult []ItemTest
		skip := 5

		err = store.Find(&skipResult, q.Skip(skip))
		if err != nil {
			t.Fatalf("Error retrieving data for skip test on the skip query.")
		}

		if len(skipResult) != len(result)-skip {
			t.Fatalf("Skip query didn't return the right number of records: Wanted %d got %d",
				(len(result) - skip), len(skipResult))
		}

		// confirm that the first records are skipped

		result = result[skip:]

		for i := range skipResult {
			found := false
			for k := range result {
				if result[i].equal(&skipResult[k]) {
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

func TestSkipNegative(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Running Find with negative skip did not panic!")
			}
		}()

		var result []ItemTest
		_ = store.Find(&result, bolthold.Where("Name").Eq("blah").Skip(-30))
	})
}

func TestLimitNegative(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Running Find with negative limit did not panic!")
			}
		}()

		var result []ItemTest
		_ = store.Find(&result, bolthold.Where("Name").Eq("blah").Limit(-30))
	})
}

func TestSkipDouble(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Running Find with double skips did not panic!")
			}
		}()

		var result []ItemTest
		_ = store.Find(&result, bolthold.Where("Name").Eq("blah").Skip(30).Skip(3))
	})
}

func TestLimitDouble(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Running Find with double limits did not panic!")
			}
		}()

		var result []ItemTest
		_ = store.Find(&result, bolthold.Where("Name").Eq("blah").Limit(30).Limit(3))
	})
}

func TestSkipInOr(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Running Find with skip in or query did not panic!")
			}
		}()

		var result []ItemTest
		_ = store.Find(&result, bolthold.Where("Name").Eq("blah").Or(bolthold.Where("Name").Eq("blah").Skip(3)))
	})
}

func TestLimitInOr(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Running Find with limit in or query did not panic!")
			}
		}()

		var result []ItemTest
		_ = store.Find(&result, bolthold.Where("Name").Eq("blah").Or(bolthold.Where("Name").Eq("blah").Limit(3)))
	})
}

func TestSlicePointerResult(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		count := 10
		for i := 0; i < count; i++ {
			err := store.Insert(i, &ItemTest{
				Key: i,
				ID:  i,
			})
			if err != nil {
				t.Fatalf("Error inserting data for Slice Pointer test: %s", err)
			}
		}

		var result []*ItemTest
		err := store.Find(&result, nil)

		if err != nil {
			t.Fatalf("Error retrieving data for Slice pointer test: %s", err)
		}

		if len(result) != count {
			t.Fatalf("Expected %d, got %d", count, len(result))
		}
	})
}

func TestKeyMatchFunc(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Running matchFunc against Key query did not panic!")
			}
		}()

		var result []ItemTest
		_ = store.Find(&result, bolthold.Where(bolthold.Key).MatchFunc(func(ra *bolthold.RecordAccess) (bool, error) {
			field := ra.Field()
			_, ok := field.(string)
			if !ok {
				return false, fmt.Errorf("field not a string, it's a %T", field)
			}

			return strings.HasPrefix(field.(string), "oat"), nil
		}))
	})
}

func TestKeyStructTag(t *testing.T) {
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

		var result []KeyTest

		err = store.Find(&result, bolthold.Where(bolthold.Key).Eq(key))
		if err != nil {
			t.Fatalf("Error running Find in TestKeyStructTag. ERROR: %s", err)
		}

		if result[0].Key != key {
			t.Fatalf("Key struct tag was not set correctly.  Expected %d, got %d", key, result[0].Key)
		}

		oneResult := KeyTest{}
		err = store.FindOne(&oneResult, bolthold.Where(bolthold.Key).Eq(key))
		if err != nil {
			t.Fatalf("Error running FindOne in TestKeyStructTag. ERROR: %s", err)
		}

		if oneResult.Key != key {
			t.Fatalf("Key struct tag was not set correctly.  Expected %d, got %d", key, result[0].Key)
		}
	})
}

func TestKeyStructTagIntoPtr(t *testing.T) {
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

		var result []KeyTest

		err = store.Find(&result, bolthold.Where(bolthold.Key).Eq(key))
		if err != nil {
			t.Fatalf("Error running Find in TestKeyStructTag. ERROR: %s", err)
		}

		if *result[0].Key != key {
			t.Fatalf("Key struct tag was not set correctly.  Expected %d, got %d", key, result[0].Key)
		}

		oneResult := KeyTest{}

		err = store.FindOne(&oneResult, bolthold.Where(bolthold.Key).Eq(key))
		if err != nil {
			t.Fatalf("Error running FindOne in TestKeyStructTag. ERROR: %s", err)
		}

		if *oneResult.Key != key {
			t.Fatalf("Key struct tag was not set correctly.  Expected %d, got %d", key, result[0].Key)
		}
	})
}

func TestNestedStructPointer(t *testing.T) {

	type notification struct {
		Enabled bool
	}

	type device struct {
		ID            string `boltholdKey:"ID"`
		Notifications *notification
	}

	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		id := "1"
		store.Insert(id, &device{
			ID: id,
			Notifications: &notification{
				Enabled: true,
			},
		})

		devices := []*device{}
		err := store.Find(&devices, nil)
		if err != nil {
			t.Fatalf("Error finding data for nested struct testing: %s", err)
		}

		device := &device{}
		err = store.Get(id, device)
		if err != nil {
			t.Fatalf("Error getting data for nested struct testing: %s", err)
		}

		if devices[0].ID != id {
			t.Fatalf("ID Expected %s, got %s", id, devices[0].ID)
		}

		if !devices[0].Notifications.Enabled {
			t.Fatalf("Notifications.Enabled Expected  %t, got %t", true, devices[0].Notifications.Enabled)
		}

		if device.ID != id {
			t.Fatalf("ID Expected %s, got %s", id, device.ID)
		}

		if !device.Notifications.Enabled {
			t.Fatalf("Notifications.Enabled Expected  %t, got %t", true, device.Notifications.Enabled)
		}
	})
}

func TestErrorsFromMatchFunc(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		var result []ItemTest
		err := store.Find(&result, bolthold.Where("Name").MatchFunc(func(name string) (bool, error) {
			return false, fmt.Errorf("New Error")
		}))

		if err == nil {
			t.Fatalf("Error did not get returned properly from MatchFunc")
		}
	})
}

func TestCount(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		for _, tst := range testResults {
			t.Run(tst.name, func(t *testing.T) {
				count, err := store.Count(ItemTest{}, tst.query)
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

func TestFindOne(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		for _, tst := range testResults {
			t.Run(tst.name, func(t *testing.T) {
				result := &ItemTest{}
				err := store.FindOne(result, tst.query)
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

func TestFindOneWithNonPtr(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Running FindOne with non pointer did not panic!")
			}
		}()
		result := ItemTest{}
		_ = store.FindOne(result, bolthold.Where("Name").Eq("blah"))
	})
}

func Test67SeekCursor(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		type Test struct {
			TestString string
		}

		store.Upsert(-6, Test{TestString: "Test"})
		store.Upsert(-7, Test{TestString: "Test"})
		store.Upsert(-5, Test{TestString: "Test"})
		store.Upsert(-3, Test{TestString: "Test2"})
		store.Upsert(-1, Test{TestString: "Test3"})

		var result []Test
		ok(t, store.Find(&result, bolthold.Where(bolthold.Key).Gt(-4)))
		equals(t, 2, len(result))

		result = nil
		ok(t, store.Find(&result, bolthold.Where(bolthold.Key).Gt(-3)))
		equals(t, 1, len(result))

		result = nil
		ok(t, store.Find(&result, bolthold.Where(bolthold.Key).Gt(-2)))
		equals(t, 1, len(result))

		result = nil
		ok(t, store.Find(&result, bolthold.Where(bolthold.Key).Gt(-1)))
		equals(t, 0, len(result))

		result = nil
		ok(t, store.Find(&result, bolthold.Where(bolthold.Key).Gt(0)))
		equals(t, 0, len(result))
	})
}

func TestIssue137SliceIndexWithPointers(t *testing.T) {

	type Event struct {
		Id         uint64
		Type       string   `boltholdIndex:"Type"`
		Categories []string `boltholdSliceIndex:"Categories"`
	}

	testWrap(t, func(store *bh.Store, t *testing.T) {
		cat1 := "Cat 1"
		cat2 := "Cat 2"
		cat3 := "Cat 3"

		e1 := &Event{Id: 1, Type: "Type1", Categories: []string{cat1, cat2}}
		e2 := &Event{Id: 2, Type: "Type1", Categories: []string{cat3}}

		ok(t, store.Insert(e1.Id, e1))
		ok(t, store.Insert(e2.Id, e2))

		var es []*Event
		query := bh.Where("Categories").ContainsAll(cat1, cat2).Index("Categories")
		ok(t, store.Find(&es, query))
		equals(t, 1, len(es))
	})
}
