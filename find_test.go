// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold_test

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/timshannon/bolthold"
)

type ItemTest struct {
	ID          int
	Name        string
	Category    string `boltholdIndex:"Category"`
	Created     time.Time
	Tags        []string
	Color       string
	Fruit       string
	UpdateField string
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
		Tags:     []string{"cooked"},
	},
	ItemTest{ //5
		ID:       1,
		Name:     "crow",
		Category: "animal",
		Created:  time.Now(),
		Color:    "blue",
		Fruit:    "orange",
	},
	ItemTest{ //6
		ID:       5,
		Name:     "van",
		Category: "vehicle",
		Created:  time.Now(),
		Color:    "orange",
		Fruit:    "orange",
	},
	ItemTest{ //7
		ID:       5,
		Name:     "pizza",
		Category: "food",
		Created:  time.Now(),
		Tags:     []string{"cooked"},
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
		Tags:     []string{"cooked"},
		Color:    "orange",
	},
	ItemTest{ //11
		ID:       10,
		Name:     "golf cart",
		Category: "vehicle",
		Created:  time.Now().AddDate(0, 0, 30),
		Color:    "pink",
		Fruit:    "apple",
	},
	ItemTest{ //12
		ID:       11,
		Name:     "oatmeal",
		Category: "food",
		Created:  time.Now().AddDate(0, 0, -30),
		Tags:     []string{"cooked"},
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
		Created:  time.Now().AddDate(0, 0, -1),
	},
	ItemTest{ //15
		ID:       13,
		Name:     "fish",
		Category: "food",
		Created:  time.Now(),
		Tags:     []string{"cooked"},
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
		name:   "Not Equal Key",
		query:  bolthold.Where(bolthold.Key()).Ne(testData[4].key()),
		result: []int{0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	},
	test{
		name:   "Not Equal Field Without Index",
		query:  bolthold.Where("Name").Ne(testData[1].Name),
		result: []int{0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	},
	test{
		name:   "Not Equal Field With Index",
		query:  bolthold.Where("Category").Ne("vehicle"),
		result: []int{2, 4, 5, 7, 8, 9, 10, 12, 13, 14, 15, 16},
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
	test{
		name:   "Less Than Key",
		query:  bolthold.Where(bolthold.Key()).Lt(testData[0].key()),
		result: []int{},
	},
	test{
		name:   "Less Than Field Without Index",
		query:  bolthold.Where("ID").Lt(5),
		result: []int{0, 1, 2, 3, 5},
	},
	test{
		name:   "Less Than Field With Index",
		query:  bolthold.Where("Category").Lt("food"),
		result: []int{2, 5, 8, 9, 13, 14, 16},
	},
	test{
		name:   "Less Than or Equal To Key",
		query:  bolthold.Where(bolthold.Key()).Le(testData[0].key()),
		result: []int{0},
	},
	test{
		name:   "Less Than or Equal To Field Without Index",
		query:  bolthold.Where("ID").Le(5),
		result: []int{0, 1, 2, 3, 5, 6, 7},
	},
	test{
		name:   "Less Than Field With Index",
		query:  bolthold.Where("Category").Le("food"),
		result: []int{2, 5, 8, 9, 13, 14, 16, 4, 7, 10, 12, 15},
	},
	test{
		name:   "Greater Than or Equal To Key",
		query:  bolthold.Where(bolthold.Key()).Ge(testData[10].key()),
		result: []int{16, 10},
	},
	test{
		name:   "Greater Than or Equal To Field Without Index",
		query:  bolthold.Where("ID").Ge(10),
		result: []int{12, 14, 15, 11},
	},
	test{
		name:   "Greater Than or Equal To Field With Index",
		query:  bolthold.Where("Category").Ge("food"),
		result: []int{0, 1, 3, 6, 11, 4, 7, 10, 12, 15},
	},
	test{
		name:   "In",
		query:  bolthold.Where("ID").In(5, 8, 3),
		result: []int{6, 7, 4, 13, 3},
	},
	test{
		name:   "Regular Expression",
		query:  bolthold.Where("Name").RegExp(regexp.MustCompile("ea")),
		result: []int{2, 9, 12},
	},
	test{
		name: "Function",
		query: bolthold.Where("Name").MatchFunc(func(field interface{}) (bool, error) {
			_, ok := field.(string)
			if !ok {
				return false, errors.New("Field not a string!")
			}

			return strings.HasPrefix(field.(string), "oat"), nil
		}),
		result: []int{12},
	},
	test{
		name:   "Time Comparison",
		query:  bolthold.Where("Created").Gt(time.Now()),
		result: []int{1, 3, 8, 9, 11},
	},
	test{
		name:   "Chained And Query with non-index lead",
		query:  bolthold.Where("Created").Gt(time.Now()).And("Category").Eq("vehicle"),
		result: []int{1, 3, 11},
	},
	test{
		name:   "Multiple Chained And Queries with non-index lead",
		query:  bolthold.Where("Created").Gt(time.Now()).And("Category").Eq("vehicle").And("ID").Ge(10),
		result: []int{11},
	},
	test{
		name:   "Chained And Query with leading Index", // also different order same criteria
		query:  bolthold.Where("Category").Eq("vehicle").And("ID").Ge(10).And("Created").Gt(time.Now()),
		result: []int{11},
	},
	test{
		name:   "Chained Or Query with leading index",
		query:  bolthold.Where("Category").Eq("vehicle").Or(bolthold.Where("Category").Eq("animal")),
		result: []int{0, 1, 3, 6, 11, 2, 5, 8, 9, 13, 14, 16},
	},
	test{
		name:   "Chained Or Query with unioned data",
		query:  bolthold.Where("Category").Eq("animal").Or(bolthold.Where("Name").Eq("fish")),
		result: []int{2, 5, 8, 9, 13, 14, 16, 15},
	},
	test{
		name:   "Multiple Chained And + Or Query ",
		query:  bolthold.Where("Category").Eq("animal").And("Created").Gt(time.Now()).Or(bolthold.Where("Name").Eq("fish").And("ID").Ge(13)),
		result: []int{8, 9, 15},
	},
	test{
		name:   "Nil Query",
		query:  nil,
		result: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	},
	test{
		name:   "Nil Comparison",
		query:  bolthold.Where("Tags").IsNil(),
		result: []int{0, 1, 2, 3, 5, 6, 8, 9, 11, 13, 14, 16},
	},
	test{
		name:   "Self-Field comparison",
		query:  bolthold.Where("Color").Eq(bolthold.Field("Fruit")).And("Fruit").Ne(""),
		result: []int{6},
	},
	test{
		name:   "Test Key in secondary",
		query:  bolthold.Where("Category").Eq("food").And(bolthold.Key()).Eq(testData[4].key()),
		result: []int{4},
	},
}

func insertTestData(t *testing.T, store *bolthold.Store) {
	for i := range testData {
		err := store.Insert(testData[i].key(), testData[i])
		if err != nil {
			t.Fatalf("Error inserting test data for find test: %s", err)
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
					if testing.Verbose() {
						t.Fatalf("Find result count is %d wanted %d.  Results: %v", len(result), len(tst.result), result)
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
							t.Fatalf("Could not find %v in the result set! Full results: %v", result[i], result)
						}
						t.Fatalf("Could not find %v in the result set!", result[i])
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

func TestQueryStringPrint(t *testing.T) {
	q := bolthold.Where("FirstField").Eq("first value").And("SecondField").Gt("Second Value").And("ThirdField").
		Lt("Third Value").And("FourthField").Ge("FourthValue").And("FifthField").Le("FifthValue").And("SixthField").
		Ne("Sixth Value").Or(bolthold.Where("FirstField").In("val1", "val2", "val3").And("SecondField").IsNil().
		And("ThirdField").RegExp(regexp.MustCompile("test")).And("FirstField").
		MatchFunc(func(field interface{}) (bool, error) {
			return true, nil
		}))

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
	}

	// map order isn't guaranteed, check if all needed lines exist

	tst := fmt.Sprintf("%s", q)

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
