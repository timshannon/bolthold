// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold_test

import (
	"reflect"
	"testing"

	"github.com/timshannon/bolthold"
)

type Nested struct {
	Key int
	Embed
	L1      Nest
	L2      Level2
	Pointer *Nest
}

type Embed struct {
	Color string
}

type Nest struct {
	Name string
}

type Level2 struct {
	Name string
	L3   Nest
}

var nestedData = []Nested{
	Nested{
		Key: 0,
		Embed: Embed{
			Color: "red",
		},
		L1: Nest{
			Name: "Joe",
		},
		L2: Level2{
			Name: "Joe",
			L3: Nest{
				Name: "Joe",
			},
		},
		Pointer: &Nest{
			Name: "Joe",
		},
	},
	Nested{
		Key: 1,
		Embed: Embed{
			Color: "red",
		},
		L1: Nest{
			Name: "Jill",
		},
		L2: Level2{
			Name: "Jill",
			L3: Nest{
				Name: "Jill",
			},
		},
		Pointer: &Nest{
			Name: "Jill",
		},
	},
	Nested{
		Key: 2,
		Embed: Embed{
			Color: "orange",
		},
		L1: Nest{
			Name: "Jill",
		},
		L2: Level2{
			Name: "Jill",
			L3: Nest{
				Name: "Jill",
			},
		},
		Pointer: &Nest{
			Name: "Jill",
		},
	},
	Nested{
		Key: 3,
		Embed: Embed{
			Color: "orange",
		},
		L1: Nest{
			Name: "Jill",
		},
		L2: Level2{
			Name: "Jill",
			L3: Nest{
				Name: "Joe",
			},
		}, Pointer: &Nest{
			Name: "Jill",
		},
	},
	Nested{
		Key: 4,
		Embed: Embed{
			Color: "blue",
		},
		L1: Nest{
			Name: "Abner",
		},
		L2: Level2{
			Name: "Abner",
			L3: Nest{
				Name: "Abner",
			},
		}, Pointer: &Nest{
			Name: "Abner",
		},
	},
}

var nestedTests = []test{
	test{
		name:   "Nested",
		query:  bolthold.Where("L1.Name").Eq("Joe"),
		result: []int{0},
	},
	test{
		name:   "Embedded",
		query:  bolthold.Where("Color").Eq("red"),
		result: []int{0, 1},
	},
	test{
		name:   "Embedded Explicit",
		query:  bolthold.Where("Embed.Color").Eq("red"),
		result: []int{0, 1},
	},
	test{
		name:   "Nested Multiple Levels",
		query:  bolthold.Where("L2.L3.Name").Eq("Joe"),
		result: []int{0, 3},
	},
	test{
		name:   "Pointer",
		query:  bolthold.Where("Pointer.Name").Eq("Jill"),
		result: []int{1, 2, 3},
	},
	test{
		name:   "Sort",
		query:  bolthold.Where("Key").Ge(0).SortBy("L2.L3.Name"),
		result: []int{4, 1, 2, 0, 3},
	},
	test{
		name:   "Sort On Pointer",
		query:  bolthold.Where("Key").Ge(0).SortBy("Pointer.Name"),
		result: []int{4, 1, 2, 0, 3},
	},
}

func TestNested(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		for i := range nestedData {
			err := store.Insert(nestedData[i].Key, nestedData[i])
			if err != nil {
				t.Fatalf("Error inserting nested test data for nested find test: %s", err)
			}
		}
		for _, tst := range nestedTests {
			t.Run(tst.name, func(t *testing.T) {
				var result []Nested
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
						if reflect.DeepEqual(result[i], nestedData[tst.result[k]]) {
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
