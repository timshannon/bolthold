// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold_test

import (
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
	Name string `boltholdIndex:"Name"`
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
		result: []int{1, 2, 0, 3},
	},
	test{
		name:   "Index",
		query:  bolthold.Where("L1.Name").Eq("Joe").Index("L1.Name"),
		result: []int{0},
	},
	test{
		name:   "Index Multiple Levels",
		query:  bolthold.Where("L2.L3.Name").Eq("Joe").Index("L2.L3.Name"),
		result: []int{0, 3},
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
		runTests(store, nestedTests, t)
	})
}
