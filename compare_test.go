// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold_test

import (
	"math/big"
	"math/rand"
	"testing"
	"time"

	"github.com/timshannon/bolthold"
)

type CItemTest struct {
	Inner ItemTest
}

func (i *ItemTest) Compare(other interface{}) (int, error) {
	if other, ok := other.(ItemTest); ok {
		if i.ID == other.ID {
			return 0, nil
		}

		if i.ID < other.ID {
			return -1, nil
		}

		return 1, nil
	}

	return 0, &bolthold.ErrTypeMismatch{i, other}
}

func TestFindWithComparer(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		data := []CItemTest{
			CItemTest{
				Inner: ItemTest{ //0
					ID:       0,
					Name:     "car",
					Category: "vehicle",
					Created:  time.Now().AddDate(-1, 0, 0),
				},
			},
			CItemTest{
				Inner: ItemTest{ //1
					ID:       1,
					Name:     "truck",
					Category: "vehicle",
					Created:  time.Now().AddDate(0, 30, 0),
				},
			},
			CItemTest{
				Inner: ItemTest{ //2
					ID:       2,
					Name:     "seal",
					Category: "animal",
					Created:  time.Now().AddDate(-1, 0, 0),
				},
			},
		}

		for i := range data {
			err := store.Insert(data[i].Inner.key(), data[i])
			if err != nil {
				t.Fatalf("Error inserting CItemData for comparer test %s", err)
			}
		}

		var result []CItemTest
		err := store.Find(&result, bolthold.Where("Inner").Gt(data[1].Inner))
		if err != nil {
			t.Fatalf("Error retriving data in comparer test: %s", err)
		}

		if len(result) != 1 {
			if testing.Verbose() {
				t.Fatalf("Find result count is %d wanted %d.  Results: %v", len(result), 1, result)
			}
			t.Fatalf("Find result count is %d wanted %d.", len(result), 1)
		}
	})
}

type DefaultType struct {
	Val string
}

func (d *DefaultType) String() {
	return d.Val
}

type All struct {
	ATime  time.Time
	AFloat big.Float
	AInt   big.Int
	ARat   big.Rat

	Aint   int
	Aint8  int8
	Aint16 int16
	Aint32 int32
	Aint64 int64

	Auint   uint
	Auint8  uint8
	Auint16 uint16
	Auint32 uint32
	Auint64 uint64

	Afloat32 float32
	Afloat64 float64

	Astring string
}

var allData = []All{
	All{ // current
		ATime:  time.Date(2016, 1, 1, 0, 0, 0, 0, time.Local),
		AFloat: big.NewFloat(30.5),
		AInt:   big.NewInt(123),
		ARat:   big.NewRat(5, 8),

		Aint:   8,
		Aint8:  8,
		Aint16: 8,
		Aint32: 8,
		Aint64: 8,

		Auint:   8,
		Auint8:  8,
		Auint16: 8,
		Auint32: 8,
		Auint64: 8,

		Afloat32: 8.8,
		Afloat64: 8.8,

		Astring: "btest",
	},
	All{ // equal
		ATime:  time.Date(2016, 1, 1, 0, 0, 0, 0, time.Local),
		AFloat: big.NewFloat(30.5),
		AInt:   big.NewInt(123),
		ARat:   big.NewRat(5, 8),

		Aint:   8,
		Aint8:  8,
		Aint16: 8,
		Aint32: 8,
		Aint64: 8,

		Auint:   8,
		Auint8:  8,
		Auint16: 8,
		Auint32: 8,
		Auint64: 8,

		Afloat32: 8.8,
		Afloat64: 8.8,

		Astring: "btest",
	},
	All{ // greater
		ATime:  time.Date(2017, 1, 1, 0, 0, 0, 0, time.Local),
		AFloat: big.NewFloat(31.5),
		AInt:   big.NewInt(128),
		ARat:   big.NewRat(14, 16),

		Aint:   9,
		Aint8:  9,
		Aint16: 9,
		Aint32: 9,
		Aint64: 9,

		Auint:   9,
		Auint8:  9,
		Auint16: 9,
		Auint32: 9,
		Auint64: 9,

		Afloat32: 9.8,
		Afloat64: 9.8,

		Astring: "ctest",
	},
	All{ // less
		ATime:  time.Date(2015, 1, 1, 0, 0, 0, 0, time.Local),
		AFloat: big.NewFloat(30.1),
		AInt:   big.NewInt(121),
		ARat:   big.NewRat(1, 4),

		Aint:   4,
		Aint8:  4,
		Aint16: 4,
		Aint32: 4,
		Aint64: 4,

		Auint:   4,
		Auint8:  4,
		Auint16: 4,
		Auint32: 4,
		Auint64: 4,

		Afloat32: 4.8,
		Afloat64: 4.8,

		Astring: "atest",
	},
}

func TestFindWithBuiltinTypes(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		for i := range allData {
			err := store.Insert(rand.Int(), allData[i])
			if err != nil {
				t.Fatalf("Error inserting allData for builtin compare test %s", err)
			}
		}

	})
}
