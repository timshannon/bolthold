// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/boltdb/bolt"
	"github.com/timshannon/bolthold"
)

func TestOpen(t *testing.T) {
	filename := tempfile()
	store, err := bolthold.Open(filename, 0666, nil)
	if err != nil {
		t.Fatalf("Error opening %s: %s", filename, err)
	}

	if store == nil {
		t.Fatalf("store is null!")
	}

	defer store.Close()
	defer os.Remove(filename)
}

func TestBolt(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		b := store.Bolt()
		if b == nil {
			t.Fatalf("Bolt is null in bolthold")
		}
	})
}

// copy from index.go
func indexName(typeName, indexName string) []byte {
	return []byte("_index" + ":" + typeName + ":" + indexName)
}

func TestRemoveIndex(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		var item ItemTest

		iName := indexName("ItemTest", "Category")

		err := store.Bolt().View(func(tx *bolt.Tx) error {
			if tx.Bucket(iName) == nil {
				return fmt.Errorf("Index %s doesn't exist!", iName)
			}
			return nil
		})

		if err != nil {
			t.Fatal(err)
		}

		err = store.RemoveIndex(item, "Category")
		if err != nil {
			t.Fatalf("Error removing index %s", err)
		}

		err = store.Bolt().View(func(tx *bolt.Tx) error {
			if tx.Bucket(iName) != nil {
				return fmt.Errorf("Index %s wasn't removed!", iName)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

	})
}

func TestReIndex(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		var item ItemTest

		iName := indexName("ItemTest", "Category")

		err := store.RemoveIndex(item, "Category")
		if err != nil {
			t.Fatalf("Error removing index %s", err)
		}

		err = store.Bolt().View(func(tx *bolt.Tx) error {
			if tx.Bucket(iName) != nil {
				return fmt.Errorf("Index %s wasn't removed!", iName)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		err = store.ReIndex(&item, nil)
		if err != nil {
			t.Fatalf("Error reindexing store: %v", err)
		}

		err = store.Bolt().View(func(tx *bolt.Tx) error {
			if tx.Bucket(iName) == nil {
				return fmt.Errorf("Index %s wasn't rebuilt!", iName)
			}
			return nil
		})

		if err != nil {
			t.Fatal(err)
		}

	})
}

type ItemTestClone ItemTest

func TestReIndexWithCopy(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)

		var item ItemTestClone

		iName := indexName("ItemTestClone", "Category")

		err := store.ReIndex(&item, []byte("ItemTest"))
		if err != nil {
			t.Fatalf("Error reindexing store: %v", err)
		}

		err = store.Bolt().View(func(tx *bolt.Tx) error {
			if tx.Bucket(iName) == nil {
				return fmt.Errorf("Index %s wasn't rebuilt!", iName)
			}
			return nil
		})

		if err != nil {
			t.Fatal(err)
		}

	})
}

func TestAlternateEncoding(t *testing.T) {
	filename := tempfile()
	store, err := bolthold.Open(filename, 0666, &bolthold.Options{
		Encoder: json.Marshal,
		Decoder: json.Unmarshal,
	})
	defer store.Close()
	defer os.Remove(filename)

	if err != nil {
		t.Fatalf("Error opening %s: %s", filename, err)
	}

	insertTestData(t, store)

	tData := testData[3]

	var result []ItemTest

	store.Find(&result, bolthold.Where(bolthold.Key()).Eq(tData.key()))

	if len(result) != 1 {
		if testing.Verbose() {
			t.Fatalf("Find result count is %d wanted %d.  Results: %v", len(result), 1, result)
		}
		t.Fatalf("Find result count is %d wanted %d.", len(result), 1)
	}

	if !result[0].equal(&tData) {
		t.Fatalf("Results not equal! Wanted %v, got %v", tData, result[0])
	}

}

// utilities

// testWrap creates a temporary database for testing and closes and cleans it up when
// completed.
func testWrap(t *testing.T, tests func(store *bolthold.Store, t *testing.T)) {
	filename := tempfile()
	store, err := bolthold.Open(filename, 0666, nil)
	if err != nil {
		t.Fatalf("Error opening %s: %s", filename, err)
	}

	if store == nil {
		t.Fatalf("store is null!")
	}

	defer store.Close()
	defer os.Remove(filename)

	tests(store, t)
}

// tempfile returns a temporary file path.
func tempfile() string {
	f, err := ioutil.TempFile("", "bolthold-")
	if err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	if err := os.Remove(f.Name()); err != nil {
		panic(err)
	}
	return f.Name()
}
