// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/timshannon/bolthold"
	bolt "go.etcd.io/bbolt"
)

func TestOpen(t *testing.T) {
	filename := tempfile()
	store, err := bolthold.Open(filename, 0666, nil)
	ok(t, err)

	assert(t, store != nil, "store is null!")

	defer store.Close()
	defer os.Remove(filename)
}

func TestBolt(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		b := store.Bolt()
		assert(t, b != nil, "Bolt is null in bolthold")
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

		ok(t, store.Bolt().View(func(tx *bolt.Tx) error {
			if tx.Bucket(iName) == nil {
				return fmt.Errorf("index %s doesn't exist", iName)
			}
			return nil
		}))

		ok(t, store.RemoveIndex(item, "Category"))

		ok(t, store.Bolt().View(func(tx *bolt.Tx) error {
			if tx.Bucket(iName) != nil {
				return fmt.Errorf("index %s wasn't removed", iName)
			}
			return nil
		}))
	})
}

func TestReIndex(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		var item ItemTest

		iName := indexName("ItemTest", "Category")

		ok(t, store.RemoveIndex(item, "Category"))
		ok(t, store.Bolt().View(func(tx *bolt.Tx) error {
			if tx.Bucket(iName) != nil {
				return fmt.Errorf("index %s wasn't removed", iName)
			}
			return nil
		}))
		ok(t, store.ReIndex(&item, nil))
		ok(t, store.Bolt().View(func(tx *bolt.Tx) error {
			if tx.Bucket(iName) == nil {
				return fmt.Errorf("index %s wasn't rebuilt", iName)
			}
			return nil
		}))
	})
}

func TestIndexExists(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)
		ok(t, store.Bolt().View(func(tx *bolt.Tx) error {
			if !store.IndexExists(tx, "ItemTest", "Category") {
				return fmt.Errorf("index %s doesn't exist", "ItemTest:Category")
			}
			return nil
		}))
	})
}

type ItemTestClone ItemTest

func TestReIndexWithCopy(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		insertTestData(t, store)

		var item ItemTestClone

		iName := indexName("ItemTestClone", "Category")

		ok(t, store.ReIndex(&item, []byte("ItemTest")))

		ok(t, store.Bolt().View(func(tx *bolt.Tx) error {
			if tx.Bucket(iName) == nil {
				return fmt.Errorf("index %s wasn't rebuilt", iName)
			}
			return nil
		}))
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

	ok(t, err)

	insertTestData(t, store)

	tData := testData[3]

	var result []ItemTest

	store.Find(&result, bolthold.Where(bolthold.Key).Eq(tData.Key))

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

func TestPerStoreEncoding(t *testing.T) {
	jsnFilename := tempfile()
	jsnStore, err := bolthold.Open(jsnFilename, 0666, &bolthold.Options{
		Encoder: json.Marshal,
		Decoder: json.Unmarshal,
	})
	defer jsnStore.Close()
	defer os.Remove(jsnFilename)

	ok(t, err)

	gobFilename := tempfile()
	gobStore, err := bolthold.Open(gobFilename, 0666, &bolthold.Options{})
	defer gobStore.Close()
	defer os.Remove(gobFilename)

	ok(t, err)

	insertTestData(t, jsnStore)
	insertTestData(t, gobStore)

	tData := testData[3]

	var result []ItemTest

	jsnStore.Find(&result, bolthold.Where(bolthold.Key).Eq(tData.Key))

	if len(result) != 1 {
		if testing.Verbose() {
			t.Fatalf("Find result count is %d wanted %d.  Results: %v", len(result), 1, result)
		}
		t.Fatalf("Find result count is %d wanted %d.", len(result), 1)
	}

	if !result[0].equal(&tData) {
		t.Fatalf("Results not equal! Wanted %v, got %v", tData, result[0])
	}

	result = []ItemTest{}

	gobStore.Find(&result, bolthold.Where(bolthold.Key).Eq(tData.Key))

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

func TestGetUnknownType(t *testing.T) {
	filename := tempfile()
	store, err := bolthold.Open(filename, 0666, &bolthold.Options{
		Encoder: json.Marshal,
		Decoder: json.Unmarshal,
	})
	defer store.Close()
	defer os.Remove(filename)

	ok(t, err)
	type test struct {
		Test string
	}

	var result test
	err = store.Get("unknownKey", &result)
	assert(t, err == bolthold.ErrNotFound, "Expected error of type ErrNotFound, not %T", err)
}

func TestEmptyReIndexIssue89(t *testing.T) {
	testWrap(t, func(store *bolthold.Store, t *testing.T) {
		var item ItemTest
		ok(t, store.ReIndex(&item, nil))
		// shouldn't panic
	})
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

// Thanks Ben Johnson https://github.com/benbjohnson/testing

// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: "+msg+"\033[39m\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.FailNow()
	}
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: unexpected error: %s\033[39m\n\n", filepath.Base(file), line, err.Error())
		tb.FailNow()
	}
}

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n", filepath.Base(file), line, exp, act)
		tb.FailNow()
	}
}
