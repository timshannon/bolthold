package gobstore_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/timshannon/gobstore"
)

func TestGet(t *testing.T) {
	testWrap(t, func(store *gobstore.Store, t *testing.T) {
		key := "testKey"
		data := &TestData{
			Name: "Test Name",
			Time: time.Now(),
		}
		err := store.Insert(key, data)
		if err != nil {
			t.Fatalf("Error creating data for get test: %s", err)
		}

		result := &TestData{}

		err = store.Get(key, result)
		if err != nil {
			t.Fatalf("Error getting data from gobstore: %s", err)
		}

		if !data.equal(result) {
			t.Fatalf("Got %s wanted %s.", result, data)
		}
	})
}

func TestFindEqualKey(t *testing.T) {
	testWrap(t, func(store *gobstore.Store, t *testing.T) {
		key := "findKey"
		data := &TestData{
			Name: "Find This",
			Time: time.Now(),
		}

		err := store.Insert(key, data)
		if err != nil {
			t.Fatalf("Error creating data for find test: %s", err)
		}

		for i := 0; i < 10; i++ {
			err := store.Insert(strconv.Itoa(i)+"test key", &TestData{
				Name: "test name",
				Time: time.Now(),
			})
			if err != nil {
				t.Fatalf("Error creating data for find test: %s", err)
			}
		}

		var result []TestData

		err = store.Find(&result, gobstore.Where(gobstore.Key()).Eq(key))

		if err != nil {
			t.Fatalf("Error finding data from gobstore: %s", err)
		}

		if len(result) != 1 {
			t.Fatalf("Find result count is %d wanted %d", len(result), 1)
		}

		if !result[0].equal(data) {
			t.Fatalf("Got %s wanted %s.", result[0], data)
		}

	})
}

func TestFindEqualField(t *testing.T) {
	testWrap(t, func(store *gobstore.Store, t *testing.T) {
		key := "findKey"
		data := &TestData{
			Name: "Find This",
			Time: time.Now(),
		}

		err := store.Insert(key, data)
		if err != nil {
			t.Fatalf("Error creating data for find test: %s", err)
		}

		for i := 0; i < 10; i++ {
			err := store.Insert(strconv.Itoa(i)+"test key", &TestData{
				Name: "test name",
				Time: time.Now(),
			})
			if err != nil {
				t.Fatalf("Error creating data for find test: %s", err)
			}
		}

		var result []TestData

		err = store.Find(&result, gobstore.Where("Name").Eq(data.Name))

		if err != nil {
			t.Fatalf("Error finding data from gobstore: %s", err)
		}

		if len(result) != 1 {
			t.Fatalf("Find result count is %d wanted %d", len(result), 1)
		}

		if !result[0].equal(data) {
			t.Fatalf("Got %s wanted %s.", result[0], data)
		}

	})
}
