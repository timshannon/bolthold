package gobstore_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/timshannon/gobstore"
)

func TestOpen(t *testing.T) {
	filename := tempfile()
	store, err := gobstore.Open(filename)
	if err != nil {
		t.Fatalf("Error opening %s: %s", filename, err)
	}

	if store == nil {
		t.Fatalf("store is null!")
	}

	defer store.Close()
	defer os.Remove(filename)
}

func TestFromBolt(t *testing.T) {
	filename := tempfile()
	db, err := bolt.Open(filename, 0666, nil)
	if err != nil {
		t.Fatalf("Error opening bolt db %s: %s", filename, err)
	}

	store, err := gobstore.FromBolt(db)
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
	testWrap(t, func(store *gobstore.Store, t *testing.T) {
		b := store.Bolt()
		if b == nil {
			t.Fatalf("Bolt is null in gobstore")
		}
	})
}

func TestRemoveIndex(t *testing.T) {
	testWrap(t, func(store *gobstore.Store, t *testing.T) {
		//TODO
	})
}

func TestReIndex(t *testing.T) {
	testWrap(t, func(store *gobstore.Store, t *testing.T) {
		//TODO
	})
}

// utilities

// testWrap creates a temporary database for testing and closes and cleans it up when
// completed.
func testWrap(t *testing.T, tests func(store *gobstore.Store, t *testing.T)) {
	filename := tempfile()
	store, err := gobstore.Open(filename)
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
	f, err := ioutil.TempFile("", "gobstore-")
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

type TestData struct {
	Name string
	Time time.Time
}

func (d *TestData) equal(other *TestData) bool {
	if d.Name != other.Name {
		return false
	}

	if !d.Time.Equal(other.Time) {
		return false
	}

	return true
}

func (d *TestData) String() string {
	return fmt.Sprintf("Name: %s \n Time: %s", d.Name, d.Time)
}
