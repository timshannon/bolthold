// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold_test

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/boltdb/bolt"
	"github.com/timshannon/bolthold"
)

type Item struct {
	ID       int
	Category string `boltholdIndex:"Category"`
	Created  time.Time
}

func Example() {
	data := []Item{
		Item{
			ID:       0,
			Category: "blue",
			Created:  time.Now().Add(-4 * time.Hour),
		},
		Item{
			ID:       1,
			Category: "red",
			Created:  time.Now().Add(-3 * time.Hour),
		},
		Item{
			ID:       2,
			Category: "blue",
			Created:  time.Now().Add(-2 * time.Hour),
		},
		Item{
			ID:       3,
			Category: "blue",
			Created:  time.Now().Add(-20 * time.Minute),
		},
	}

	filename := tempfile()
	store, err := bolthold.Open(filename, 0666, nil)
	defer store.Close()
	defer os.Remove(filename)

	if err != nil {
		// handle error
		log.Fatal(err)
	}

	// insert the data in one transaction

	err = store.Bolt().Update(func(tx *bolt.Tx) error {
		for i := range data {
			err := store.TxInsert(tx, data[i].ID, data[i])
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		// handle error
		log.Fatal(err)
	}

	// Find all items in the blue category that have been update in the past hour
	var result []Item

	err = store.Find(&result, bolthold.Where("Category").Eq("blue").And("Created").Ge(time.Now().Add(-1*time.Hour)))

	if err != nil {
		// handle error
		log.Fatal(err)
	}

	fmt.Println(result[0].ID)
	// Output: 3

}
