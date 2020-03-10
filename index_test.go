// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold_test

import (
	"testing"
	"time"

	bh "github.com/timshannon/bolthold"
	bolt "go.etcd.io/bbolt"
)

func TestIndexSlice(t *testing.T) {
	testWrap(t, func(store *bh.Store, t *testing.T) {
		var testData = []ItemTest{
			ItemTest{
				Key:  0,
				Name: "John",
				Tags: []string{"red", "green", "blue"},
			},
			ItemTest{
				Key:  1,
				Name: "Bill",
				Tags: []string{"red", "purple"},
			},
			ItemTest{
				Key:  2,
				Name: "Jane",
				Tags: []string{"red", "orange"},
			},
			ItemTest{
				Key:  3,
				Name: "Brian",
				Tags: []string{"red", "purple"},
			},
		}

		for _, data := range testData {
			ok(t, store.Insert(data.Key, data))
		}

		b := store.Bolt()

		ok(t, b.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("_index:ItemTest:Tags"))
			assert(t, bucket != nil, "No index bucket found for Tags index")

			indexCount := 0
			bucket.ForEach(func(k, v []byte) error {
				indexCount++
				return nil
			})

			// each tag chould be indexed individually and there are 5 different tags
			equals(t, indexCount, 5)
			return nil
		}))

	})
}

func Test85SliceIndex(t *testing.T) {
	type Event struct {
		Id         uint64
		Type       string   `boltholdIndex:"Type"`
		Categories []string `boltholdSliceIndex:"Categories"`
	}

	testWrap(t, func(store *bh.Store, t *testing.T) {
		e1 := &Event{Id: 1, Type: "Type1", Categories: []string{"Cat 1", "Cat 2"}}
		e2 := &Event{Id: 2, Type: "Type1", Categories: []string{"Cat 3"}}

		ok(t, store.Insert(e1.Id, e1))
		ok(t, store.Insert(e2.Id, e2))

		var es []*Event
		ok(t, store.Find(&es, bh.Where("Categories").Contains("Cat 1").Index("Categories")))
		equals(t, len(es), 1)
	})
}

func Test87SliceIndex(t *testing.T) {
	type Event struct {
		Id         uint64
		Type       string   `boltholdIndex:"Type"`
		Categories []string `boltholdSliceIndex:"Categories"`
	}

	testWrap(t, func(store *bh.Store, t *testing.T) {
		e1 := &Event{Id: 1, Type: "Type1", Categories: []string{"Cat 1", "Cat 2"}}
		e2 := &Event{Id: 2, Type: "Type1", Categories: []string{"Cat 3"}}

		ok(t, store.Insert(e1.Id, e1))
		ok(t, store.Insert(e2.Id, e2))
		var es []*Event
		ok(t, store.Find(&es, bh.Where("Categories").ContainsAny("Cat 1").Index("Categories")))
		equals(t, len(es), 1)
	})
}

func TestSliceIndexWithPointers(t *testing.T) {
	type Event struct {
		Id         uint64
		Type       string    `boltholdIndex:"Type"`
		Categories []*string `boltholdSliceIndex:"Categories"`
	}

	testWrap(t, func(store *bh.Store, t *testing.T) {
		cat1 := "Cat 1"
		cat2 := "Cat 2"
		cat3 := "Cat 3"

		e1 := &Event{Id: 1, Type: "Type1", Categories: []*string{&cat1, &cat2}}
		e2 := &Event{Id: 2, Type: "Type1", Categories: []*string{&cat3}}

		ok(t, store.Insert(e1.Id, e1))
		ok(t, store.Insert(e2.Id, e2))

		var es []*Event
		ok(t, store.Find(&es, bh.Where("Categories").ContainsAll("Cat 1").Index("Categories")))
		equals(t, len(es), 1)
	})
}

func Test90AnonIndex(t *testing.T) {
	type (
		Profile struct {
			Username string `boltholdIndex:"Username" json:"username"`
			Password string `json:"password"`
		}

		User struct {
			Profile

			ID   string
			Name string
		}
	)

	testWrap(t, func(store *bh.Store, t *testing.T) {
		ok(t, store.Insert(1, &User{
			Profile: Profile{
				Username: "test",
				Password: "test",
			},
			ID:   "1234",
			Name: "Tester",
		}))

		b := store.Bolt()

		ok(t, b.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(indexName("User", "Username"))
			assert(t, bucket != nil, "No index bucket found")

			indexCount := 0
			bucket.ForEach(func(k, v []byte) error {
				indexCount++
				return nil
			})

			equals(t, indexCount, 1)
			return nil
		}))

	})
}

func Test90AnonIndexPointer(t *testing.T) {
	type (
		Profile struct {
			Username string `boltholdIndex:"Username" json:"username"`
			Password string `json:"password"`
		}

		User struct {
			*Profile

			ID   string
			Name string
		}
	)

	testWrap(t, func(store *bh.Store, t *testing.T) {
		ok(t, store.Insert(1, &User{
			Profile: &Profile{
				Username: "test",
				Password: "test",
			},
			ID:   "1234",
			Name: "Tester",
		}))

		b := store.Bolt()

		ok(t, b.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(indexName("User", "Username"))
			assert(t, bucket != nil, "No index bucket found")

			indexCount := 0
			bucket.ForEach(func(k, v []byte) error {
				indexCount++
				return nil
			})

			equals(t, indexCount, 1)
			return nil
		}))

	})
}

func Test94NilAnonIndexPointer(t *testing.T) {
	type (
		Profile struct {
			Username string `boltholdIndex:"Username" json:"username"`
			Password string `json:"password"`
		}

		User struct {
			*Profile

			ID   string
			Name string
		}
	)

	testWrap(t, func(store *bh.Store, t *testing.T) {
		ok(t, store.Insert(1, &User{
			ID:   "1234",
			Name: "Tester",
		}))

		b := store.Bolt()

		ok(t, b.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(indexName("User", "Username"))
			assert(t, bucket == nil, "Found index where none should've been added")
			return nil
		}))

	})
}

func Test98MultipleAnonIndex(t *testing.T) {
	type (
		Profile struct {
			Username string `boltholdIndex:"Username"`
			Password string
		}
		Account struct {
			GuardianName string `boltholdIndex:"GuardianName"`
			Create       time.Time
		}

		User struct {
			*Profile
			*Account

			ID   string
			Name string
		}
	)

	t.Run("one nil", func(t *testing.T) {
		testWrap(t, func(store *bh.Store, t *testing.T) {
			ok(t, store.Insert(1, &User{
				ID:   "1234",
				Name: "Tester",
				Account: &Account{
					GuardianName: "Test",
					Create:       time.Now(),
				},
			}))

			b := store.Bolt()

			ok(t, b.View(func(tx *bolt.Tx) error {
				bucket := tx.Bucket(indexName("User", "Username"))
				assert(t, bucket == nil, "Found index where none should've been added")

				bucket = tx.Bucket(indexName("User", "GuardianName"))
				assert(t, bucket != nil, "No index found for GuardianName")
				return nil
			}))

		})
	})

	t.Run("neither nil", func(t *testing.T) {
		testWrap(t, func(store *bh.Store, t *testing.T) {
			ok(t, store.Insert(1, &User{
				ID:   "1234",
				Name: "Tester",
				Profile: &Profile{
					Username: "test",
					Password: "test",
				},
				Account: &Account{
					GuardianName: "Test",
					Create:       time.Now(),
				},
			}))

			b := store.Bolt()

			ok(t, b.View(func(tx *bolt.Tx) error {
				bucket := tx.Bucket(indexName("User", "Username"))
				assert(t, bucket != nil, "No index found for Username")

				bucket = tx.Bucket(indexName("User", "GuardianName"))
				assert(t, bucket != nil, "No index found for GuardianName")
				return nil
			}))
		})
	})

	t.Run("both nil", func(t *testing.T) {
		testWrap(t, func(store *bh.Store, t *testing.T) {
			ok(t, store.Insert(1, &User{
				ID:   "1234",
				Name: "Tester",
			}))

			b := store.Bolt()

			ok(t, b.View(func(tx *bolt.Tx) error {
				bucket := tx.Bucket(indexName("User", "Username"))
				assert(t, bucket == nil, "Index found for Username")

				bucket = tx.Bucket(indexName("User", "GuardianName"))
				assert(t, bucket == nil, "Index found for GuardianName")
				return nil
			}))
		})
	})
}

func TestNestedAnonIndex(t *testing.T) {
	type (
		Credential struct {
			Username string `boltholdIndex:"Username"`
			Password string
		}
		Profile struct {
			*Credential
			Address string
		}
		Account struct {
			GuardianName string `boltholdIndex:"Guardian"`
			Create       time.Time
		}

		User struct {
			*Profile
			*Account

			ID   string
			Name string
		}
	)

	t.Run("nil", func(t *testing.T) {
		testWrap(t, func(store *bh.Store, t *testing.T) {
			ok(t, store.Insert(1, &User{
				ID:   "1234",
				Name: "Tester",
				Account: &Account{
					GuardianName: "Test",
					Create:       time.Now(),
				},
			}))

			b := store.Bolt()

			ok(t, b.View(func(tx *bolt.Tx) error {
				bucket := tx.Bucket(indexName("User", "Username"))
				assert(t, bucket == nil, "Found index where none should've been added")

				bucket = tx.Bucket(indexName("User", "Guardian"))
				assert(t, bucket != nil, "No index found for Guardian")
				return nil
			}))

		})
	})

	t.Run("not nil", func(t *testing.T) {
		testWrap(t, func(store *bh.Store, t *testing.T) {
			ok(t, store.Insert(1, &User{
				ID:   "1234",
				Name: "Tester",
				Profile: &Profile{
					Credential: &Credential{
						Username: "test",
						Password: "test",
					},
					Address: "test",
				},
				Account: &Account{
					GuardianName: "Test",
					Create:       time.Now(),
				},
			}))

			b := store.Bolt()

			ok(t, b.View(func(tx *bolt.Tx) error {
				bucket := tx.Bucket(indexName("User", "Username"))
				assert(t, bucket != nil, "No index found for Username")

				bucket = tx.Bucket(indexName("User", "Guardian"))
				assert(t, bucket != nil, "No index found for Guardian")
				return nil
			}))
		})
	})

	t.Run("nested nil", func(t *testing.T) {
		testWrap(t, func(store *bh.Store, t *testing.T) {
			ok(t, store.Insert(1, &User{
				ID:   "1234",
				Name: "Tester",
				Profile: &Profile{
					Address: "test",
				},
			}))

			b := store.Bolt()

			ok(t, b.View(func(tx *bolt.Tx) error {
				bucket := tx.Bucket(indexName("User", "Username"))
				assert(t, bucket == nil, "Index found for Username")

				bucket = tx.Bucket(indexName("User", "Guardian"))
				assert(t, bucket == nil, "Index found for Guardian")
				return nil
			}))
		})
	})

	t.Run("Select Nested Anon", func(t *testing.T) {
		testWrap(t, func(store *bh.Store, t *testing.T) {
			ok(t, store.Insert(1, &User{
				ID:   "1234",
				Name: "Tester",
				Profile: &Profile{
					Address: "test",
				},
			}))

			res := &User{}

			ok(t, store.FindOne(res, bh.Where("Profile.Address").Eq("test")))
			equals(t, res.Profile.Address, "test")

			ok(t, store.FindOne(res, bh.Where("Address").Eq("test")))
			equals(t, res.Profile.Address, "test")
		})
	})

	t.Run("Select Nested Anon with index", func(t *testing.T) {
		testWrap(t, func(store *bh.Store, t *testing.T) {
			ok(t, store.Insert(1, &User{
				ID:   "1234",
				Name: "Tester",
				Profile: &Profile{
					Credential: &Credential{
						Username: "test",
						Password: "test",
					},
					Address: "test",
				},
				Account: &Account{
					GuardianName: "Test",
					Create:       time.Now(),
				},
			}))

			res := &User{}

			ok(t, store.FindOne(res, bh.Where("Profile.Username").Eq("test")))
			equals(t, res.Profile.Username, "test")

			ok(t, store.FindOne(res, bh.Where("Username").Eq("test")))
			equals(t, res.Profile.Username, "test")

			ok(t, store.FindOne(res, bh.Where("Profile.Username").Eq("test").Index("Username")))
			equals(t, res.Profile.Username, "test")

			ok(t, store.FindOne(res, bh.Where("Username").Eq("test").Index("Username")))
			equals(t, res.Profile.Username, "test")

			ok(t, store.FindOne(res, bh.Where("Profile.Credential.Username").Eq("test")))
			equals(t, res.Profile.Username, "test")

			ok(t, store.FindOne(res, bh.Where("Profile.Credential.Username").Eq("test").Index("Username")))
			equals(t, res.Profile.Username, "test")
		})
	})
	t.Run("Select Nested Anon with non field name index", func(t *testing.T) {
		testWrap(t, func(store *bh.Store, t *testing.T) {
			ok(t, store.Insert(1, &User{
				ID:   "1234",
				Name: "Tester",
				Profile: &Profile{
					Credential: &Credential{
						Username: "test",
						Password: "test",
					},
					Address: "test",
				},
				Account: &Account{
					GuardianName: "Test",
					Create:       time.Now(),
				},
			}))

			res := &User{}

			ok(t, store.FindOne(res, bh.Where("Account.GuardianName").Eq("Test")))
			equals(t, res.GuardianName, "Test")

			ok(t, store.FindOne(res, bh.Where("GuardianName").Eq("Test")))
			equals(t, res.GuardianName, "Test")

			ok(t, store.FindOne(res, bh.Where("Account.GuardianName").Eq("Test").Index("Guardian")))
			equals(t, res.GuardianName, "Test")

			ok(t, store.FindOne(res, bh.Where("GuardianName").Eq("Test").Index("Guardian")))
			equals(t, res.GuardianName, "Test")
		})
	})
}
