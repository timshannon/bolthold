// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/timshannon/bolthold"
	bh "github.com/timshannon/bolthold"
	bolt "go.etcd.io/bbolt"
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
				ok(t, store.Find(&result, tst.query))
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

func TestIssue100(t *testing.T) {
	type (
		Profile struct {
			Username   string `boltholdIndex:"Username" json:"name"`
			Name       string `boltholdIndex:"Name" json:"name"`
			Phone      string `boltholdIndex:"Phone" json:"name"`
			Address1   string `json:"address1"`
			Address2   string `json:"address2"`
			City       string `json:"city"`
			State      string `json:"state"`
			PostalCode string `json:"postal_code"`
		}

		Parent struct {
			ID string `json:"id"`
		}

		User struct {
			*Parent
			*Profile

			ID       string `boltholdKey:"ID" json:"id"`
			District string `json:"district"`
		}
	)

	testWrap(t, func(store *bh.Store, t *testing.T) {
		store.Insert("userID", &User{
			ID:       "userID",
			District: "West",
		})

		users := []*User{}
		query := bolthold.Where("Name").Eq("blah")
		ok(t, store.Find(&users, query))

		equals(t, len(users), 0)
	})
}
