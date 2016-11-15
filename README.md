#BoltHold [![Build Status](https://travis-ci.org/timshannon/bolthold.svg?branch=master)](https://travis-ci.org/timshannon/bolthold) [![GoDoc](https://godoc.org/github.com/timshannon/bolthold?status.svg)](https://godoc.org/github.com/timshannon/bolthold) [![Coverage Status](https://coveralls.io/repos/github/timshannon/bolthold/badge.svg?branch=master)](https://coveralls.io/github/timshannon/bolthold?branch=master) [![Go Report Card](https://goreportcard.com/badge/github.com/timshannon/bolthold)](https://goreportcard.com/report/github.com/timshannon/bolthold)


BoltHold is a simple querying and indexing layer on top of a Bolt DB instance. The goal is to create a simple,
higher level interface on top of Bolt DB that simplifies dealing with Go Types and finding data, but exposes the underlying 
Bolt DB for customizing as you wish.  By default the encoding used is Gob, so feel free to `gob.Register` any types you 
wish, or use the GobEncoder/Decoder interface for faster serialization.  Or you can use any serialization you want by 
supplying encode / decode funcs to the `Options` struct on Open.

One Go Type will have one bucket, and multiple index buckets in a BoltDB file, so you can store multiple Go Types in the
same database.

## Why not just use Bolt DB directly?
I love BoltDB, and I've used it in several projects.  However, I find myself writing the same code over and over again,
for encoding and decoding objects and searching through data.  I figure formalizing how I've been using BoltDB 
and including tests and benchmarks will, at a minimum, be useful to me.  Maybe it'll be useful to others as well.

##Indexes
Indexes allow you to skip checking any records that don't meet your index criteria.  If you have 1000 records and only
10 of them are of the Division you want to deal with, then you don't need to check to see if the other 990 records match
your query criteria if you create an index on the Division field.  The downside of an index is added disk reads and writes
on every write operation.  For read heavy operations datasets, indexes can be very useful.

In every BoltHold store, there will be a reserved bucket *_indexes* which will be used to hold indexes that point back 
to another bucket's Key system.  Indexes will be defined by setting the `boltholdIndex` struct tag on a field in a type.

```Go
type Person struct {
	Name string
	Division string `boltholdIndex:"Division"`
}

```

This means that there will be an index created for `Division` that will contain the set of unique divisions, and the
main record keys they refer to.

Optionally, you can implement the `Storer` interface, to specify your own indexes, rather than using the `boltHoldIndex` 
struct tag.

## Queries
Queries will be chain-able constructs that filters out any data that doesn't match it's criteria. There will be no 
"query optimiser". The first field listed in the query will be the index that the query starts at (if one exists).

Queries will look like this:
```Go
s.Find(bolthold.Where("FieldName").Eq(value).And("AnotherField").Lt(AnotherValue).Or(bolthold.Where("FieldName").Eq(anotherValue)

```

Fields must be exported, and thus always need to start with an upper-case letter.  Available operators include:
* Equal - `Where("field").Eq(value)`
* Not Equal - `Where("field").Ne(value)`
* Greater Than - `Where("field").Gt(value)`
* Less Than - `Where("field").Lt(value)`
* Less than or Equal To - `Where("field").Le(value)`
* Greater Than or Equal To - `Where("field").Ge(value)`
* In - `Where("field").In(val1, val2, val3)`
* IsNil - `Where("field").IsNil()`
* Regular Expression - `Where("field").RegExp(regexp.MustCompile("ea"))`
* Matches Function - `Where("field").MatchFunc(func(field interface{}) (bool, error))`

Instead of passing in a specific value to compare against in a query, you can compare against another field in the same
struct.  Consider the following struct:

```Go
type Person struct {
	Name string
	Birth time.Time
	Death time.Time
}

```

If you wanted to find any invalid records where a Person's death was before their birth, you could do the following: 

```Go

store.Find(&result, bolthold.Where("Death").Lt(bolthold.Field("Birth")))

```

If you want to run a query's criteria against the Key value, you can use the `bolthold.Key()` function:
```Go

store.Find(&result, bolthold.Where(bolthold.Key()).Ne(value))

```

## Comparing

Just like with Go, types must be the same in order to be compared with each other.  You cannot compare an int to a int32.
The built-in Go comparable types (ints, floats, strings, etc) will work as expected.  Other types from the standard library
can also be compared such as `time.Time`, `big.Rat`, `big.Int`, and `big.Float`.  If there are other standard library
types that I missed, let me know.

You can compare any custom type either by using the `MatchFunc` criteria, or by satisfying the `Comparer` interface with
your type by adding the Compare method: `Compare(other interface{}) (int, error)`.

If a type doesn't have a predefined comparer, and doesn't satisfy the Comparer interface, then types value is converted
to a string and compared lexicographically.

## Behavior Changes
Since BoltHold is a higher level interface than BoltDB, there are some added helpers.  Instead of *Put*, you
have the options of: 
* *Insert* - Fails if key already exists.
* *Update* - Fails if key doesn't exist `ErrNotFound`.
* *Upsert* - If key doesn't exist, it inserts the data, otherwise it updates the existing record.

When getting data instead of returning `nil` if a value doesn't exist, BoltHold returns `boldhold.ErrNotFound`, and
similarly when deleting data, instead of silently continuing if a value isn't found to delete, BoltHold returns 
`bolthold.ErrNotFound`.

## When should I use BoltHold?
BoltHold will be useful in the same scenarios where BoltDB is useful, with the added benefit of being able to retire 
some of your data filtering code and possibly improved performance.

You can also use it instead of SQLite for many scenarios.  BoltHold's main benefit over SQLite is it's simplicity when 
working with Go Types.  There is no need for an ORM layer to translate records to types, simply put types in, and get 
types out.  You also don't have to deal with database initialization.  Usually with SQLite you'll need several scripts 
to create the database, create the tables you expect, and create any indexes.  With BoltHold you simply open a new file 
and put any type of data you want in it.

```Go
store, err := bolthold.Open(filename, 0666, nil)
if err != nil {
	//handle error
}
err = store.Insert("key", &Item{
	Name:    "Test Name",
	Created: time.Now(),
})

```

That's it!

Bolthold is still very much alpha software at this point, but there is currently over 80% coverage in unit tests, and 
it's backed by BoltDB which is a very solid and well built piece of software, so I encourage you to give it a try.

If you end up using BoltHold, I'd love to hear about it.
