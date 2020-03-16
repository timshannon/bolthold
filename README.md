# BoltHold
[![Build Status](https://travis-ci.org/timshannon/bolthold.svg?branch=master)](https://travis-ci.org/timshannon/bolthold) [![GoDoc](https://godoc.org/github.com/timshannon/bolthold?status.svg)](https://pkg.go.dev/github.com/timshannon/bolthold) [![Coverage Status](https://coveralls.io/repos/github/timshannon/bolthold/badge.svg?branch=master)](https://coveralls.io/github/timshannon/bolthold?branch=master) [![Go Report Card](https://goreportcard.com/badge/github.com/timshannon/bolthold)](https://goreportcard.com/report/github.com/timshannon/bolthold)


BoltHold is a simple querying and indexing layer on top of a Bolt DB instance.  For a similar library built on
[Badger](https://github.com/dgraph-io/badger) see [BadgerHold](https://github.com/timshannon/badgerhold).

The goal is to create a simple,
higher level interface on top of Bolt DB that simplifies dealing with Go Types and finding data, but exposes the underlying
Bolt DB for customizing as you wish.  By default the encoding used is Gob, so feel free to use the GobEncoder/Decoder
interface for faster serialization.  Or, alternately, you can use any serialization you want by supplying encode / decode
funcs to the `Options` struct on Open.

One Go Type will have one bucket, and multiple index buckets in a BoltDB file, so you can store multiple Go Types in the
same database.

## Why not just use Bolt DB directly?
I love BoltDB, and I've used it in several projects.  However, I find myself writing the same code over and over again,
for encoding and decoding objects and searching through data.  I figure formalizing how I've been using BoltDB
and including tests and benchmarks will, at a minimum, be useful to me.  Maybe it'll be useful to others as well.

## Indexes
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
main record keys they refer to.  More information on how indexes work can be found [here](https://github.com/timshannon/bolthold/issues/36#issuecomment-414720348)

Optionally, you can implement the `Storer` interface, to specify your own indexes, rather than using the `boltHoldIndex`
struct tag.

### Slice Indexes
When you create an index on a slice of items, by default it may not do what you expect.  Consider the following records:

| ID 	| Name 		| Categories 		|
| --- 	| --- 		| --- 				|
| 1 	| John 		| red, green, blue 	|
| 2 	| Bill 		| red, purple	 	|
| 3 	| Jane 		| red, orange	 	|
| 4 	| Brian		| red, purple	 	|


You may expect your `Categories` index to look like the following:

| Categories 	| ID 			| 
| --- 			| --- 			| 
| red			| 1, 2, 3, 4	| 
| green			| 1 			|
| blue			| 1 			|
| purple		| 2, 4 			|
| orange		| 3 			|

But they'll actually look like this:

| Categories 		| ID 			| 
| --- 				| --- 			| 
| red, green, blue	| 1				| 
| red, purple		| 2, 4 			|
| red, orange		| 3 			|


So if you did a query like this:
```Go
bh.Where("Categories").Contains("red").Index("Categories")
```

It'll work, but you'll be reading more records than you'd expect.  You'd only "save reads" on values where the list of 
categories exactly match.

If instead you want to index each individual item in the slice, you can use the struct tag `boltholdSliceIndex`. It
will then individually index each item in the slice, and potentially give you performance benefits if you have a lot
of overlap with individual items in your sliced fields.

Be sure to benchmark both a regular index and a sliced index to see which performs better for your specific dataset.


## Queries
Queries are chain-able constructs that filters out any data that doesn't match it's criteria. An index will be used if
the `.Index()` chain is called, otherwise bolthold won't use any index.

Queries will look like this:
```Go
s.Find(bolthold.Where("FieldName").Eq(value).And("AnotherField").Lt(AnotherValue).Or(bolthold.Where("FieldName").Eq(anotherValue)))

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
* Matches Function 
  * `Where("field").MatchFunc(func(ra *RecordAccess) (bool, error)) // see RecordAccess Type`
  * `Where("field").MatchFunc(func(m *MyType) (bool, error))`
  * `Where("field").MatchFunc(func(field string) (bool, error))`
* Skip - `Where("field").Eq(value).Skip(10)`
* Limit - `Where("field").Eq(value).Limit(10)`
* SortBy - `Where("field").Eq(value).SortBy("field1", "field2")`
* Reverse - `Where("field").Eq(value).SortBy("field").Reverse()`
* Index - `Where("field").Eq(value).Index("indexName")`
* Not - `Where("field").Not().In(val1, val2, val3)`
* Contains - `Where("field").Contains(val1)`
* ContainsAll - `Where("field").Contains(val1, val2, val3)`
* ContainsAny - `Where("field").Contains(val1, val2, val3)`
* HasKey - `Where("field").HasKey(val1) // to test if a Map value has a key`


If you want to run a query's criteria against the Key value, you can use the `bolthold.Key` constant:
```Go

store.Find(&result, bolthold.Where(bolthold.Key).Ne(value))

```

You can access nested structure fields in queries like this:

```Go
type Repo struct {
  Name string
  Contact ContactPerson
}

type ContactPerson struct {
  Name string
}

store.Find(&repo, bolthold.Where("Contact.Name").Eq("some-name")
```

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

Queries can be used in more than just selecting data.  You can delete or update data that matches a query.

Using the example above, if you wanted to remove all of the invalid records where Death < Birth:

```Go

// you must pass in a sample type, so BoltHold knows which bucket to use and what indexes to update
store.DeleteMatching(&Person{}, bolthold.Where("Death").Lt(bolthold.Field("Birth")))

```

Or if you wanted to update all the invalid records to flip/flop the Birth and Death dates:
```Go

store.UpdateMatching(&Person{}, bolthold.Where("Death").Lt(bolthold.Field("Birth")), func(record interface{}) error {
	update, ok := record.(*Person) // record will always be a pointer
	if !ok {
		return fmt.Errorf("Record isn't the correct type!  Wanted Person, got %T", record)
	}

	update.Birth, update.Death = update.Death, update.Birth

	return nil
})
```

If you simply want to count the number of records returned by a query use the `Count` method:
```Go
 // need to pass in empty datatype so bolthold knows what type to count
count, err := store.Count(&Person{}, bolthold.Where("Death").Lt(bolthold.Field("Birth")))
```

### Keys in Structs

A common scenario is to store the bolthold Key in the same struct that is stored in the boltDB value.  You can
automatically populate a record's Key in a struct by using the `boltholdKey` struct tag when running `Find` queries.

Another common scenario is to insert data with an auto-incrementing key assigned by the database.
When performing an `Insert`, if the type of the key matches the type of the `boltholdKey` tagged field,
the data is passed in by reference, **and** the field's current value is the zero-value for that type,
then it is set on the data _before_ insertion.

```Go
type Employee struct {
	ID string `boltholdKey:"ID"`  // the tagName isn't required, but some linters will complain without it
	FirstName string
	LastName string
	Division string
	Hired time.Time
}
```
Bolthold assumes only one of such struct tags exists. If a value already exists in the key field, it will be overwritten.

If you want to insert an auto-incrementing Key you can pass the `bolthold.NextSequence()` func as the Key value.

```Go
err := store.Insert(bolthold.NextSequence(), data)
```

The key value will be a `uint64`.

If you want to know the value of the auto-incrementing Key that was generated using `bolthold.NextSequence()`,
then make sure to pass your data by value and that the `boltholdKey` tagged field is of type `uint64`.

```Go
err := store.Insert(bolthold.NextSequence(), &data)
```


### Slices in Structs and Queries
When querying slice fields in structs you can use the `Contains`, `ContainsAll` and `ContainsAny` criterion.

```Go
val := struct {
    Set []string
}{
    Set: []string{"1", "2", "3"},
}
bh.Where("Set").Contains("1") // true
bh.Where("Set").ContainsAll("1", "3") // true
bh.Where("Set").ContainsAll("1", "3", "4") // false
bh.Where("Set").ContainsAny("1", "7", "4") // true
```

The `In`, `ContainsAll` and `ContainsAny` critierion accept a slice of `interface{}` values.  This means you can build
your queries by passing in your values as arguments:
```
where := bolthold.Where("Id").In("1", "2", "3")
```

However if you have an existing slice of values to test against, you can't pass in that slice because it is not of type
`[]interface{}`.

```Go
t := []string{"1", "2", "3", "4"}
where := bolthold.Where("Id").In(t...) // compile error
```

Instead you need to copy your slice into another slice of empty interfaces:
```Go
t := []string{"1", "2", "3", "4"}
s := make([]interface{}, len(t))
for i, v := range t {
    s[i] = v
}
where := bolthold.Where("Id").In(s...)
```

You can use the helper function `bolthold.Slice` which does exactly that.
```Go
t := []string{"1", "2", "3", "4"}
where := bolthold.Where("Id").In(bolthold.Slice(t)...)

```

### ForEach

When working with large datasets, you may not want to have to store the entire dataset in memory.  It's be much more
efficient to work with a single record at a time rather than grab all the records and loop through them, which is
what cursors are used for in databases.  In BoltHold you can accomplish the same thing by calling ForEach:

```Go
err := store.ForEach(boltholdWhere("Id").Gt(4), func(record *Item) error {
	// do stuff with record

	// if you return an error, then the query will stop iterating through records

	return nil
})

```

### Aggregate Queries

Aggregate queries are queries that group results by a field.  For example, lets say you had a collection of employees:

```Go
type Employee struct {
	FirstName string
	LastName string
	Division string
	Hired time.Time
}
```

And you wanted to find the most senior (first hired) employee in each division:

```Go

result, err := store.FindAggregate(&Employee{}, nil, "Division") //nil query matches against all records
```

This will return a slice of `Aggregate Result` from which you can extract your groups and find Min, Max, Avg, Count,
etc.

```Go
for i := range result {
	var division string
	employee := &Employee{}

	result[i].Group(&division)
	result[i].Min("Hired", employee)

	fmt.Printf("The most senior employee in the %s division is %s.\n",
		division, employee.FirstName + " " + employee.LastName)
}
```

Aggregate queries become especially powerful when combined with the sub-querying capability of `MatchFunc`.


Many more examples of queries can be found in the [find_test.go](https://github.com/timshannon/bolthold/blob/master/find_test.go)
file in this repository.

## Comparing

Just like with Go, types must be the same in order to be compared with each other.  You cannot compare an int to a int32.
The built-in Go comparable types (ints, floats, strings, etc) will work as expected.  Other types from the standard library
can also be compared such as `time.Time`, `big.Rat`, `big.Int`, and `big.Float`.  If there are other standard library
types that I missed, let me know.

You can compare any custom type either by using the `MatchFunc` criteria, or by satisfying the `Comparer` interface with
your type by adding the Compare method: `Compare(other interface{}) (int, error)`.

If a type doesn't have a predefined comparer, and doesn't satisfy the Comparer interface, then the types value is converted
to a string and compared lexicographically.

## Behavior Changes
Since BoltHold is a higher level interface than BoltDB, there are some added helpers.  Instead of *Put*, you
have the options of:
* *Insert* - Fails if key already exists.
* *Update* - Fails if key doesn't exist `ErrNotFound`.
* *Upsert* - If key doesn't exist, it inserts the data, otherwise it updates the existing record.

When getting data instead of returning `nil` if a value doesn't exist, BoltHold returns `bolthold.ErrNotFound`, and
similarly when deleting data, instead of silently continuing if a value isn't found to delete, BoltHold returns
`bolthold.ErrNotFound`.  The exception to this is when using query based functions such as `Find` (returns an empty slice),
`DeleteMatching` and `UpdateMatching` where no error is returned.


## When should I use BoltHold?
BoltHold will be useful in the same scenarios where BoltDB is useful, with the added benefit of being able to retire
some of your data filtering code and possibly improved performance.

You can also use it instead of SQLite for many scenarios.  BoltHold's main benefit over SQLite is its simplicity when
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

Bolthold currently has over 80% coverage in unit tests, and it's backed by BoltDB which is a very solid and well built
piece of software, so I encourage you to give it a try.

If you end up using BoltHold, I'd love to hear about it.
