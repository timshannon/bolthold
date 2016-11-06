#BoltHold [![Build Status](https://travis-ci.org/timshannon/bolthold.svg?branch=master)](https://travis-ci.org/timshannon/bolthold) [![GoDoc](https://godoc.org/github.com/timshannon/bolthold?status.svg)](https://godoc.org/github.com/timshannon/bolthold) [![Coverage Status](https://coveralls.io/repos/github/timshannon/bolthold/badge.svg?branch=master)](https://coveralls.io/github/timshannon/bolthold?branch=master) [![Go Report Card](https://goreportcard.com/badge/github.com/timshannon/bolthold)](https://goreportcard.com/report/github.com/timshannon/bolthold)


BoltHold is a simple querying and indexing layer on top of a Bolt DB instance. The goal is to create a simple,
higher level interface on top of Bolt DB that simplifies dealing with Go Types and finding data, but exposes the underlying Bolt DB for customizing as you wish.  By default the encoding used is Gob, so feel free to *gob.Register* any types you wish, or use the 
GobEncoder/Decoder interface for faster serialization.  Or you can use any serialization you want by supplying encode / decode funcs
to the Options struct on Open.

## Why not just use Bolt DB directly?
I love BoltDB, and I've used it in several projects.  However, I find myself writing the same code over and over again,
for encoding and decoding objects and searching through data.  I figure formalizing how I've been using BoltDB 
and including tests and benchmarks will, at a minimum, be useful to me.  Maybe it'll be useful to others as well.

##Indexes
In every BoltHold there will be a reserved bucket *_indexes* which will be used to hold indexes that point back to another
bucket's Key system.  Indexes will be defined as functions which will return a GoType and be run against every existing 
row in a given Bucket, or simple as struct tags defining a particular field as indexable. 

## Queries
Queries will be chain-able constructs that apply to the dataset in the order they are chained. There will be no "query optimiser".
The first index hit will be the one used.

Queries will look like this:
```
s.Find(Where("Name").Eq("Tim Shannon").And("DOB").Lt(time.Now()).Or(Where("Title").Eq("Boss").And("DOB").Lt(time.Now())))

```

## Comparing

## Bucket Layout
One Go Type will have one bucket, and multiple index buckets.  You can skip all of reflect calls by implementing the 
*Storer* interface.
You can query custom types by implementing the Comparer interface on them

## Behavior Changes
Since this will be a higher level interface, there will also be some helper functions.  Instead of *Put*, you'll have the
option of *Insert* (fails if key already exists), *Update* (fails if key doesn't exist), and *Upsert* (if key doesn't
exist, it inserts the data).

## When should I use BoltHold?

