#GobStore

GobStore is a simple querying and indexing interface on top of a Bolt DB instance. The goal is to create a simple,
higher level interface on top of Bolt DB that applies some sane defaults, but exposes the underlying Bolt DB for customizing
as you wish.

## Why not just use Bolt DB directly?
I love BoltDB, and I've used it in several projects.  However I find myself writing the same code over and over again,
for encoding and decoding objects and searching through data.  I figure formalizing how I've been using BoltDB 
and including tests and benchmarks will, at a minimum, be useful to me.  Maybe it'll be useful to others as well.

##Indexes
In every GobStore there will be a reserved bucket *_indexes* which will be used to hold indexes that point back to another
bucket's Key system.  Indexes will be defined as functions which will return a GoType and be run against every existing 
row in a given Bucket, or simple as struct tags defining a particular field as indexable. 

## Queries
Queries will be chainable constructs that apply to the dataset in the order they are chained.  Indexes will be used in 
queries only if explicitly specified.  There will be no "query optimiser".


## Bucket Layout
One Go Type will have one bucket, and multiple index buckets.

## Behavior Changes
Since this will be a higher level interface, there will also be some helper functions.  Instead of *Put*, you'll have the
option of *Insert* (fails if key already exists), *Update* (fails if key doesn't exist), and *Upsert* (if key doesn't
exist, it inserts the data).

