#GobStore

GobStore is a simple querying interface on top of a Bolt DB instance.

## Why not just use Bolt DB directly?
I love BoltDB, and I've used it in several projects.  However I find myself writing the same code over and over again,
for encoding and decoding objects and looping through searching data.  I figure formalizing how I've been using BoltDB 
and including tests and benchmarks will at a minimum be useful to me.  Maybe it'll be useful to others as well.

##Indexes
In every GobStore there will be a reserved bucket *_indexes* which will be used to hold indexes that point back to another
bucket's Key system.  Indexes will be defined as functions which will return a GoType and be run against every existing 
row in a given Bucket.


## Queries
Queries will be chainable constructs that apply to the dataset in the order they are chained.  There will be 2 types of
queries.  Key queries (includes indexes) and value queries. 

