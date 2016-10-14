package gobstore

import "unicode"

const (
	eq = iota //==
	ne        // !=
	gt        // >
	lt        // <
	ge        // >=
	le        // <=
)

// Key is shorthand for specifying a query to run again the Key in a gobstore, simply returns ""
// Where(Key()).Eq("testkey")
func Key() string {
	return ""
}

// Query is a chained collection of criteria of which an object in the gobstore needs to match to be returned
type Query struct {
	currentField  string
	fieldCriteria map[string][]*Criterion
	ors           []*Query
}

// Criterion is an operator and a value that a given field needs to match on
type Criterion struct {
	query    *Query
	operator int
	value    interface{}
}

// Where starts a query for specifying the criteria that an object in the gobstore needs to match to
// be returned in a Find result
/*
	Query API Example

	s.Find(Where("Name").Eq("Tim Shannon").And("DOB").Lt(time.Now()).
		Or(Where("Title").Eq("Boss").And("DOB").Lt(time.Now())))


	Since Gobs only encode exported fields, this will panic if you pass in a field with a lower case first letter
*/
func Where(field string) *Criterion {
	if !startsUpper(field) {
		panic("The first letter of a field in a gobstore query must be upper-case")
	}
	return &Criterion{
		query: &Query{
			currentField:  field,
			fieldCriteria: make(map[string][]*Criterion),
		},
	}
}

func startsUpper(str string) bool {
	for _, r := range str {
		return unicode.IsUpper(r)
	}

	return false
}

// And creates a nother set of criterion the needs to apply to a query
func (q *Query) And(field string) *Criterion {
	if !startsUpper(field) {
		panic("The first letter of a field in a gobstore query must be upper-case")
	}

	q.currentField = field
	return &Criterion{
		query: q,
	}
}

// Or creates another separate query that gets unioned with any other results in the query
func (q *Query) Or(query *Query) *Query {
	q.ors = append(q.ors, query)
	return q
}

func (c *Criterion) op(op int, value interface{}) *Query {
	c.operator = op
	c.value = value

	q := c.query
	q.fieldCriteria[q.currentField] = append(q.fieldCriteria[q.currentField], c)

	return q
}

// Eq tests if the current field is Equal to the passed in value
func (c *Criterion) Eq(value interface{}) *Query {
	return c.op(eq, value)
}

// Ne test if the current field is Not Equal to the passed in value
func (c *Criterion) Ne(value interface{}) *Query {
	return c.op(ne, value)
}

// Gt test if the current field is Greater Than the passed in value
func (c *Criterion) Gt(value interface{}) *Query {
	return c.op(gt, value)
}

// Lt test if the current field is Less Than the passed in value
func (c *Criterion) Lt(value interface{}) *Query {
	return c.op(lt, value)
}

// Ge test if the current field is Greater Than or Equal To the passed in value
func (c *Criterion) Ge(value interface{}) *Query {
	return c.op(ge, value)
}

// Le test if the current field is Less Than or Equal To the passed in value
func (c *Criterion) Le(value interface{}) *Query {
	return c.op(le, value)
}
