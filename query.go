// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package gobstore

import (
	"bytes"
	"fmt"
	"reflect"
	"unicode"
)

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
	index         string
	currentField  string
	fieldCriteria map[string][]*Criterion
	ors           []*Query
	badIndex      bool
}

// Criterion is an operator and a value that a given field needs to match on
type Criterion struct {
	query        *Query
	operator     int
	value        interface{}
	valueEncoded []byte
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
			index:         field,
			currentField:  field,
			fieldCriteria: make(map[string][]*Criterion),
		},
	}
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

func (q *Query) matchesAllFields(key []byte, value reflect.Value) (bool, error) {
	for field, criteria := range q.fieldCriteria {
		if field == q.index && !q.badIndex {
			// already handled by index Iterator
			continue
		}

		if field == Key() {
			ok, err := matchesAllCriteria(criteria, key)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}

			continue
		}

		fVal := value.Elem().FieldByName(field)
		if !fVal.IsValid() {
			return false, fmt.Errorf("The field %s does not exist in the type %s", field, value)
		}
		fBts, err := encode(fVal.Interface())
		if err != nil {
			return false, err
		}
		ok, err := matchesAllCriteria(criteria, fBts)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}

	return true, nil
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

// test if the criterion passes with the passed in value
func (c *Criterion) test(value []byte) (bool, error) {
	if c.valueEncoded == nil {
		d, err := encode(c.value)
		if err != nil {
			return false, err
		}

		c.valueEncoded = d
	}

	result := bytes.Compare(value, c.valueEncoded)
	switch c.operator {
	case eq:
		return result == 0, nil
	case ne:
		return result != 0, nil
	case gt:
		return result > 0, nil
	case lt:
		return result < 0, nil
	case le:
		return result < 0 || result == 0, nil
	case ge:
		return result > 0 || result == 0, nil
	default:
		panic("invalid operator")
	}
}

func matchesAllCriteria(criteria []*Criterion, value []byte) (bool, error) {
	for i := range criteria {
		ok, err := criteria[i].test(value)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}

	return true, nil
}

func startsUpper(str string) bool {
	if str == "" {
		return true
	}

	for _, r := range str {
		return unicode.IsUpper(r)
	}

	return false
}

func (q *Query) String() string {
	s := ""

	if q.index != "" {
		s += "Using Index [" + q.index + "] "
	}

	s += "Where "
	for field, criteria := range q.fieldCriteria {
		for i := range criteria {
			s += field + " "
			switch criteria[i].operator {
			case eq:
				s += "=="
			case ne:
				s += "!="
			case gt:
				s += ">"
			case lt:
				s += "<"
			case le:
				s += "<="
			case ge:
				s += ">="
			default:
				panic("invalid operator")
			}
			s += " " + fmt.Sprintf("%v", criteria[i].value)
			s += "\n\tAND "
		}
	}

	// remove last AND
	s = s[:len(s)-6]

	for i := range q.ors {
		s += "\nOr " + q.ors[i].String()
	}

	return s
}
