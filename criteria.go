// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"unicode"
)

const (
	eq    = iota //==
	ne           // !=
	gt           // >
	lt           // <
	ge           // >=
	le           // <=
	in           // in
	re           // regular expression
	fn           // func
	isnil        // test's for nil
	hk           // match map keys

	contains // slice only
	any      // slice only
	all      // slice only
)

// Key is shorthand for specifying a query to run again the Key in a bolthold, simply returns ""
// Where(bolthold.Key).Eq("testkey")
const Key = ""

// BoltholdKeyTag is the struct tag used to define an a field as a key for use in a Find query
const BoltholdKeyTag = "boltholdKey"

// Query is a chained collection of criteria of which an object in the bolthold needs to match to be returned
// an empty query matches against all records
type Query struct {
	index         string
	currentField  string
	fieldCriteria map[string][]*Criterion
	ors           []*Query

	badIndex bool
	dataType reflect.Type
	source   BucketSource

	limit   int
	skip    int
	sort    []string
	reverse bool
}

// IsEmpty returns true if the query is an empty query
// an empty query matches against everything
func (q *Query) IsEmpty() bool {
	if q.index != "" {
		return false
	}
	if len(q.fieldCriteria) != 0 {
		return false
	}

	if q.ors != nil {
		return false
	}

	return true
}

// Criterion is an operator and a value that a given field needs to match on
type Criterion struct {
	query    *Query
	operator int
	value    interface{}
	values   []interface{}
	negate   bool
}

func hasMatchFunc(criteria []*Criterion) bool {
	for _, c := range criteria {
		if c.operator == fn {
			return true
		}
	}
	return false
}

// Slice turns a slice of any time into []interface{} by copying the slice values so it can be easily passed
// into queries that accept variadic parameters.
// Will panic if value is not a slice
func Slice(value interface{}) []interface{} {
	slc := reflect.ValueOf(value)

	s := make([]interface{}, slc.Len(), slc.Len()) // panics if value is not slice, array or map
	for i := range s {
		s[i] = slc.Index(i).Interface()
	}
	return s
}

// Field allows for referencing a field in structure being compared
type Field string

// Where starts a query for specifying the criteria that an object in the bolthold needs to match to
// be returned in a Find result
/*
Query API Example

	s.Find(bolthold.Where("FieldName").Eq(value).And("AnotherField").Lt(AnotherValue).Or(bolthold.Where("FieldName").Eq(anotherValue)

Since Gobs only encode exported fields, this will panic if you pass in a field with a lower case first letter
*/
func Where(field string) *Criterion {
	if !startsUpper(field) {
		panic("The first letter of a field in a bolthold query must be upper-case")
	}

	return &Criterion{
		query: &Query{
			currentField:  field,
			fieldCriteria: make(map[string][]*Criterion),
		},
	}
}

// And creates a nother set of criterion the needs to apply to a query
func (q *Query) And(field string) *Criterion {
	if !startsUpper(field) {
		panic("The first letter of a field in a bolthold query must be upper-case")
	}

	if q.fieldCriteria == nil {
		q.fieldCriteria = make(map[string][]*Criterion)
	}

	q.currentField = field
	return &Criterion{
		query: q,
	}
}

// Skip skips the number of records that match all the rest of the query criteria, and does not return them
// in the result set.  Setting skip multiple times, or to a negative value will panic
func (q *Query) Skip(amount int) *Query {
	if amount < 0 {
		panic("Skip must be set to a positive number")
	}

	if q.skip != 0 {
		panic(fmt.Sprintf("Skip has already been set to %d", q.skip))
	}

	q.skip = amount

	return q
}

// Limit sets the maximum number of records that can be returned by a query
// Setting Limit multiple times, or to a negative value will panic
func (q *Query) Limit(amount int) *Query {
	if amount < 0 {
		panic("Limit must be set to a positive number")
	}

	if q.limit != 0 {
		panic(fmt.Sprintf("Limit has already been set to %d", q.limit))
	}

	q.limit = amount

	return q
}

// SortBy sorts the results by the given fields name
// Multiple fields can be used
func (q *Query) SortBy(fields ...string) *Query {
	for i := range fields {
		if fields[i] == Key {
			panic("Cannot sort by Key.")
		}
		found := false
		for k := range q.sort {
			if q.sort[k] == fields[i] {
				found = true
				break
			}
		}
		if !found {
			q.sort = append(q.sort, fields[i])
		}
	}
	return q
}

// Reverse will reverse the current result set
// useful with SortBy
func (q *Query) Reverse() *Query {
	q.reverse = !q.reverse
	return q
}

// Index specifies the index to use when running this query
func (q *Query) Index(indexName string) *Query {
	q.index = indexName
	return q
}

// Or creates another separate query that gets unioned with any other results in the query
// Or will panic if the query passed in contains a limit or skip value, as they are only
// allowed on top level queries
func (q *Query) Or(query *Query) *Query {
	if query.skip != 0 || query.limit != 0 {
		panic("Or'd queries cannot contain skip or limit values")
	}
	q.ors = append(q.ors, query)
	return q
}

func (q *Query) matchesAllFields(s *Store, key []byte, value reflect.Value, currentRow interface{}) (bool, error) {
	if q.IsEmpty() {
		return true, nil
	}

	for field, criteria := range q.fieldCriteria {
		if field == q.index && !q.badIndex {
			// already handled by index Iterator
			continue
		}

		if field == Key {
			ok, err := matchesAllCriteria(s, criteria, key, true, currentRow)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}

			continue
		}

		fVal, err := fieldValue(value, field)
		if err != nil {
			return false, err
		}

		ok, err := matchesAllCriteria(s, criteria, fVal, false, currentRow)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}

	return true, nil
}

func fieldValue(value reflect.Value, field string) (interface{}, error) {
	current := value

	if current.Kind() == reflect.Ptr {
		if current.IsNil() {
			return reflect.Value{}, nil
		}
		current = current.Elem()
	}

	if field == "" {
		if !value.IsValid() {
			return reflect.Value{}, fmt.Errorf("The field %s does not exist in the type %s", field,
				value.Interface())
		}
		return value.Interface(), nil
	}

	split := strings.SplitN(field, ".", 2)

	currentField := split[0]
	remainder := ""
	if len(split) != 1 {
		remainder = split[1]
	}

	typ := current.Type()
	f, ok := typ.FieldByNameFunc(func(name string) bool {
		return name == currentField
	})

	if !ok {
		return reflect.Value{}, fmt.Errorf("The field %s does not exist in the type %s", field,
			value.Interface())
	}

	// test is any fields in this index chain are anonymous and nil
	v := current
	for _, index := range f.Index {
		vField := v.Field(index)
		if vField.Kind() == reflect.Ptr {
			if vField.IsNil() {
				return reflect.Value{}, nil
			}
			vField = vField.Elem()
		}
		v = vField
	}

	return fieldValue(current.FieldByIndex(f.Index), remainder)

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

// In test if the current field is a member of the slice of values passed in
func (c *Criterion) In(values ...interface{}) *Query {
	c.operator = in
	c.values = values

	q := c.query
	q.fieldCriteria[q.currentField] = append(q.fieldCriteria[q.currentField], c)

	return q
}

// HasKey tests if the field has a map key matching the passed in value
func (c *Criterion) HasKey(value interface{}) *Query {
	return c.op(hk, value)
}

// RegExp will test if a field matches against the regular expression
// The Field Value will be converted to string (%s) before testing
func (c *Criterion) RegExp(expression *regexp.Regexp) *Query {
	return c.op(re, expression)
}

// IsNil will test if a field is equal to nil
func (c *Criterion) IsNil() *Query {
	return c.op(isnil, nil)
}

// Not will negate the following critierion
func (c *Criterion) Not() *Criterion {
	c.negate = !c.negate
	return c
}

// Contains tests if the current field is a slice that contains the passed in value
func (c *Criterion) Contains(value interface{}) *Query {
	return c.op(contains, value)
}

// ContainsAll tests if the current field is a slice that contains all of the passed in values.  If any of the
// values are NOT contained in the slice, then no match is made
func (c *Criterion) ContainsAll(values ...interface{}) *Query {
	c.operator = all
	c.values = values

	q := c.query
	q.fieldCriteria[q.currentField] = append(q.fieldCriteria[q.currentField], c)

	return q
}

// ContainsAny tests if the current field is a slice that contains any of the passed in values.  If any of the
// values are contained in the slice, then a match is made
func (c *Criterion) ContainsAny(values ...interface{}) *Query {
	c.operator = any
	c.values = values

	q := c.query
	q.fieldCriteria[q.currentField] = append(q.fieldCriteria[q.currentField], c)

	return q
}

// MatchFunc is a function used to test an arbitrary matching value in a query
type MatchFunc func(ra interface{}) (bool, error)

// RecordAccess allows access to the current record, field or allows running a subquery within a
// MatchFunc
type RecordAccess struct {
	source BucketSource
	s      *Store
	record interface{}
	field  interface{}
}

// Field is the current field being queried
func (r *RecordAccess) Field() interface{} {
	return r.field
}

// Record is the complete record for a given row in bolthold
func (r *RecordAccess) Record() interface{} {
	return r.record
}

// SubQuery allows you to run another query in the same transaction for each
// record in a parent query
func (r *RecordAccess) SubQuery(result interface{}, query *Query) error {
	return r.s.findQuery(r.source, result, query)
}

// SubAggregateQuery allows you to run another aggregate query in the same transaction for each
// record in a parent query
func (r *RecordAccess) SubAggregateQuery(query *Query, groupBy ...string) ([]*AggregateResult, error) {
	return r.s.aggregateQuery(r.source, r.record, query, groupBy...)
}

// MatchFunc will test if a field matches the passed in function
func (c *Criterion) MatchFunc(match interface{}) *Query {
	if c.query.currentField == Key {
		panic("Match func cannot be used against Keys, as the Key type is unknown at runtime, and there is no value compare against")
	}

	return c.op(fn, match)
}

// test if the criterion passes with the passed in value
func (c *Criterion) test(s *Store, testValue interface{}, encoded bool, currentRow interface{}) (bool, error) {
	var recordValue interface{}
	if encoded {
		if len(testValue.([]byte)) != 0 {
			// used with keys
			if c.operator == in || c.operator == any || c.operator == all {
				// value is a slice of values, use c.values
				recordValue = reflect.New(reflect.TypeOf(c.values[0])).Interface()
			} else {
				recordValue = reflect.New(reflect.TypeOf(c.value)).Interface()
			}
			err := s.decode(testValue.([]byte), recordValue)
			if err != nil {
				return false, err
			}
		}

	} else {
		recordValue = testValue
	}

	switch c.operator {
	case in:
		for i := range c.values {
			result, err := c.compare(recordValue, c.values[i], currentRow)
			if err != nil {
				return false, err
			}
			if result == 0 {
				return true, nil
			}
		}

		return false, nil
	case hk:
		v := reflect.ValueOf(recordValue).MapIndex(reflect.ValueOf(c.value))
		return !reflect.ValueOf(v).IsZero(), nil
	case re:
		return c.value.(*regexp.Regexp).Match([]byte(fmt.Sprintf("%s", recordValue))), nil
	case fn:
		fnVal := reflect.ValueOf(c.value)
		fnType := reflect.TypeOf(c.value)
		ra := &RecordAccess{
			s:      s,
			field:  recordValue,
			record: currentRow,
			source: c.query.source,
		}

		var out []reflect.Value

		if fnType.In(0) == reflect.TypeOf(ra) {
			out = fnVal.Call([]reflect.Value{reflect.ValueOf(ra)})
		}

		if fnType.In(0) == reflect.TypeOf(ra.field) {
			out = fnVal.Call([]reflect.Value{reflect.ValueOf(ra.field)})
		}
		if fnType.In(0) == reflect.TypeOf(ra.record) {
			out = fnVal.Call([]reflect.Value{reflect.ValueOf(ra.record)})
		}

		if len(out) != 2 {
			return false, fmt.Errorf("MatchFunc does not return (bool, error)")
		}

		if out[1].IsNil() {
			return out[0].Interface().(bool), nil
		}
		return false, out[1].Interface().(error)
	case isnil:
		return reflect.ValueOf(recordValue).IsNil(), nil
	case contains, any, all:
		slc := reflect.ValueOf(recordValue)
		kind := slc.Kind()
		if kind != reflect.Slice && kind != reflect.Array {
			// make slice containing recordValue
			for slc.Kind() == reflect.Ptr {
				slc = slc.Elem()
			}
			slc = reflect.Append(reflect.MakeSlice(reflect.SliceOf(slc.Type()), 0, 1), slc)
		}

		if c.operator == contains {
			for i := 0; i < slc.Len(); i++ {
				result, err := c.compare(slc.Index(i), c.value, currentRow)
				if err != nil {
					return false, err
				}
				if result == 0 {
					return true, nil
				}
			}
			return false, nil
		}

		if c.operator == any {
			for i := 0; i < slc.Len(); i++ {
				for k := range c.values {
					result, err := c.compare(slc.Index(i), c.values[k], currentRow)
					if err != nil {
						return false, err
					}
					if result == 0 {
						return true, nil
					}
				}
			}

			return false, nil
		}

		// c.operator == all {
		for k := range c.values {
			found := false
			for i := 0; i < slc.Len(); i++ {
				result, err := c.compare(slc.Index(i), c.values[k], currentRow)
				if err != nil {
					return false, err
				}
				if result == 0 {
					found = true
					break
				}
			}
			if !found {
				return false, nil
			}
		}

		return true, nil

	default:
		//comparison operators
		result, err := c.compare(recordValue, c.value, currentRow)
		if err != nil {
			return false, err
		}

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
}

func matchesAllCriteria(s *Store, criteria []*Criterion, value interface{}, encoded bool,
	currentRow interface{}) (bool, error) {
	for i := range criteria {
		ok, err := criteria[i].test(s, value, encoded, currentRow)
		if err != nil {
			return false, err
		}

		if criteria[i].negate == ok {
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
			s += field + " " + criteria[i].String()
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

func (c *Criterion) String() string {
	s := ""
	if c.negate {
		s += "NOT "
	}
	switch c.operator {
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
	case in:
		return "in " + fmt.Sprintf("%v", c.values)
	case re:
		s += "matches the regular expression"
	case fn:
		s += "matches the function"
	case isnil:
		return "is nil"
	case contains:
		s += "contains"
	case any:
		return "contains any of " + fmt.Sprintf("%v", c.values)
	case all:
		return "contains all of " + fmt.Sprintf("%v", c.values)
	default:
		panic("invalid operator")
	}
	return s + " " + fmt.Sprintf("%v", c.value)
}
