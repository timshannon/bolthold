// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold

import (
	"fmt"
	"reflect"
	"regexp"
	"unicode"

	"github.com/boltdb/bolt"
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
)

// Key is shorthand for specifying a query to run again the Key in a bolthold, simply returns ""
// Where(bolthold.Key).Eq("testkey")
const Key = ""

// Query is a chained collection of criteria of which an object in the bolthold needs to match to be returned
// an empty query matches against all records
type Query struct {
	index         string
	currentField  string
	fieldCriteria map[string][]*Criterion
	ors           []*Query
	badIndex      bool
	currentRow    interface{}

	limit int64
	skip  int64
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
	inValues []interface{}
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
			index:         field,
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

	q.currentField = field
	return &Criterion{
		query: q,
	}
}

// Skip skips the number of records that match all the rest of the query criteria, and does not return them
// in the result set.  Setting skip multiple times, or to a negative value will panic
func (q *Query) Skip(amount int64) *Query {
	if amount < 0 {
		panic("Skip must be set to a postive number")
	}

	if q.skip != 0 {
		panic(fmt.Sprintf("Skip has already been set to %d", q.skip))
	}

	q.skip = amount

	return q
}

// Limit sets the maximum number of records that can be returned by a query
// Setting Limit multiple times, or to a negative value will panic
func (q *Query) Limit(amount int64) *Query {
	if amount < 0 {
		panic("Limit must be set to a postive number")
	}

	if q.limit != 0 {
		panic(fmt.Sprintf("Limit has already been set to %d", q.limit))
	}

	q.limit = amount

	return q
}

// Or creates another separate query that gets unioned with any other results in the query
// Or will panic if the query passed in contains a limit or skip value, as they are only
// allowed on top level queries
func (q *Query) Or(query *Query) *Query {
	if q.skip != 0 || q.limit != 0 {
		panic("Or'd queries cannot contain skip or limit values")
	}
	q.ors = append(q.ors, query)
	return q
}

func (q *Query) matchesAllFields(key []byte, value reflect.Value) (bool, error) {
	if q.IsEmpty() {
		return true, nil
	}

	for field, criteria := range q.fieldCriteria {
		if field == q.index && !q.badIndex {
			// already handled by index Iterator
			continue
		}

		if field == Key {
			ok, err := matchesAllCriteria(criteria, key, true)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}

			continue
		}

		//TODO: Allow deep names. struct1.field1.fieldChild
		fVal := value.Elem().FieldByName(field)
		if !fVal.IsValid() {
			return false, fmt.Errorf("The field %s does not exist in the type %s", field, value)
		}

		ok, err := matchesAllCriteria(criteria, fVal.Interface(), false)
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

// In test if the current field is a member of the slice of values passed in
func (c *Criterion) In(values ...interface{}) *Query {
	c.operator = in
	c.inValues = values

	q := c.query
	q.fieldCriteria[q.currentField] = append(q.fieldCriteria[q.currentField], c)

	return q
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

// MatchFunc is a function used to test an arbitrary matching value in a query
type MatchFunc func(field interface{}) (bool, error)

// MatchFunc will test if a field matches the passed in function
func (c *Criterion) MatchFunc(match MatchFunc) *Query {
	return c.op(fn, match)
}

// test if the criterion passes with the passed in value
func (c *Criterion) test(testValue interface{}, encoded bool) (bool, error) {
	var value interface{}
	if encoded {
		if len(testValue.([]byte)) != 0 {
			// used with keys
			value = reflect.New(reflect.TypeOf(c.value)).Interface()
			err := decode(testValue.([]byte), value)
			if err != nil {
				return false, err
			}
		}

	} else {
		value = testValue
	}

	switch c.operator {
	case in:
		for i := range c.inValues {
			result, err := c.compare(value, c.inValues[i])
			if err != nil {
				return false, err
			}
			if result == 0 {
				return true, nil
			}
		}

		return false, nil
	case re:
		return c.value.(*regexp.Regexp).Match([]byte(fmt.Sprintf("%s", value))), nil
	case fn:
		return c.value.(MatchFunc)(value)
	case isnil:
		return reflect.ValueOf(value).IsNil(), nil
	default:
		//comparison operators
		result, err := c.compare(value, c.value)
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

func matchesAllCriteria(criteria []*Criterion, value interface{}, encoded bool) (bool, error) {
	for i := range criteria {
		ok, err := criteria[i].test(value, encoded)
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
		return "in " + fmt.Sprintf("%v", c.inValues)
	case re:
		s += "matches the regular expression"
	case fn:
		s += "matches the function"
	case isnil:
		return "is nil"
	default:
		panic("invalid operator")
	}
	return s + " " + fmt.Sprintf("%v", c.value)
}

func runQuery(tx *bolt.Tx, result interface{}, query *Query, retrievedKeys keyList, skip int64) error {
	if query == nil {
		query = &Query{}
	}
	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr || resultVal.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}

	sliceVal := resultVal.Elem()

	elType := sliceVal.Type().Elem()

	// preserve original type
	oType := elType

	for elType.Kind() == reflect.Ptr {
		elType = elType.Elem()
	}

	iter := newIterator(tx, newStorer(reflect.New(elType).Interface()).Type(), query)

	newKeys := make(keyList, 0)

	//limit := query.limit
	//if limit != 0 {
	//limit = query.limit - int64(len(retrievedKeys))
	//}

	for k, v := iter.Next(); k != nil; k, v = iter.Next() {

		if len(retrievedKeys) != 0 {
			// don't check this record if it's already been retrieved
			if retrievedKeys.in(k) {
				continue
			}
		}

		val := reflect.New(elType)

		err := decode(v, val.Interface())
		if err != nil {
			return err
		}

		query.currentRow = val.Interface()

		ok, err := query.matchesAllFields(k, val)
		if err != nil {
			return err
		}

		if ok {
			if skip > 0 {
				skip--
				continue
			}

			// add to result
			if oType.Kind() == reflect.Ptr {
				sliceVal = reflect.Append(sliceVal, val)
			} else {
				sliceVal = reflect.Append(sliceVal, val.Elem())
			}
			// track that this key's entry has been added to the result list
			newKeys.add(k)
		}

		//if query.limit != 0 {
		//if limit == 0 {
		//break
		//}
		//limit--
		//}
	}

	if iter.Error() != nil {
		return iter.Error()
	}

	resultVal.Elem().Set(sliceVal.Slice(0, sliceVal.Len()))

	if len(query.ors) > 0 { //&& limit != 0{
		for i := range newKeys {
			retrievedKeys.add(newKeys[i])
		}

		for i := range query.ors {
			err := runQuery(tx, result, query.ors[i], retrievedKeys, skip)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func deleteQuery(tx *bolt.Tx, dataType interface{}, query *Query, deletedKeys keyList, skip int64) error {
	if query == nil {
		query = &Query{}
	}
	storer := newStorer(dataType)

	for reflect.TypeOf(dataType).Kind() == reflect.Ptr {
		dataType = reflect.ValueOf(dataType).Elem().Interface()
	}

	iter := newIterator(tx, storer.Type(), query)

	newKeys := make(keyList, 0)

	for k, v := iter.Next(); k != nil; k, v = iter.Next() {

		if len(deletedKeys) != 0 {
			// don't check this record if it's already been deleted
			if deletedKeys.in(k) {
				continue
			}
		}

		val := reflect.New(reflect.TypeOf(dataType))

		err := decode(v, val.Interface())
		if err != nil {
			return err
		}

		query.currentRow = val.Interface()

		ok, err := query.matchesAllFields(k, val)
		if err != nil {
			return err
		}

		if ok {
			if skip > 0 {
				skip--
				continue
			}
			b := tx.Bucket([]byte(storer.Type()))
			err = b.Delete(k)
			if err != nil {
				return err
			}

			// remove any indexes
			err = indexDelete(storer, tx, k, val.Interface())
			if err != nil {
				return err
			}

			newKeys.add(k)
		}
	}

	if iter.Error() != nil {
		return iter.Error()
	}

	if len(query.ors) > 0 {
		for i := range newKeys {
			deletedKeys.add(newKeys[i])
		}

		for i := range query.ors {
			err := deleteQuery(tx, dataType, query.ors[i], deletedKeys, skip)
			if err != nil {
				return err
			}
		}
	}

	return nil

}

func updateQuery(tx *bolt.Tx, dataType interface{}, query *Query, update func(record interface{}) error,
	updatedKeys keyList, skip int64) error {
	if query == nil {
		query = &Query{}
	}
	storer := newStorer(dataType)

	for reflect.TypeOf(dataType).Kind() == reflect.Ptr {
		dataType = reflect.ValueOf(dataType).Elem().Interface()
	}

	iter := newIterator(tx, storer.Type(), query)

	newKeys := make(keyList, 0)

	for k, v := iter.Next(); k != nil; k, v = iter.Next() {

		if len(updatedKeys) != 0 {
			// don't check this record if it's already been updated
			if updatedKeys.in(k) {
				continue
			}
		}

		val := reflect.New(reflect.TypeOf(dataType))

		err := decode(v, val.Interface())
		if err != nil {
			return err
		}

		query.currentRow = val.Interface()

		ok, err := query.matchesAllFields(k, val)
		if err != nil {
			return err
		}

		if ok {
			if skip > 0 {
				skip--
				continue
			}

			upVal := val.Interface()
			err = update(upVal)
			if err != nil {
				return err
			}

			encVal, err := encode(upVal)
			if err != nil {
				return err
			}

			b := tx.Bucket([]byte(storer.Type()))
			err = b.Put(k, encVal)
			if err != nil {
				return err
			}

			// delete any existing indexes
			err = indexDelete(storer, tx, k, upVal)
			if err != nil {
				return err
			}
			// insert any new indexes
			err = indexAdd(storer, tx, k, upVal)
			if err != nil {
				return err
			}

			newKeys.add(k)
		}
	}

	if iter.Error() != nil {
		return iter.Error()
	}

	if len(query.ors) > 0 {
		for i := range newKeys {
			updatedKeys.add(newKeys[i])
		}

		for i := range query.ors {
			err := updateQuery(tx, dataType, query.ors[i], update, updatedKeys, skip)
			if err != nil {
				return err
			}
		}
	}

	return nil

}
