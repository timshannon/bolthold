// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold

import (
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/boltdb/bolt"
)

// AggregateResult allows you to access the results of an aggregate query
type AggregateResult struct {
	reduction []reflect.Value // always pointers
	group     reflect.Value
	sortby    string
}

// Group returns the field grouped by in the query
func (a *AggregateResult) Group(result interface{}) {
	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr {
		panic("result argument must be an address")
	}

	resultVal.Elem().Set(a.group)
}

// Reduction is the collection of records that are part of the AggregateResult Group
func (a *AggregateResult) Reduction(result interface{}) {
	resultVal := reflect.ValueOf(result)

	if resultVal.Kind() != reflect.Ptr || resultVal.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}

	sliceVal := resultVal.Elem()

	elType := sliceVal.Type().Elem()

	for i := range a.reduction {
		if elType.Kind() == reflect.Ptr {
			sliceVal = reflect.Append(sliceVal, a.reduction[i])
		} else {
			sliceVal = reflect.Append(sliceVal, a.reduction[i].Elem())
		}
	}

	resultVal.Elem().Set(sliceVal.Slice(0, sliceVal.Len()))
}

//TODO: replace with 1.8 sort.Slice
type aggregateResultSort AggregateResult

func (a *aggregateResultSort) Len() int { return len(a.reduction) }
func (a *aggregateResultSort) Swap(i, j int) {
	a.reduction[i], a.reduction[j] = a.reduction[j], a.reduction[i]
}
func (a *aggregateResultSort) Less(i, j int) bool {
	//reduction values are always pointers
	iVal := a.reduction[i].Elem().FieldByName(a.sortby)
	if !iVal.IsValid() {
		panic(fmt.Sprintf("The field %s does not exist in the type %s", a.sortby, a.reduction[i].Type()))
	}

	jVal := a.reduction[j].Elem().FieldByName(a.sortby)
	if !jVal.IsValid() {
		panic(fmt.Sprintf("The field %s does not exist in the type %s", a.sortby, a.reduction[j].Type()))
	}

	c, err := compare(iVal.Interface(), jVal.Interface())
	if err != nil {
		panic(err)
	}

	return c == -1
}

// Sort sorts the aggregate reduction by the passed in field in ascending order
// Sort is called automatically by calls to Min / Max to get the min and max values
func (a *AggregateResult) Sort(field string) {
	if !startsUpper(field) {
		panic("The first letter of a field must be upper-case")
	}
	if a.sortby == field {
		// already sorted
		return
	}

	a.sortby = field
	sort.Sort((*aggregateResultSort)(a))
}

// Max Returns the maxiumum value of the Aggregate Grouping, uses the Comparer interface if field
// can be automatically compared
func (a *AggregateResult) Max(field string, result interface{}) {
	a.Sort(field)

	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr {
		panic("result argument must be an address")
	}

	if resultVal.IsNil() {
		panic("result argument must not be nil")
	}

	resultVal.Elem().Set(a.reduction[len(a.reduction)-1].Elem())
}

// Min returns the minimum value of the Aggregate Grouping, uses the Comparer interface if field
// can be automatically compared
func (a *AggregateResult) Min(field string, result interface{}) {
	a.Sort(field)

	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr {
		panic("result argument must be an address")
	}

	if resultVal.IsNil() {
		panic("result argument must not be nil")
	}

	resultVal.Elem().Set(a.reduction[0].Elem())
}

// AvgInt returns the average integer value of the aggregate grouping
// returns an error if the field cannot be converted to an int64
func (a *AggregateResult) AvgInt(field string) (int64, error) {
	return 0, errors.New("TODO")
}

// AvgFloat returns the average float value of the aggregate grouping
// returns an error if the field cannot be converted to an float64
func (a *AggregateResult) AvgFloat(field string) (float64, error) {
	return 0, errors.New("TODO")
}

// SumInt returns the sum value of the aggregate grouping
// returns an error if the field cannot be converted to an int64
func (a *AggregateResult) SumInt(field string) (int64, error) {
	return 0, errors.New("TODO")
}

// SumFloat returns the sum value of the aggregate grouping
// returns an error if the field cannot be converted to an float64
func (a *AggregateResult) SumFloat(field string) (float64, error) {
	return 0, errors.New("TODO")
}

// Count returns the number of records in the aggregate grouping
func (a *AggregateResult) Count() int {
	return len(a.reduction)
}

// FindAggregate returns an aggregate grouping for the passed in query
// groupBy is optional
func (s *Store) FindAggregate(dataType interface{}, query *Query, groupBy string) ([]*AggregateResult, error) {
	var result []*AggregateResult
	var err error
	err = s.Bolt().View(func(tx *bolt.Tx) error {
		result, err = s.TxFindAggregate(tx, dataType, query, groupBy)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

// TxFindAggregate is the same as FindAggregate, but you specify your own transaction
// groupBy is optional
func (s *Store) TxFindAggregate(tx *bolt.Tx, dataType interface{}, query *Query, groupBy string) ([]*AggregateResult, error) {
	return aggregateQuery(tx, dataType, query, groupBy)
}
