package bolthold

import (
	"errors"
	"reflect"

	"github.com/boltdb/bolt"
)

// AggregateResult allows you to access the results of an aggregate query
type AggregateResult struct {
	reduction []reflect.Value
	group     reflect.Value
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

	for i := range a.reduction {
		sliceVal = reflect.Append(sliceVal, a.reduction[i])
	}

	resultVal.Elem().Set(sliceVal.Slice(0, sliceVal.Len()))
}

func (a *AggregateResult) sort(field string) {
	//TODO: Sort
}

// Max Returns the maxiumum value of the Aggregate Grouping
func (a *AggregateResult) Max(field string, result interface{}) {
	a.sort(field)

	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr {
		panic("result argument must be an address")
	}

	resultVal.Elem().Set(a.reduction[:len(a.reduction)-1][0])
}

// Min returns the minimum value of the Aggregate Grouping
func (a *AggregateResult) Min(field string, result interface{}) {
	a.sort(field)

	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr {
		panic("result argument must be an address")
	}

	resultVal.Elem().Set(a.reduction[0])
}

// Avg returns the average value of the aggregate grouping
func (a *AggregateResult) Avg(field string) (float64, error) {
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
