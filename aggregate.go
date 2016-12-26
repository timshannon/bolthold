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
func (a *AggregateResult) Group(result interface{}) error {
	result = a.group.Interface()
	return nil
}

// Reduction is the collection of records that are part of the AggregateResult Group
func (a *AggregateResult) Reduction(result interface{}) error {
	return errors.New("TODO")
}

// Max Returns the maxiumum value of the Aggregate Grouping
func (a *AggregateResult) Max(field string, result interface{}) error {
	return errors.New("TODO")
}

// Min returns the minimum value of the Aggregate Grouping
func (a *AggregateResult) Min(field string, result interface{}) error {
	return errors.New("TODO")
}

// Median returns the median record of the aggregate grouping
func (a *AggregateResult) Median(result interface{}) error {
	return errors.New("TODO")
}

// Avg returns the average value of the aggregate grouping
func (a *AggregateResult) Avg(field string) (float64, error) {
	return 0, errors.New("TODO")
}

// Count returns the number of records in the aggregate grouping
func (a *AggregateResult) Count() int64 {
	return -1
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
