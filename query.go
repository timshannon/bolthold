// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
)

type record struct {
	key   []byte
	value reflect.Value
}

func (s *Store) runQuery(source BucketSource, dataType interface{}, query *Query, retrievedKeys keyList, skip int,
	action func(r *record) error) error {
	storer := s.newStorer(dataType)

	bkt := source.Bucket([]byte(storer.Type()))
	if bkt == nil {
		// if the bucket doesn't exist or is empty then our job is really easy!
		return nil
	}

	if query.index != "" && source.Bucket(indexBucketName(storer.Type(), query.index)) == nil {
		return fmt.Errorf("The index %s does not exist", query.index)
	}

	tp := dataType

	for reflect.TypeOf(tp).Kind() == reflect.Ptr {
		tp = reflect.ValueOf(tp).Elem().Interface()
	}

	query.dataType = reflect.TypeOf(tp)

	if len(query.sort) > 0 {
		return s.runQuerySort(source, dataType, query, action)
	}

	iter := s.newIterator(source, storer.Type(), query)

	newKeys := make(keyList, 0)

	limit := query.limit - len(retrievedKeys)

	for k, v := iter.Next(); k != nil; k, v = iter.Next() {
		if len(retrievedKeys) != 0 {
			// don't check this record if it's already been retrieved
			if retrievedKeys.in(k) {
				continue
			}
		}

		val := reflect.New(reflect.TypeOf(tp))

		err := s.decode(v, val.Interface())
		if err != nil {
			return err
		}

		query.source = source

		ok, err := query.matchesAllFields(s, k, val, val.Interface())
		if err != nil {
			return err
		}

		if ok {
			if skip > 0 {
				skip--
				continue
			}

			err = action(&record{
				key:   k,
				value: val,
			})
			if err != nil {
				return err
			}

			// track that this key's entry has been added to the result list
			newKeys.add(k)

			if query.limit != 0 {
				limit--
				if limit == 0 {
					break
				}
			}
		}

	}

	if iter.Error() != nil {
		return iter.Error()
	}

	if query.limit != 0 && limit == 0 {
		return nil
	}

	if len(query.ors) > 0 {
		for i := range newKeys {
			retrievedKeys.add(newKeys[i])
		}

		for i := range query.ors {
			err := s.runQuery(source, tp, query.ors[i], retrievedKeys, skip, action)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// runQuerySort runs the query without sort, skip, or limit, then applies them to the entire result set
func (s *Store) runQuerySort(source BucketSource, dataType interface{}, query *Query, action func(r *record) error) error {
	// Validate sort fields
	for _, field := range query.sort {
		fields := strings.Split(field, ".")

		current := query.dataType
		for i := range fields {
			var structField reflect.StructField
			found := false
			if current.Kind() == reflect.Ptr {
				structField, found = current.Elem().FieldByName(fields[i])
			} else {
				structField, found = current.FieldByName(fields[i])
			}

			if !found {
				return fmt.Errorf("The field %s does not exist in the type %s", field, query.dataType)
			}
			current = structField.Type
		}
	}

	// Run query without sort, skip or limit
	// apply sort, skip and limit to entire dataset
	qCopy := *query
	qCopy.sort = nil
	qCopy.limit = 0
	qCopy.skip = 0

	var records []*record
	err := s.runQuery(source, dataType, &qCopy, nil, 0,
		func(r *record) error {
			records = append(records, r)

			return nil
		})

	if err != nil {
		return err
	}

	sort.Slice(records, func(i, j int) bool {
		for _, field := range query.sort {
			value, err := fieldValue(records[i].value.Elem(), field)
			if err != nil {
				panic(err.Error()) // shouldn't happen due to field check above
			}

			other, err := fieldValue(records[j].value.Elem(), field)
			if err != nil {
				panic(err.Error()) // shouldn't happen due to field check above
			}

			if query.reverse {
				value, other = other, value
			}

			cmp, cerr := compare(value, other)
			if cerr != nil {
				// if for some reason there is an error on compare, fallback to a lexicographic compare
				valS := fmt.Sprintf("%s", value)
				otherS := fmt.Sprintf("%s", other)
				if valS < otherS {
					return true
				} else if valS == otherS {
					continue
				}
				return false
			}

			if cmp == -1 {
				return true
			} else if cmp == 0 {
				continue
			}
			return false
		}
		return false
	})

	// apply skip and limit
	limit := query.limit
	skip := query.skip

	if skip > len(records) {
		records = records[0:0]
	} else {
		records = records[skip:]
	}

	if limit > 0 && limit <= len(records) {
		records = records[:limit]
	}

	for i := range records {
		err = action(records[i])
		if err != nil {
			return err
		}
	}

	return nil

}

func (s *Store) findQuery(source BucketSource, result interface{}, query *Query) error {
	if query == nil {
		query = &Query{}
	}

	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr || resultVal.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}

	sliceVal := resultVal.Elem()

	elType := sliceVal.Type().Elem()

	tp := elType

	for tp.Kind() == reflect.Ptr {
		tp = tp.Elem()
	}

	var keyType reflect.Type
	var keyField string

	for i := 0; i < tp.NumField(); i++ {
		if strings.Contains(string(tp.Field(i).Tag), BoltholdKeyTag) {
			keyType = tp.Field(i).Type
			keyField = tp.Field(i).Name
			break
		}
	}

	val := reflect.New(tp)

	err := s.runQuery(source, val.Interface(), query, nil, query.skip,
		func(r *record) error {
			var rowValue reflect.Value

			// FIXME:
			if elType.Kind() == reflect.Ptr {
				rowValue = r.value
			} else {
				rowValue = r.value.Elem()
			}

			if keyType != nil {
				rowKey := rowValue
				for rowKey.Kind() == reflect.Ptr {
					rowKey = rowKey.Elem()
				}
				err := s.decode(r.key, rowKey.FieldByName(keyField).Addr().Interface())
				if err != nil {
					return err
				}
			}

			sliceVal = reflect.Append(sliceVal, rowValue)

			return nil
		})

	if err != nil {
		return err
	}

	resultVal.Elem().Set(sliceVal.Slice(0, sliceVal.Len()))

	return nil
}

func (s *Store) deleteQuery(source BucketSource, dataType interface{}, query *Query) error {
	if query == nil {
		query = &Query{}
	}

	var records []*record

	err := s.runQuery(source, dataType, query, nil, query.skip,
		func(r *record) error {
			records = append(records, r)

			return nil
		})

	if err != nil {
		return err
	}

	storer := s.newStorer(dataType)

	b := source.Bucket([]byte(storer.Type()))
	for i := range records {
		err := b.Delete(records[i].key)
		if err != nil {
			return err
		}

		// remove any indexes
		err = s.deleteIndexes(storer, source, records[i].key, records[i].value.Interface())
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) updateQuery(source BucketSource, dataType interface{}, query *Query, update func(record interface{}) error) error {
	if query == nil {
		query = &Query{}
	}

	var records []*record

	err := s.runQuery(source, dataType, query, nil, query.skip,
		func(r *record) error {
			records = append(records, r)

			return nil

		})

	if err != nil {
		return err
	}

	storer := s.newStorer(dataType)
	b := source.Bucket([]byte(storer.Type()))

	for i := range records {
		upVal := records[i].value.Interface()

		// delete any existing indexes bad on original value
		err := s.deleteIndexes(storer, source, records[i].key, upVal)
		if err != nil {
			return err
		}

		err = update(upVal)
		if err != nil {
			return err
		}

		encVal, err := s.encode(upVal)
		if err != nil {
			return err
		}

		err = b.Put(records[i].key, encVal)
		if err != nil {
			return err
		}

		// insert any new indexes
		err = s.addIndexes(storer, source, records[i].key, upVal)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) aggregateQuery(source BucketSource, dataType interface{}, query *Query,
	groupBy ...string) ([]*AggregateResult, error) {
	if query == nil {
		query = &Query{}
	}

	var result []*AggregateResult

	if len(groupBy) == 0 {
		result = append(result, &AggregateResult{})
	}

	err := s.runQuery(source, dataType, query, nil, query.skip,
		func(r *record) error {
			if len(groupBy) == 0 {
				result[0].reduction = append(result[0].reduction, r.value)
				return nil
			}

			grouping := make([]reflect.Value, len(groupBy))

			for i := range groupBy {
				fVal := r.value.Elem().FieldByName(groupBy[i])
				if !fVal.IsValid() {
					return fmt.Errorf("The field %s does not exist in the type %s", groupBy[i], r.value.Type())
				}

				grouping[i] = fVal
			}

			var err error
			var c int
			var allEqual bool

			i := sort.Search(len(result), func(i int) bool {
				for j := range grouping {
					c, err = compare(result[i].group[j].Interface(), grouping[j].Interface())
					if err != nil {
						return true
					}
					if c != 0 {
						return c >= 0
					}
					// if group part is equal, compare the next group part
				}
				allEqual = true
				return true
			})

			if err != nil {
				return err
			}

			if i < len(result) {
				if allEqual {
					// group already exists, append results to reduction
					result[i].reduction = append(result[i].reduction, r.value)
					return nil
				}
			}

			// group  not found, create another grouping at i
			result = append(result, nil)
			copy(result[i+1:], result[i:])
			result[i] = &AggregateResult{
				group:     grouping,
				reduction: []reflect.Value{r.value},
			}

			return nil
		})

	if err != nil {
		return nil, err
	}

	return result, nil
}

func (s *Store) countQuery(source BucketSource, dataType interface{}, query *Query) (int, error) {
	if query == nil {
		query = &Query{}
	}

	count := 0

	err := s.runQuery(source, dataType, query, nil, query.skip,
		func(r *record) error {
			count++
			return nil
		})

	if err != nil {
		return 0, err
	}

	return count, nil
}

func (s *Store) findOneQuery(source BucketSource, result interface{}, query *Query) error {
	if query == nil {
		query = &Query{}
	}

	originalLimit := query.limit

	query.limit = 1

	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr {
		panic("result argument must be an address")
	}

	structType := resultVal.Elem().Type()

	var keyType reflect.Type
	var keyField string

	for i := 0; i < structType.NumField(); i++ {
		if strings.Contains(string(structType.Field(i).Tag), BoltholdKeyTag) {
			keyType = structType.Field(i).Type
			keyField = structType.Field(i).Name
			break
		}
	}

	found := false

	err := s.runQuery(source, result, query, nil, query.skip,
		func(r *record) error {
			found = true

			if keyType != nil {
				rowKey := r.value
				for rowKey.Kind() == reflect.Ptr {
					rowKey = rowKey.Elem()
				}
				err := s.decode(r.key, rowKey.FieldByName(keyField).Addr().Interface())
				if err != nil {
					return err
				}
			}
			resultVal.Elem().Set(r.value.Elem())

			return nil
		})
	query.limit = originalLimit

	if err != nil {
		return err
	}

	if !found {
		return ErrNotFound
	}

	return nil
}

func (s *Store) forEach(source BucketSource, query *Query, fn interface{}) error {
	if query == nil {
		query = &Query{}
	}

	fnVal := reflect.ValueOf(fn)
	argType := reflect.TypeOf(fn).In(0)

	if argType.Kind() == reflect.Ptr {
		argType = argType.Elem()
	}

	dataType := reflect.New(argType).Interface()

	return s.runQuery(source, dataType, query, nil, query.skip, func(r *record) error {
		out := fnVal.Call([]reflect.Value{r.value})
		if len(out) != 1 {
			return fmt.Errorf("foreach function does not return an error")
		}

		if out[0].IsNil() {
			return nil
		}

		return out[0].Interface().(error)
	})
}
