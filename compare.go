// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package bolthold

import (
	"fmt"
	"math/big"
	"reflect"
	"time"
)

//Comparer compares a type against the encoded value in the store. The result should be 0 if current==other,
// -1 if current < other, and +1 if current > other.
// if a field in a struct doesn't specify a comparer, then the default comparison is used (convert to string and compare)
// this interface is already handled for standard Go Types as well as more complex ones such as those in time and big
// an error is returned if the type cannot be compared
type Comparer interface {
	Compare(other interface{}) (int, error)
}

func (c *Criterion) compare(testValue, otherValue interface{}) (int, error) {
	value := testValue

	for reflect.TypeOf(value).Kind() == reflect.Ptr {
		value = reflect.ValueOf(value).Elem().Interface()
	}

	other := otherValue
	for reflect.TypeOf(other).Kind() == reflect.Ptr {
		other = reflect.ValueOf(other).Elem().Interface()
	}

	switch t := value.(type) {
	case time.Time:
		other, ok := other.(time.Time)
		if !ok {
			return 0, fmt.Errorf("Type %s cannot be compared with %v", t, other)
		}
		if value.(time.Time).Equal(other) {
			return 0, nil
		}

		if value.(time.Time).Before(other) {
			return -1, nil
		}
		return 1, nil
	case big.Float:
		o, ok := other.(big.Float)
		if !ok {
			return 0, fmt.Errorf("Type %s cannot be compared with %v", t, other)
		}

		v := value.(big.Float)

		return v.Cmp(&o), nil
	case big.Int:
		o, ok := other.(big.Int)
		if !ok {
			return 0, fmt.Errorf("Type %s cannot be compared with %v", t, other)
		}

		v := value.(big.Int)

		return v.Cmp(&o), nil
	case big.Rat:
		o, ok := other.(big.Rat)
		if !ok {
			return 0, fmt.Errorf("Type %s cannot be compared with %v", t, other)
		}

		v := value.(big.Rat)

		return v.Cmp(&o), nil
	case int:
		other, ok := other.(int)
		if !ok {
			return 0, fmt.Errorf("Type %s cannot be compared with %v", t, other)
		}

		if value.(int) == other {
			return 0, nil
		}

		if value.(int) < other {
			return -1, nil
		}
		return 1, nil
	case int8:
		other, ok := other.(int8)
		if !ok {
			return 0, fmt.Errorf("Type %s cannot be compared with %v", t, other)
		}

		if value.(int8) == other {
			return 0, nil
		}

		if value.(int8) < other {
			return -1, nil
		}
		return 1, nil

	case int16:
		other, ok := other.(int16)
		if !ok {
			return 0, fmt.Errorf("Type %s cannot be compared with %v", t, other)
		}

		if value.(int16) == other {
			return 0, nil
		}

		if value.(int16) < other {
			return -1, nil
		}
		return 1, nil
	case int32:
		other, ok := other.(int32)
		if !ok {
			return 0, fmt.Errorf("Type %s cannot be compared with %v", t, other)
		}

		if value.(int32) == other {
			return 0, nil
		}

		if value.(int32) < other {
			return -1, nil
		}
		return 1, nil

	case int64:
		other, ok := other.(int64)
		if !ok {
			return 0, fmt.Errorf("Type %s cannot be compared with %v", t, other)
		}

		if value.(int64) == other {
			return 0, nil
		}

		if value.(int64) < other {
			return -1, nil
		}
		return 1, nil
	case uint:
		other, ok := other.(uint)
		if !ok {
			return 0, fmt.Errorf("Type %s cannot be compared with %v", t, other)
		}

		if value.(uint) == other {
			return 0, nil
		}

		if value.(uint) < other {
			return -1, nil
		}
		return 1, nil
	case uint8:
		other, ok := other.(uint8)
		if !ok {
			return 0, fmt.Errorf("Type %s cannot be compared with %v", t, other)
		}

		if value.(uint8) == other {
			return 0, nil
		}

		if value.(uint8) < other {
			return -1, nil
		}
		return 1, nil

	case uint16:
		other, ok := other.(uint16)
		if !ok {
			return 0, fmt.Errorf("Type %s cannot be compared with %v", t, other)
		}

		if value.(uint16) == other {
			return 0, nil
		}

		if value.(uint16) < other {
			return -1, nil
		}
		return 1, nil
	case uint32:
		other, ok := other.(uint32)
		if !ok {
			return 0, fmt.Errorf("Type %s cannot be compared with %v", t, other)
		}

		if value.(uint32) == other {
			return 0, nil
		}

		if value.(uint32) < other {
			return -1, nil
		}
		return 1, nil

	case uint64:
		other, ok := other.(uint64)
		if !ok {
			return 0, fmt.Errorf("Type %s cannot be compared with %v", t, other)
		}

		if value.(uint64) == other {
			return 0, nil
		}

		if value.(uint64) < other {
			return -1, nil
		}
		return 1, nil
	case float32:
		other, ok := other.(float32)
		if !ok {
			return 0, fmt.Errorf("Type %s cannot be compared with %v", t, other)
		}

		if value.(float32) == other {
			return 0, nil
		}

		if value.(float32) < other {
			return -1, nil
		}
		return 1, nil
	case float64:
		other, ok := other.(float64)
		if !ok {
			return 0, fmt.Errorf("Type %s cannot be compared with %v", t, other)
		}

		if value.(float64) == other {
			return 0, nil
		}

		if value.(float64) < other {
			return -1, nil
		}
		return 1, nil
	case string:
		other, ok := other.(string)
		if !ok {
			return 0, fmt.Errorf("Type %s cannot be compared with %v", t, other)
		}

		if value.(string) == other {
			return 0, nil
		}

		if value.(string) < other {
			return -1, nil
		}
		return 1, nil
	case Comparer:
		return value.(Comparer).Compare(other)
	default:
		valS := fmt.Sprintf("%s", value)
		otherS := fmt.Sprintf("%s", other)
		if valS == otherS {
			return 0, nil
		}

		if valS < otherS {
			return -1, nil
		}

		return 1, nil
	}
}
