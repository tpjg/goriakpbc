// +build go1.1

package riak

import (
	"reflect"
)

// This part of the package requires Go1.1 which gave us reflect.SliceOf (and some more).

// Allocates a slice of models for the result and populates it with the data
// from the siblings held in Riak.
func (m *Model) Siblings(dest Resolver) (result interface{}, err error) {
	_, dt, _, _, err := check_dest(dest)
	if err != nil {
		return result, err
	}

	// Count non-empty siblings
	count := 0
	for _, s := range m.robject.Siblings {
		if len(s.Data) != 0 {
			count += 1
		}
	}

	types := reflect.SliceOf(dt)
	values := reflect.MakeSlice(types, count, count)

	err = m.GetSiblings(values.Interface())
	return values.Interface(), err
}
