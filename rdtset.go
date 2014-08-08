package riak

import (
	"bytes"

	"github.com/tpjg/goriakpbc/pb"
)

type RDtSet struct {
	RDataTypeObject
	Value    [][]byte
	ToAdd    [][]byte
	ToRemove [][]byte
}

func (set *RDtSet) GetValue() [][]byte {
	if set.Value == nil {
		return [][]byte{}
	}
	return set.Value
}

func (set *RDtSet) Add(value []byte) {
	for _, e := range set.ToAdd {
		if bytes.Compare(e, value) == 0 {
			return
		}
	}
	set.ToAdd = append(set.ToAdd, value)
}

func (set *RDtSet) Remove(value []byte) {
	for _, e := range set.ToRemove {
		if bytes.Compare(e, value) == 0 {
			return
		}
	}
	set.ToRemove = append(set.ToRemove, value)
}

func (set *RDtSet) ToOp() *pb.DtOp {
	if len(set.ToAdd) == 0 && len(set.ToRemove) == 0 {
		return nil
	}
	return &pb.DtOp{
		SetOp: &pb.SetOp{
			Adds:    set.ToAdd,
			Removes: set.ToRemove,
		},
	}
}

func (set *RDtSet) Store() (err error) {
	op := set.ToOp()
	if op == nil {
		// nothing to do
		return nil
	}
	return set.RDataTypeObject.store(op)
}
