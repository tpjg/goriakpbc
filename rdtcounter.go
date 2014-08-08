package riak

import (
	"github.com/tpjg/goriakpbc/pb"
)

type RDtCounter struct {
	RDataTypeObject
	Value   *int64
	Incr    int64
}

func (counter *RDtCounter) GetValue() int64 {
	if counter.Value == nil {
		return 0
	}
	return *counter.Value
}

func (counter *RDtCounter) Increment(value int64) {
	counter.Incr += value
}

func (counter *RDtCounter) ToOp() *pb.DtOp {
	if counter.Incr == 0 {
		return nil
	}
	return &pb.DtOp{
		CounterOp: &pb.CounterOp{
			Increment: &counter.Incr,
		},
	}
}

func (counter *RDtCounter) Store() (err error) {
	op := counter.ToOp()
	if op == nil {
		// nothing to do
		return nil
	}
	return counter.RDataTypeObject.store(op)
}
