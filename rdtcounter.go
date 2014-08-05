package riak

import (
	"github.com/tpjg/goriakpbc/pb"
)

type RDtCounter struct {
	Bucket    *Bucket
	Key       string
	Options   []map[string]uint32
	Value     *pb.DtValue
	Context   []uint8
	Incr int64
}

func (counter *RDtCounter) GetValue() int64 {
	if counter.Value == nil {
		return 0
	}
	return *counter.Value.CounterValue
}

func (counter *RDtCounter) Increment(value int64) {
	counter.Incr += value
}

func (counter *RDtCounter) Store() (err error) {
	req := &pb.DtUpdateReq{
		Type:   []byte(counter.Bucket.bucket_type),
		Bucket: []byte(counter.Bucket.name),
		Context: counter.Context,
		Key: []byte(counter.Key),
		Op: &pb.DtOp{
			CounterOp: &pb.CounterOp{
				Increment: &counter.Incr,
			},
		},
	}

	// Add the options
	for _, omap := range counter.Options {
		for k, v := range omap {
			switch k {
			case "w":
				req.W = &v
			case "dw":
				req.Dw = &v
			case "pw":
				req.Pw = &v
			}
		}
	}

	// Send the request
	err, conn := counter.Bucket.client.request(req, dtUpdateReq)
	if err != nil {
		return err
	}
	// Get response, ReturnHead is true, so we can store the vclock
	resp := &pb.DtUpdateResp{}
	err = counter.Bucket.client.response(conn, resp)
	if err != nil {
		return err
	}
	return nil
}

