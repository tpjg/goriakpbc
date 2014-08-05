package riak

import (
	"bytes"

	"github.com/tpjg/goriakpbc/pb"
)

type RDtSet struct {
	Bucket    *Bucket
	Key       string
	Options   []map[string]uint32
	Value    *pb.DtValue
	Context  []uint8
	ToAdd    [][]byte
	ToRemove [][]byte
}

func (set *RDtSet) GetValue() [][]byte {
	if set.Value == nil {
		return [][]byte{}
	}
	return set.Value.SetValue
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

func (set *RDtSet) Store() (err error) {
	req := &pb.DtUpdateReq{
		Type:   []byte(set.Bucket.bucket_type),
		Bucket: []byte(set.Bucket.name),
		Context: set.Context,
		Key: []byte(set.Key),
		Op: &pb.DtOp{
			SetOp: &pb.SetOp{
				Adds: set.ToAdd,
				Removes: set.ToRemove,
			},
		},
	}

	// Add the options
	for _, omap := range set.Options {
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
	err, conn := set.Bucket.client.request(req, dtUpdateReq)
	if err != nil {
		return err
	}
	// Get response, ReturnHead is true, so we can store the vclock
	resp := &pb.DtUpdateResp{}
	err = set.Bucket.client.response(conn, resp)
	if err != nil {
		return err
	}
	return nil
}

