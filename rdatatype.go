package riak

import (
	"errors"

	"github.com/atticlab/goriakpbc/pb"
)

const (
	TYPE_COUNTER = 1
	TYPE_SET     = 2
	TYPE_MAP     = 3
)

type RDataTypeObject struct {
	Bucket  *Bucket
	Key     string
	Options []map[string]uint32
	Context []uint8
}

type RDataType interface {
	Store() error
	Destroy() error
}

func (b *Bucket) FetchCounter(key string, options ...map[string]uint32) (obj *RDtCounter, err error) {
	o, err := b.fetch(key, options...)
	if o != nil {
		obj = o.(*RDtCounter)
	}
	return
}

func (b *Bucket) FetchSet(key string, options ...map[string]uint32) (obj *RDtSet, err error) {
	o, err := b.fetch(key, options...)
	if o != nil {
		obj = o.(*RDtSet)
	}
	return
}

func (b *Bucket) FetchMap(key string, options ...map[string]uint32) (obj *RDtMap, err error) {
	o, err := b.fetch(key, options...)
	if o != nil {
		obj = o.(*RDtMap)
	}
	return
}

func (b *Bucket) fetch(key string, options ...map[string]uint32) (obj RDataType, err error) {
	t := true
	req := &pb.DtFetchReq{
		Type:       []byte(b.bucket_type),
		Bucket:     []byte(b.name),
		Key:        []byte(key),
		NotfoundOk: &t,
	}
	for _, omap := range options {
		for k, v := range omap {
			switch k {
			case "r":
				req.R = &v
			case "pr":
				req.Pr = &v
			case "include_context":
				include_context := v == 1
				req.IncludeContext = &include_context
			}
		}
	}
	err, conn := b.client.request(req, dtFetchReq)
	if err != nil {
		return nil, err
	}
	resp := &pb.DtFetchResp{}
	err = b.client.response(conn, resp)
	if err != nil {
		return nil, err
	}
	// Create a new object (even if only for storing the returned Vclock)

	switch *resp.Type {
	case TYPE_COUNTER:
		obj = &RDtCounter{RDataTypeObject: RDataTypeObject{Key: key, Bucket: b, Options: options, Context: resp.Context}}
	case TYPE_SET:
		obj = &RDtSet{RDataTypeObject: RDataTypeObject{Key: key, Bucket: b, Options: options, Context: resp.Context}}
	case TYPE_MAP:
		obj = &RDtMap{RDataTypeObject: RDataTypeObject{Key: key, Bucket: b, Options: options, Context: resp.Context}}
		obj.(*RDtMap).Init(nil)
	default:
		return nil, errors.New("Type mismatch")
	}

	// If no Content is returned then the object was  not found
	if resp.Value == nil {
		return obj, NotFound
	}

	switch dt := obj.(type) {
	case *RDtCounter:
		dt.Value = resp.Value.CounterValue
	case *RDtSet:
		dt.Value = resp.Value.SetValue
	case *RDtMap:
		dt.Init(resp.Value.MapValue)
	}

	return obj, nil
}

func (m RDataTypeObject) store(op *pb.DtOp) (err error) {
	req := &pb.DtUpdateReq{
		Type:    []byte(m.Bucket.bucket_type),
		Bucket:  []byte(m.Bucket.name),
		Context: m.Context,
		Key:     []byte(m.Key),
		Op:      op,
	}

	// Add the options
	for _, omap := range m.Options {
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
	err, conn := m.Bucket.client.request(req, dtUpdateReq)
	if err != nil {
		return err
	}
	// Get response, ReturnHead is true, so we can store the vclock
	resp := &pb.DtUpdateResp{}
	err = m.Bucket.client.response(conn, resp)
	if err != nil {
		return err
	}
	return nil
}

func (obj *RDataTypeObject) Destroy() (err error) {
	req := &pb.RpbDelReq{
		Type:   []byte(obj.Bucket.bucket_type),
		Bucket: []byte(obj.Bucket.name),
		Key:    []byte(obj.Key),
	}
	for _, omap := range obj.Options {
		for k, v := range omap {
			switch k {
			case "r":
				req.R = &v
			case "pr":
				req.Pr = &v
			case "rw":
				req.Rw = &v
			case "w":
				req.W = &v
			case "dw":
				req.Dw = &v
			case "pw":
				req.Pw = &v
			}
		}
	}

	err, conn := obj.Bucket.client.request(req, rpbDelReq)
	if err != nil {
		return err
	}
	err = obj.Bucket.client.response(conn, req)
	if err != nil {
		return err
	}
	return nil
}
