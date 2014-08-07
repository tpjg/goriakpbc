package riak

import (
	"errors"

	"github.com/tpjg/goriakpbc/pb"
)

const (
	TYPE_COUNTER = 1
	TYPE_SET     = 2
	TYPE_MAP     = 3
)

func (b *Bucket) FetchCounter(key string, options ...map[string]uint32) (obj *RDtCounter, err error) {
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
	obj = &RDtCounter{Key: key, Bucket: b, Options: options}

	switch *resp.Type {
	case TYPE_COUNTER:
		obj.Context = resp.Context
		obj.Value = resp.Value.CounterValue
	default:
		return nil, errors.New("Type mismatch")
	}

	// If no Content is returned then the object was  not found
	if resp.Value == nil {
		return obj, NotFound
	}

	return obj, nil
}

func (b *Bucket) FetchSet(key string, options ...map[string]uint32) (obj *RDtSet, err error) {
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
	obj = &RDtSet{Key: key, Bucket: b, Options: options}

	switch *resp.Type {
	case TYPE_SET:
		obj.Context = resp.Context
		obj.Value = resp.Value.SetValue
	default:
		return nil, errors.New("Type mismatch")
	}

	// If no Content is returned then the object was  not found
	if resp.Value == nil {
		return obj, NotFound
	}

	return obj, nil
}

func (b *Bucket) FetchMap(key string, options ...map[string]uint32) (obj *RDtMap, err error) {
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
	obj = &RDtMap{Key: key, Bucket: b, Options: options}

	switch *resp.Type {
	case TYPE_MAP:
		obj.Context = resp.Context
		if resp.Value != nil {
			obj.Init(resp.Value.MapValue)
		} else {
			obj.Init(nil)
		}
	default:
		return nil, errors.New("Type mismatch")
	}

	// If no Content is returned then the object was  not found
	if resp.Value == nil {
		return obj, NotFound
	}

	return obj, nil
}
