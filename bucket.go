package riak

import (
	"github.com/tpjg/goriakpbc/pb"
)

// Implements access to a bucket and its properties
type Bucket struct {
	name      string
	client    *Client
	nval      uint32
	allowMult bool
}

// Return a new bucket object
func (c *Client) NewBucket(name string) (*Bucket, error) {
	if name == "" {
		return nil, NoBucketName
	}
	req := &pb.RpbGetBucketReq{
		Bucket: []byte(name),
	}
	err, conn := c.request(req, rpbGetBucketReq)

	if err != nil {
		return nil, err
	}
	resp := &pb.RpbGetBucketResp{}
	err = c.response(conn, resp)

	if err != nil {
		return nil, err
	}
	bucket := &Bucket{name: name, client: c, nval: *resp.Props.NVal, allowMult: *resp.Props.AllowMult}
	return bucket, nil
}

// Return a new bucket object. DEPRECATED, use NewBucket instead.
func (c *Client) Bucket(name string) (*Bucket, error) {
	return c.NewBucket(name)
}

// Return the bucket name
func (b *Bucket) Name() string {
	return b.name
}

// Return the nval property of a bucket
func (b *Bucket) NVal() uint32 {
	return b.nval
}

// Return the allowMult property of a bucket
func (b *Bucket) AllowMult() bool {
	return b.allowMult
}

// Set the nval property of a bucket
func (b *Bucket) SetNVal(nval uint32) (err error) {
	props := &pb.RpbBucketProps{NVal: &nval, AllowMult: &b.allowMult}
	req := &pb.RpbSetBucketReq{Bucket: []byte(b.name), Props: props}
	err, conn := b.client.request(req, rpbSetBucketReq)
	if err != nil {
		return err
	}
	err = b.client.response(conn, req)
	if err != nil {
		return err
	}
	b.nval = nval
	return nil
}

// Set the allowMult property of a bucket
func (b *Bucket) SetAllowMult(allowMult bool) (err error) {
	props := &pb.RpbBucketProps{NVal: &b.nval, AllowMult: &allowMult}
	req := &pb.RpbSetBucketReq{Bucket: []byte(b.name), Props: props}
	err, conn := b.client.request(req, rpbSetBucketReq)
	if err != nil {
		return err
	}
	err = b.client.response(conn, req)
	if err != nil {
		return err
	}
	b.allowMult = allowMult
	return nil
}

// Delete a key/value from the bucket
func (b *Bucket) Delete(key string, options ...map[string]uint32) (err error) {
	req := &pb.RpbDelReq{Bucket: []byte(b.name), Key: []byte(key)}
	for _, omap := range options {
		for k, v := range omap {
			switch k {
			case "r":
				req.R = &v
			case "pr":
				req.Pr = &v
			case "rq":
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

	err, conn := b.client.request(req, rpbDelReq)
	if err != nil {
		return err
	}
	err = b.client.response(conn, req)
	if err != nil {
		return err
	}
	return nil
}

// Delete directly from a bucket, without creating a bucket object first
func (c *Client) DeleteFrom(bucketname string, key string, options ...map[string]uint32) (err error) {
	var bucket *Bucket
	bucket, err = c.Bucket(bucketname)
	if err != nil {
		return
	}
	return bucket.Delete(key, options...)
}

// Create a new RObject
func (b *Bucket) NewObject(key string, options ...map[string]uint32) *RObject {
	obj := &RObject{Key: key, Bucket: b,
		Links: make([]Link, 0),
		Meta:  make(map[string]string), Indexes: make(map[string]string),
		Options: options}
	return obj
}

// Create a new RObject. DEPRECATED, use NewObject instead
func (b *Bucket) New(key string, options ...map[string]uint32) *RObject {
	return b.NewObject(key, options...)
}

// Create a new RObject in a bucket directly, without creating a bucket object first
func (c *Client) NewObjectIn(bucketname string, key string, options ...map[string]uint32) (*RObject, error) {
	bucket, err := c.Bucket(bucketname)
	if err != nil {
		return nil, err
	}
	return bucket.NewObject(key, options...), nil
}

// Test if an object exists
func (b *Bucket) Exists(key string, options ...map[string]uint32) (exists bool, err error) {
	t := true
	req := &pb.RpbGetReq{
		Bucket:     []byte(b.name),
		Key:        []byte(key),
		NotfoundOk: &t,
		Head:       &t}
	for _, omap := range options {
		for k, v := range omap {
			switch k {
			case "r":
				req.R = &v
			case "pr":
				req.Pr = &v
			}
		}
	}

	err, conn := b.client.request(req, rpbGetReq)
	if err != nil {
		return false, err
	}
	resp := &pb.RpbGetResp{}
	err = b.client.response(conn, resp)
	if err != nil {
		return false, err
	}
	return len(resp.Content) != 0, nil
}

// Test if an object exists in a bucket directly, without creating a bucket object first
func (c *Client) ExistsIn(bucketname string, key string, options ...map[string]uint32) (exists bool, err error) {
	bucket, err := c.Bucket(bucketname)
	if err != nil {
		return false, err
	}
	return bucket.Exists(key, options...)
}

// Return a list of keys using the index for a single key
func (b *Bucket) IndexQuery(index string, key string) (keys []string, err error) {
	req := &pb.RpbIndexReq{Bucket: []byte(b.name), Index: []byte(index),
		Qtype: pb.RpbIndexReq_eq.Enum(), Key: []byte(key)}
	err, conn := b.client.request(req, rpbIndexReq)
	if err != nil {
		return nil, err
	}
	resp := &pb.RpbIndexResp{}
	err = b.client.response(conn, resp)
	if err != nil {
		return nil, err
	}
	keys = make([]string, len(resp.Keys))
	for i, v := range resp.Keys {
		keys[i] = string(v)
	}
	return
}

// Return a list of keys using the index range query
func (b *Bucket) IndexQueryRange(index string, min string, max string) (keys []string, err error) {
	req := &pb.RpbIndexReq{Bucket: []byte(b.name), Index: []byte(index),
		Qtype:    pb.RpbIndexReq_range.Enum(),
		RangeMin: []byte(min), RangeMax: []byte(max)}
	err, conn := b.client.request(req, rpbIndexReq)
	if err != nil {
		return nil, err
	}
	resp := &pb.RpbIndexResp{}
	err = b.client.response(conn, resp)
	if err != nil {
		return nil, err
	}
	keys = make([]string, len(resp.Keys))
	for i, v := range resp.Keys {
		keys[i] = string(v)
	}
	return
}

// List all keys from bucket
func (b *Bucket) ListKeys() (response [][]byte, err error) {
	req := &pb.RpbListKeysReq{Bucket: []byte(b.name)}

	err, conn := b.client.request(req, rpbListKeysReq)
	if err != nil {
		return nil, err
	}

	return b.client.mp_response(conn)
}
