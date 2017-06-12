package riak

import (
	"github.com/tpjg/goriakpbc/pb"
)

// Implements access to a bucket and its properties
type Bucket struct {
	bucket_type   string
	name          string
	client        *Client
	nval          uint32
	allowMult     bool
	lastWriteWins bool
	search        bool
	searchIndex   string
	datatype      string
	consistent    bool
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

	bucket := &Bucket{
		name:          name,
		client:        c,
		nval:          resp.Props.GetNVal(),
		allowMult:     resp.Props.GetAllowMult(),
		lastWriteWins: resp.Props.GetLastWriteWins(),
		search: 			resp.Props.GetSearch(),
		searchIndex:	string(resp.Props.GetSearchIndex()),
		bucket_type:   `default`,
	}

	return bucket, nil
}

func (c *Client) NewBucketType(btype, name string) (*Bucket, error) {
	if name == "" || btype == "" {
		return nil, NoBucketName
	}
	req := &pb.RpbGetBucketReq{
		Bucket: []byte(name),
		Type:   []byte(btype),
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

	bucket := &Bucket{
		name:          name,
		bucket_type:   btype,
		client:        c,
		nval:          resp.Props.GetNVal(),
		allowMult:     resp.Props.GetAllowMult(),
		lastWriteWins: resp.Props.GetLastWriteWins(),
		datatype:      string(resp.Props.GetDatatype()),
		consistent:    resp.Props.GetConsistent(),
	}

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

// Return the lastWriteWins property of a bucket
func (b *Bucket) LastWriteWins() bool {
	return b.lastWriteWins
}

// Return the search property of a bucket
func (b *Bucket) Search() bool {
	return b.search
}

// Set the search property of a bucket
func (b *Bucket) SetSearch(search bool) (err error) {
	props := &pb.RpbBucketProps{NVal: &b.nval, AllowMult: &b.allowMult, LastWriteWins: &b.lastWriteWins, Search: &search}
	req := &pb.RpbSetBucketReq{Bucket: []byte(b.name), Type: []byte(b.bucket_type), Props: props}
	err, conn := b.client.request(req, rpbSetBucketReq)
	if err != nil {
		return err
	}
	err = b.client.response(conn, req)
	if err != nil {
		return err
	}
	b.search = search
	return nil
}

// Set the search_index property of a bucket
func (b *Bucket) SetSearchIndex(searchIndex string) (err error) {
	props := &pb.RpbBucketProps{SearchIndex: []byte(searchIndex)}
	req := &pb.RpbSetBucketReq{Bucket: []byte(b.name), Type: []byte(b.bucket_type), Props: props}
	err, conn := b.client.request(req, rpbSetBucketReq)
	if err != nil {
		return err
	}
	err = b.client.response(conn, req)
	if err != nil {
		return err
	}
	b.searchIndex = searchIndex
	return nil
}

// Return the search_index property of a bucket
func (b *Bucket) SearchIndex() string {
	return b.searchIndex
}

// Set the nval property of a bucket
func (b *Bucket) SetNVal(nval uint32) (err error) {
	props := &pb.RpbBucketProps{NVal: &nval, AllowMult: &b.allowMult, LastWriteWins: &b.lastWriteWins, Search: &b.search}
	req := &pb.RpbSetBucketReq{Bucket: []byte(b.name), Type: []byte(b.bucket_type), Props: props}
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
	props := &pb.RpbBucketProps{NVal: &b.nval, AllowMult: &allowMult, LastWriteWins: &b.lastWriteWins, Search: &b.search}
	req := &pb.RpbSetBucketReq{Bucket: []byte(b.name), Type: []byte(b.bucket_type), Props: props}
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

// Set the lastWriteWins property of a bucket
func (b *Bucket) SetLastWriteWins(lastWriteWins bool) (err error) {
	props := &pb.RpbBucketProps{NVal: &b.nval, AllowMult: &b.allowMult, LastWriteWins: &lastWriteWins, Search: &b.search}
	req := &pb.RpbSetBucketReq{Bucket: []byte(b.name), Type: []byte(b.bucket_type), Props: props}
	err, conn := b.client.request(req, rpbSetBucketReq)
	if err != nil {
		return err
	}
	err = b.client.response(conn, req)
	if err != nil {
		return err
	}
	b.lastWriteWins = lastWriteWins
	return nil
}

// Delete a key/value from the bucket
func (b *Bucket) Delete(key string, options ...map[string]uint32) (err error) {
	req := &pb.RpbDelReq{Bucket: []byte(b.name), Type: []byte(b.bucket_type), Key: []byte(key)}
	for _, omap := range options {
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
		Links:   make([]Link, 0),
		Meta:    make(map[string]string),
		Indexes: make(map[string][]string),
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
		Type:       []byte(b.bucket_type),
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
	req := &pb.RpbIndexReq{Bucket: []byte(b.name), Type: []byte(b.bucket_type), Index: []byte(index),
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

// Return a page of keys using the index for a single key
func (b *Bucket) IndexQueryPage(index string, key string, results uint32, continuation string) (keys []string, next string, err error) {
	req := &pb.RpbIndexReq{Bucket: []byte(b.name), Type: []byte(b.bucket_type), Index: []byte(index),
		Qtype: pb.RpbIndexReq_eq.Enum(), Key: []byte(key),
		MaxResults: &results}

	if continuation != "" {
		req.Continuation = []byte(continuation)
	}

	err, conn := b.client.request(req, rpbIndexReq)
	if err != nil {
		return nil, "", err
	}
	resp := &pb.RpbIndexResp{}
	err = b.client.response(conn, resp)
	if err != nil {
		return nil, "", err
	}
	keys = make([]string, len(resp.Keys))
	for i, v := range resp.Keys {
		keys[i] = string(v)
	}

	next = string(resp.Continuation)
	return
}

// Return a list of keys using the index range query
func (b *Bucket) IndexQueryRange(index string, min string, max string) (keys []string, err error) {
	req := &pb.RpbIndexReq{Bucket: []byte(b.name), Type: []byte(b.bucket_type), Index: []byte(index),
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

// Return a page of keys using the index range query
func (b *Bucket) IndexQueryRangePage(index string, min string, max string, results uint32, continuation string) (keys []string, next string, err error) {
	req := &pb.RpbIndexReq{Bucket: []byte(b.name), Type: []byte(b.bucket_type), Index: []byte(index),
		Qtype:    pb.RpbIndexReq_range.Enum(),
		RangeMin: []byte(min), RangeMax: []byte(max),
		MaxResults: &results}

	if continuation != "" {
		req.Continuation = []byte(continuation)
	}

	err, conn := b.client.request(req, rpbIndexReq)
	if err != nil {
		return nil, "", err
	}
	resp := &pb.RpbIndexResp{}
	err = b.client.response(conn, resp)
	if err != nil {
		return nil, "", err
	}
	keys = make([]string, len(resp.Keys))
	for i, v := range resp.Keys {
		keys[i] = string(v)
	}

	next = string(resp.Continuation)
	return
}

// List all keys from bucket
func (b *Bucket) ListKeys() (response [][]byte, err error) {
	req := &pb.RpbListKeysReq{Bucket: []byte(b.name), Type: []byte(b.bucket_type)}

	err, conn := b.client.request(req, rpbListKeysReq)
	if err != nil {
		return nil, err
	}

	return b.client.mp_response(conn)
}
