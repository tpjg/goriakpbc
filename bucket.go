package riak

// Implements access to a bucket and its properties
type Bucket struct {
    name      string
    client    *Client
    nval      uint32
    allowMult bool
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
    props := &RpbBucketProps{NVal: &nval, AllowMult: &b.allowMult}
    req := &RpbSetBucketReq{Bucket: []byte(b.name), Props: props}
    err = b.client.request(req, "RpbSetBucketReq")
    if err != nil {
        return err
    }
    err = b.client.response(req)
    if err != nil {
        return err
    }
    b.nval = nval
    return nil
}

// Set the allowMult property of a bucket
func (b *Bucket) SetAllowMult(allowMult bool) (err error) {
    props := &RpbBucketProps{NVal: &b.nval, AllowMult: &allowMult}
    req := &RpbSetBucketReq{Bucket: []byte(b.name), Props: props}
    err = b.client.request(req, "RpbSetBucketReq")
    if err != nil {
        return err
    }
    err = b.client.response(req)
    if err != nil {
        return err
    }
    b.allowMult = allowMult
    return nil
}

// Delete a key/value from the bucket
func (b *Bucket) Delete(key string) (err error) {
    req := &RpbDelReq{Bucket: []byte(b.name), Key: []byte(key)}
    err = b.client.request(req, "RpbDelReq")
    if err != nil {
        return err
    }
    err = b.client.response(req)
    if err != nil {
        return err
    }
    return nil
}

// Create a new RObject
func (b *Bucket) New(key string) *RObject {
    obj := &RObject{Key: key, Bucket: b,
        Links: make([]Link, 0),
        Meta:  make(map[string]string), Indexes: make(map[string]string)}
    return obj
}

// Test if an object exists
func (b *Bucket) Exists(key string) (exists bool, err error) {
    t := true
    req := &RpbGetReq{
        Bucket:     []byte(b.name),
        Key:        []byte(key),
        NotfoundOk: &t,
        Head:       &t}

    err = b.client.request(req, "RpbGetReq")
    if err != nil {
        return false, err
    }
    resp := &RpbGetResp{}
    err = b.client.response(resp)
    if err != nil {
        return false, err
    }
    return len(resp.Content) != 0, nil
}

// Return a list of keys using the index for a single key
func (b *Bucket) IndexQuery(index string, key string) (keys []string, err error) {
    req := &RpbIndexReq{Bucket: []byte(b.name), Index: []byte(index),
        Qtype: RpbIndexReq_eq.Enum(), Key: []byte(key)}
    err = b.client.request(req, "RpbIndexReq")
    if err != nil {
        return nil, err
    }
    resp := &RpbIndexResp{}
    err = b.client.response(resp)
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
    req := &RpbIndexReq{Bucket: []byte(b.name), Index: []byte(index),
        Qtype:    RpbIndexReq_range.Enum(),
        RangeMin: []byte(min), RangeMax: []byte(max)}
    err = b.client.request(req, "RpbIndexReq")
    if err != nil {
        return nil, err
    }
    resp := &RpbIndexResp{}
    err = b.client.response(resp)
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
    req := &RpbListKeysReq{Bucket: []byte(b.name)}

    err = b.client.request(req, "RpbListKeysReq")
    if err != nil {
        return nil, err
    }

    return b.client.mp_response()
}
