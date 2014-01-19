package riak

import (
	"github.com/estebarb/goriakpbc/pb"
)

type Counter struct {
	Bucket  *Bucket
	Key     string
	Value   int64
	Options []map[string]uint32
}

// Reload the value of a counter
func (c *Counter) Reload() (err error) {
	t := true

	req := &pb.RpbCounterGetReq{
		Bucket:     []byte(c.Bucket.name),
		Key:        []byte(c.Key),
		NotfoundOk: &t,
	}

	for _, omap := range c.Options {
		for k, v := range omap {
			switch k {
			case "r":
				req.R = &v
			case "pr":
				req.Pr = &v
			}
		}
	}

	err, conn := c.Bucket.client.request(req, rpbCounterGetReq)
	if err != nil {
		return err
	}

	resp := &pb.RpbCounterGetResp{}
	err = c.Bucket.client.response(conn, resp)
	if err != nil {
		return err
	}

	c.Value = resp.GetValue()

	return nil
}

// Increment a counter by a given amount
func (c *Counter) Increment(amount int64) (err error) {
	return c.increment(amount, false)
}

// Increment a counter by a given amount and reload its value
func (c *Counter) IncrementAndReload(amount int64) (err error) {
	return c.increment(amount, true)
}

// Decrement a counter by a given amount
func (c *Counter) Decrement(amount int64) (err error) {
	return c.increment(-amount, false)
}

// Decrement a counter by a given amount and reload its value
func (c *Counter) DecrementAndReload(amount int64) (err error) {
	return c.increment(-amount, true)
}

func (c *Counter) increment(amount int64, reload bool) (err error) {
	req := &pb.RpbCounterUpdateReq{
		Bucket:      []byte(c.Bucket.name),
		Key:         []byte(c.Key),
		Amount:      &amount,
		Returnvalue: &reload,
	}

	for _, omap := range c.Options {
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

	err, conn := c.Bucket.client.request(req, rpbCounterUpdateReq)
	if err != nil {
		return err
	}

	resp := &pb.RpbCounterUpdateResp{}
	err = c.Bucket.client.response(conn, resp)
	if err != nil {
		return err
	}

	if reload {
		c.Value = resp.GetValue()
	}

	return nil
}

// Destroy the counter
func (c *Counter) Destroy() (err error) {
	all := QuorumAll
	f := false

	req := &pb.RpbDelReq{
		Bucket:       []byte(c.Bucket.name),
		Key:          []byte(c.Key),
		R:            &all,
		W:            &all,
		Pr:           &all,
		Pw:           &all,
		SloppyQuorum: &f,
	}

	for _, omap := range c.Options {
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

	err, conn := c.Bucket.client.request(req, rpbDelReq)
	if err != nil {
		return err
	}
	err = c.Bucket.client.response(conn, req)
	if err != nil {
		return err
	}
	return nil
}

// Get a counter
func (b *Bucket) GetCounter(key string, options ...map[string]uint32) (c *Counter, err error) {
	c = &Counter{
		Bucket:  b,
		Key:     key,
		Options: options,
	}

	err = c.Reload()

	return
}

func (b *Bucket) GetCounterWithoutLoad(key string, options ...map[string]uint32) (c *Counter, err error) {
	c = &Counter{
		Bucket:  b,
		Key:     key,
		Options: options,
	}

	return
}

// Get counter directly from a bucket, without creating a bucket first
func (c *Client) GetCounterFrom(bucketname string, key string, options ...map[string]uint32) (counter *Counter, err error) {
	var bucket *Bucket
	bucket, err = c.Bucket(bucketname)
	if err != nil {
		return
	}
	return bucket.GetCounter(key, options...)
}
