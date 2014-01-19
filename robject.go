package riak

import (
	"errors"

	"github.com/estebarb/goriakpbc/pb"
)

// A Riak link
type Link struct {
	Bucket string
	Key    string
	Tag    string
}

// An object van have siblings that can each have their own content
type Sibling struct {
	ContentType  string
	Data         []byte
	Links        []Link
	Meta         map[string]string
	Indexes      map[string]string
	MultiIndexes map[string][]string
	Vtag         string
	LastMod      uint32
	LastModUsecs uint32
}

// An RObject is an object or document that is or can be stored in Riak
type RObject struct {
	Bucket       *Bucket
	Vclock       []byte
	Key          string
	ContentType  string
	Data         []byte
	Links        []Link
	Meta         map[string]string
	Indexes      map[string]string
	MultiIndexes map[string][]string
	conflict     bool
	Siblings     []Sibling
	Options      []map[string]uint32
}

// Error definitions
var (
	NotFound = errors.New("Object not found")
)

// Store an RObject
func (obj *RObject) Store() (err error) {
	// Create base pb.RpbPutReq
	t := true
	req := &pb.RpbPutReq{
		Bucket: []byte(obj.Bucket.name),
		Content: &pb.RpbContent{
			Value:       []byte(obj.Data),
			ContentType: []byte(obj.ContentType),
		},
		ReturnHead: &t,
	}
	if obj.Key != "" {
		req.Key = []byte(obj.Key)
	}
	if len(obj.Vclock) > 0 {
		req.Vclock = obj.Vclock
	}
	// Add the links
	req.Content.Links = make([]*pb.RpbLink, len(obj.Links))
	for i, v := range obj.Links {
		req.Content.Links[i] = &pb.RpbLink{Bucket: []byte(v.Bucket),
			Key: []byte(v.Key),
			Tag: []byte(v.Tag)}
	}
	// Add the user metadata
	req.Content.Usermeta = make([]*pb.RpbPair, len(obj.Meta))
	i := 0
	for k, v := range obj.Meta {
		req.Content.Usermeta[i] = &pb.RpbPair{Key: []byte(k), Value: []byte(v)}
		i += 1
	}
	// Add the indexes
	req.Content.Indexes = make([]*pb.RpbPair, len(obj.Indexes))
	i = 0
	for k, v := range obj.Indexes {
		req.Content.Indexes[i] = &pb.RpbPair{Key: []byte(k), Value: []byte(v)}
		i += 1
	}

	// Add the indexes ("MultiIndexes" support)
	for k, vals := range obj.MultiIndexes {
		for _, v := range vals {
			// Tests if the current index value was duplicated
			// in obj.Indexes
			if obj.Indexes[k] != v {
				req.Content.Indexes = append(req.Content.Indexes,
					&pb.RpbPair{Key: []byte(k), Value: []byte(v)})
			}
		}
	}

	// Add the options
	for _, omap := range obj.Options {
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
	err, conn := obj.Bucket.client.request(req, rpbPutReq)
	if err != nil {
		return err
	}
	// Get response, ReturnHead is true, so we can store the vclock
	resp := &pb.RpbPutResp{}
	err = obj.Bucket.client.response(conn, resp)
	if err != nil {
		return err
	}
	obj.Vclock = resp.Vclock
	// If applicable, store the key
	if obj.Key == "" {
		obj.Key = string(resp.Key)
	}

	return nil
}

// Delete the object from Riak
func (obj *RObject) Destroy() (err error) {
	req := &pb.RpbDelReq{Bucket: []byte(obj.Bucket.name), Key: []byte(obj.Key), Vclock: obj.Vclock}
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

// Returns true if the object was fetched with multiple siblings (AllowMult=true on the bucket)
func (obj *RObject) Conflict() bool {
	return obj.conflict
}

// Sets the values that returned from a pb.RpbGetResp in the RObject
func (obj *RObject) setContent(resp *pb.RpbGetResp) {
	// Check if there are siblings
	if len(resp.Content) > 1 {
		// Mark as conflict, set fields
		obj.conflict = true
		obj.Siblings = make([]Sibling, len(resp.Content))
		for i, content := range resp.Content {
			obj.Siblings[i].ContentType = string(content.ContentType)
			obj.Siblings[i].Data = content.Value
			obj.Siblings[i].Vtag = string(content.Vtag)
			obj.Siblings[i].LastMod = *content.LastMod
			obj.Siblings[i].LastModUsecs = *content.LastModUsecs
			obj.Siblings[i].Links = make([]Link, len(content.Links))
			for j, link := range content.Links {
				obj.Siblings[i].Links[j] = Link{string(link.Bucket),
					string(link.Key),
					string(link.Tag)}
			}
			obj.Siblings[i].Meta = make(map[string]string)
			for _, meta := range content.Usermeta {
				obj.Siblings[i].Meta[string(meta.Key)] = string(meta.Value)
			}

			// Indexes (original)
			obj.Siblings[i].Indexes = make(map[string]string)
			for _, index := range content.Indexes {
				obj.Siblings[i].Indexes[string(index.Key)] = string(index.Value)
			}

			// MultiIndexes support
			obj.Siblings[i].MultiIndexes = make(map[string][]string)
			for _, index := range content.Indexes {
				obj.Siblings[i].MultiIndexes[string(index.Key)] = append(obj.Siblings[i].MultiIndexes[string(index.Key)],
					string(index.Value))
			}
		}
	} else if len(resp.Content) == 1 {
		// No conflict, set the fields in object directly
		obj.conflict = false
		obj.ContentType = string(resp.Content[0].ContentType)
		obj.Data = resp.Content[0].Value
		obj.Links = make([]Link, len(resp.Content[0].Links))
		for j, link := range resp.Content[0].Links {
			obj.Links[j] = Link{string(link.Bucket),
				string(link.Key),
				string(link.Tag)}
		}
		obj.Meta = make(map[string]string)
		for _, meta := range resp.Content[0].Usermeta {
			obj.Meta[string(meta.Key)] = string(meta.Value)
		}

		// Indexes (original api)
		obj.Indexes = make(map[string]string)
		for _, index := range resp.Content[0].Indexes {
			obj.Indexes[string(index.Key)] = string(index.Value)
		}

		// MultiIndexes support
		obj.MultiIndexes = make(map[string][]string)
		for _, index := range resp.Content[0].Indexes {
			obj.MultiIndexes[string(index.Key)] = append(obj.MultiIndexes[string(index.Key)],
				string(index.Value))
		}
	}
}

// Add a link to another object (does not store the object, must explicitly call "Store()")
func (obj *RObject) LinkTo(target *RObject, tag string) {
	if target.Bucket.name != "" && target.Key != "" {
		obj.Links = append(obj.Links, Link{target.Bucket.name, target.Key, tag})
	}
}

// Add a link if it is not already in the Links slics, returns false if already present
func (obj *RObject) AddLink(link Link) bool {
	for _, el := range obj.Links {
		if el.Bucket == link.Bucket && el.Key == link.Key && el.Tag == link.Tag {
			return false
		}
	}
	obj.Links = append(obj.Links, link)
	return true
}

// Get an object
func (b *Bucket) Get(key string, options ...map[string]uint32) (obj *RObject, err error) {
	t := true
	req := &pb.RpbGetReq{
		Bucket:        []byte(b.name),
		Key:           []byte(key),
		NotfoundOk:    &t,
		Deletedvclock: &t,
	}
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
		return nil, err
	}
	resp := &pb.RpbGetResp{}
	err = b.client.response(conn, resp)
	if err != nil {
		return nil, err
	}
	// Create a new object (even if only for storing the returned Vclock)
	obj = &RObject{Key: key, Bucket: b, Vclock: resp.Vclock, Options: options}

	// If no Content is returned then the object was  not found
	if len(resp.Content) == 0 {
		return obj, NotFound
	}
	// Set the fields
	obj.setContent(resp)

	return obj, nil
}

// Get directly from a bucket, without creating a bucket first
func (c *Client) GetFrom(bucketname string, key string, options ...map[string]uint32) (obj *RObject, err error) {
	var bucket *Bucket
	bucket, err = c.Bucket(bucketname)
	if err != nil {
		return
	}
	return bucket.Get(key, options...)
}

// Reload an object if it has changed (new Vclock)
func (obj *RObject) Reload() (err error) {
	req := &pb.RpbGetReq{
		Bucket:     []byte(obj.Bucket.name),
		Key:        []byte(obj.Key),
		IfModified: obj.Vclock}
	for _, omap := range obj.Options {
		for k, v := range omap {
			switch k {
			case "r":
				req.R = &v
			case "pr":
				req.Pr = &v
			}
		}
	}
	err, conn := obj.Bucket.client.request(req, rpbGetReq)
	if err != nil {
		return err
	}
	resp := &pb.RpbGetResp{}
	err = obj.Bucket.client.response(conn, resp)
	if err != nil {
		return err
	}
	if resp.Unchanged != nil && *resp.Unchanged == true {
		return nil
	}
	// Object has new content, reload object
	obj.Vclock = resp.Vclock
	obj.setContent(resp)

	return nil
}
