package riak

import (
	"errors"
)

// A Riak link
type Link struct {
	Bucket string
	Key    string
	Tag    string
}

// An object van have siblings that can each have their own content
type Sibling struct {
	ContentType string
	Data        []byte
	Links       []Link
	Meta        map[string]string
	Indexes     map[string]string
}

// An RObject is an object or document that is or can be stored in Riak
type RObject struct {
	Bucket      *Bucket
	Vclock      []byte
	Key         string
	ContentType string
	Data        []byte
	Links       []Link
	Meta        map[string]string
	Indexes     map[string]string
	conflict    bool
	Siblings    []Sibling
	Options     []map[string]uint32
}

// Store an RObject
func (obj *RObject) Store() (err error) {
	// Create base RpbPutReq
	t := true
	req := &RpbPutReq{
		Bucket: []byte(obj.Bucket.name),
		Content: &RpbContent{
			Value:       []byte(obj.Data),
			ContentType: []byte(obj.ContentType),
		},
		ReturnHead: &t,
	}
	if obj.Key != "" {
		req.Key = []byte(obj.Key)
	}
	// Add the links
	req.Content.Links = make([]*RpbLink, len(obj.Links))
	for i, v := range obj.Links {
		req.Content.Links[i] = &RpbLink{Bucket: []byte(v.Bucket),
			Key: []byte(v.Key),
			Tag: []byte(v.Tag)}
	}
	// Add the user metadata
	req.Content.Usermeta = make([]*RpbPair, len(obj.Meta))
	i := 0
	for k, v := range obj.Meta {
		req.Content.Usermeta[i] = &RpbPair{Key: []byte(k), Value: []byte(v)}
		i += 1
	}
	// Add the indexes
	req.Content.Indexes = make([]*RpbPair, len(obj.Indexes))
	i = 0
	for k, v := range obj.Indexes {
		req.Content.Indexes[i] = &RpbPair{Key: []byte(k), Value: []byte(v)}
		i += 1
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
	err, conn := obj.Bucket.client.request(req, "RpbPutReq")
	if err != nil {
		return err
	}
	// Get response, ReturnHead is true, so we can store the vclock
	resp := &RpbPutResp{}
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
	req := &RpbDelReq{Bucket: []byte(obj.Bucket.name), Key: []byte(obj.Key), Vclock: obj.Vclock}
	err, conn := obj.Bucket.client.request(req, "RpbDelReq")
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

// Sets the values that returned from a RpbGetResp in the RObject
func (obj *RObject) setContent(resp *RpbGetResp) {
	// Check if there are siblings
	if len(resp.Content) > 1 {
		// Mark as conflict, set fields
		obj.conflict = true
		obj.Siblings = make([]Sibling, len(resp.Content))
		for i, content := range resp.Content {
			obj.Siblings[i].ContentType = string(content.ContentType)
			obj.Siblings[i].Data = content.Value
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
			obj.Siblings[i].Indexes = make(map[string]string)
			for _, index := range content.Indexes {
				obj.Siblings[i].Indexes[string(index.Key)] = string(index.Value)
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
		obj.Indexes = make(map[string]string)
		for _, index := range resp.Content[0].Indexes {
			obj.Indexes[string(index.Key)] = string(index.Value)
		}
	}
}

// Add a link to another object (does not store the object, must explicitly call "Store()")
func (obj *RObject) LinkTo(target *RObject, tag string) {
	if target.Bucket.name != "" && target.Key != "" {
		obj.Links = append(obj.Links, Link{target.Bucket.name, target.Key, tag})
	}
}

// Get an object
func (b *Bucket) Get(key string, options ...map[string]uint32) (obj *RObject, err error) {
	req := &RpbGetReq{
		Bucket: []byte(b.name),
		Key:    []byte(key),
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
	err, conn := b.client.request(req, "RpbGetReq")
	if err != nil {
		return nil, err
	}
	resp := &RpbGetResp{}
	err = b.client.response(conn, resp)
	if err != nil {
		return nil, err
	}
	// If no Content is returned then the object was  not found
	if len(resp.Content) == 0 {
		return &RObject{}, errors.New("Object not found")
	}
	// Create a new object and set the fields
	obj = &RObject{Key: key, Bucket: b, Vclock: resp.Vclock, Options: options}
	obj.setContent(resp)

	return obj, nil
}

// Reload an object if it has changed (new Vclock)
func (obj *RObject) Reload() (err error) {
	req := &RpbGetReq{
		Bucket:     []byte(obj.Bucket.name),
		Key:        []byte(obj.Key),
		IfModified: obj.Vclock}
	err, conn := obj.Bucket.client.request(req, "RpbGetReq")
	if err != nil {
		return err
	}
	resp := &RpbGetResp{}
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
