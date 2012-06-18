/*
	Package riak is a riak-client, inspired by the Ruby riak-client gem and the riakpbc go package from mrb.
	It implements a connection to Riak using protobuf.
*/
package riak

import (
	"code.google.com/p/goprotobuf/proto"
	"errors"
	"net"
	"sync"
	"time"
)

/*
	To generate the necessary protobuf interface types, do:
	git clone https://github.com/basho/riak_pb.git || pushd riak_pb ; git pull ; popd
	cat riak_pb/src/riak.proto riak_pb/src/riak_kv.proto | grep -v import >riak.proto
	protoc --go_out=. riak.proto

	(or in case we also need search use "cat riak_pb/src/*.proto")
*/

// riak.Client the client interface
type Client struct {
	mu           sync.Mutex
	connected    bool
	conn         *net.TCPConn
	addr         string
	readTimeout  time.Duration
	writeTimeout time.Duration
}

// Implements access to a bucket and its properties
type Bucket struct {
	name      string
	client    *Client
	nval      uint32
	allowMult bool
}

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
}

// Returns a new Client connection
func New(addr string) *Client {
	return &Client{addr: addr, connected: false, readTimeout: 1e8, writeTimeout: 1e8}
}

// Connects to a single riak server.
func (c *Client) Connect() (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	tcpaddr, err := net.ResolveTCPAddr("tcp", c.addr)
	if err != nil {
		return err
	}
	c.conn, err = net.DialTCP("tcp", nil, tcpaddr)
	if err != nil {
		return err
	}
	return nil
}

// Close the connection
func (c *Client) Close() {
	c.conn.Close()
}

// Write data to the connection
func (c *Client) write(request []byte) (err error) {
	// Connect if necessary
	if !c.connected {
		err = c.Connect()
		if err != nil {
			return err
		}
	}
	c.mu.Lock() // Lock until the response is read
	_, err = c.conn.Write(request)

	return err
}

// Read data from the connection
func (c *Client) read(size int) (response []byte, err error) {
	response = make([]byte, size)
	s := 0
	for i := 0; (size > 0) && (i < size); {
		s, err = c.conn.Read(response[i:size])
		i += s
		if err != nil {
			return
		}
	}
	return
}

// Request serializes the data (using protobuf), adds the header and sends it to Riak.
func (c *Client) request(req interface{}, name string) (err error) {
	// Serialize the request using protobuf
	pbmsg, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	// Build message with header: <length:32> <msg_code:8> <pbmsg>
	i := int32(len(pbmsg) + 1)
	msgbuf := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i), messageCodes[name]}
	msgbuf = append(msgbuf, pbmsg...)
	// Send to Riak
	err = c.write(msgbuf)

	return err
}

// Reponse deserializes the data and returns a struct.
func (c *Client) response(response interface{}) (err error) {

	// Read the response from Riak
	msgbuf, err := c.read(5)
	if err != nil {
		return err
	}
	// Check the length
	if len(msgbuf) < 5 {
		return errors.New("Response length too short")
	}
	// Read the message length, read the rest of the message if necessary
	msglen := int(msgbuf[0])<<24 + int(msgbuf[1])<<16 + int(msgbuf[2])<<8 + int(msgbuf[3])
	pbmsg, err := c.read(msglen - 1)
	if err != nil {
		return err
	}
	defer c.mu.Unlock() // Unlock the Mutex after reading the complete response

	// Deserialize, by default the calling method should provide the expected RbpXXXResp
	msgcode := msgbuf[4]
	switch msgcode {
	case messageCodes["RpbErrorResp"]:
		errResp := &RpbErrorResp{}
		err = proto.Unmarshal(pbmsg, errResp)
		if err == nil {
			err = errors.New(string(errResp.Errmsg))
		}
	case messageCodes["RpbPingResp"], messageCodes["RpbSetClientIdResp"],
		messageCodes["RpbSetBucketResp"], messageCodes["RpbDelResp"]:
		return nil
	default:
		err = proto.Unmarshal(pbmsg, response)
	}
	return err
}

// Reponse deserializes the data from a MapReduce response and returns the data, 
// this can come from multiple response messages
func (c *Client) mr_response() (response [][]byte, err error) {

	// Read the response from Riak
	msgbuf, err := c.read(5)
	if err != nil {
		return nil, err
	}
	// Check the length
	if len(msgbuf) < 5 {
		return nil, errors.New("Response length too short")
	}
	// Read the message length, read the rest of the message if necessary
	msglen := int(msgbuf[0])<<24 + int(msgbuf[1])<<16 + int(msgbuf[2])<<8 + int(msgbuf[3])
	pbmsg, err := c.read(msglen - 1)
	if err != nil {
		return nil, err
	}
	defer c.mu.Unlock() // Unlock the Mutex after reading the complete response

	// Deserialize, by default the calling method should provide the expected RbpXXXResp
	msgcode := msgbuf[4]
	if msgcode == messageCodes["RpbMapRedResp"] {
		partial := &RpbMapRedResp{}
		err = proto.Unmarshal(pbmsg, partial)
		if err != nil {
			return nil, err
		}
		done := partial.Done
		resp := make([][]byte, 1)
		resp[0] = partial.Response

		for done == nil {
			partial = &RpbMapRedResp{}
			// Read another response
			msgbuf, err = c.read(5)
			if err != nil {
				return nil, err
			}
			// Check the length
			if len(msgbuf) < 5 {
				return nil, errors.New("Response length too short")
			}
			// Read the message length, read the rest of the message if necessary
			msglen := int(msgbuf[0])<<24 + int(msgbuf[1])<<16 + int(msgbuf[2])<<8 + int(msgbuf[3])
			pbmsg, err := c.read(msglen - 1)
			if err != nil {
				return nil, err
			}
			err = proto.Unmarshal(pbmsg, partial)
			if err != nil {
				return nil, err
			}
			done = partial.Done
			if partial.Response != nil {
				resp = append(resp, partial.Response)
			}
		}
		response = resp
		return
	} else {
		return nil, err
	}
	return
}

// Reponse deserializes the data and returns a struct.
func (c *Client) response_old(response interface{}) (err error) {
	var pbmsg []byte

	// Read the response from Riak
	msgbuf, err := c.read(65536)
	if err != nil {
		return err
	}
	// Check the length
	if len(msgbuf) < 5 {
		return errors.New("Response length too short")
	}
	// Read the message length, read the rest of the message if necessary
	msglen := int(msgbuf[0])<<24 + int(msgbuf[1])<<16 + int(msgbuf[2])<<8 + int(msgbuf[3])
	for len(msgbuf) < (msglen + 4) {
		msgadd, err := c.read(msglen)
		if err != nil {
			return err
		}
		msgbuf = append(msgbuf, msgadd...)
	}
	c.mu.Unlock() // Unlock the Mutex after reading the complete response

	if msglen > 1 {
		pbmsg = msgbuf[5:(msglen + 4)]
	}

	// Deserialize, by default the calling method should provide the expected RbpXXXResp
	msgcode := msgbuf[4]
	switch msgcode {
	case messageCodes["RpbErrorResp"]:
		errResp := &RpbErrorResp{}
		err = proto.Unmarshal(pbmsg, errResp)
		if err == nil {
			err = errors.New(string(errResp.Errmsg))
		}
	case messageCodes["RpbPingResp"], messageCodes["RpbSetClientIdResp"],
		messageCodes["RpbSetBucketResp"], messageCodes["RpbDelResp"]:
		return nil
	default:
		err = proto.Unmarshal(pbmsg, response)
	}
	return err
}

// Ping the server
func (c *Client) Ping() (err error) {
	// Use hardcoded request, no need to serialize
	msg := []byte{0, 0, 0, 1, messageCodes["RpbPingReq"]}
	c.write(msg)
	// Get response and return error if there was one
	err = c.response(nil)

	return err
}

// Get the client Id
func (c *Client) Id() (id string, err error) {
	// Use hardcoded request, no need to serialize
	msg := []byte{0, 0, 0, 1, messageCodes["RpbGetClientIdReq"]}
	c.write(msg)
	resp := &RpbGetClientIdResp{}
	err = c.response(resp)
	if err == nil {
		id = string(resp.ClientId)
	}
	return id, err
}

// Set the client Id
func (c *Client) SetId(id string) (err error) {
	req := &RpbSetClientIdReq{ClientId: []byte(id)}
	err = c.request(req, "RpbSetClientIdReq")
	if err != nil {
		return err
	}
	err = c.response(req)
	return err
}

// Get the server version
func (c *Client) ServerVersion() (node string, version string, err error) {
	msg := []byte{0, 0, 0, 1, messageCodes["RpbGetServerInfoReq"]}
	c.write(msg)
	resp := &RpbGetServerInfoResp{}
	err = c.response(resp)
	if err == nil {
		node = string(resp.Node)
		version = string(resp.ServerVersion)
	}
	return node, version, err
}

// Return a bucket
func (c *Client) Bucket(name string) *Bucket {
	req := &RpbGetBucketReq{
		Bucket: []byte(name),
	}
	err := c.request(req, "RpbGetBucketReq")
	if err != nil {
		return nil
	}
	resp := &RpbGetBucketResp{}
	err = c.response(resp)
	if err != nil {
		return nil
	}
	bucket := &Bucket{name: name, client: c, nval: *resp.Props.NVal, allowMult: *resp.Props.AllowMult}
	return bucket
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

	// Send the request
	err = obj.Bucket.client.request(req, "RpbPutReq")
	if err != nil {
		return err
	}
	// Get response, ReturnHead is true, so we can store the vclock
	resp := &RpbPutResp{}
	err = obj.Bucket.client.response(resp)
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
	err = obj.Bucket.client.request(req, "RpbDelReq")
	if err != nil {
		return err
	}
	err = obj.Bucket.client.response(req)
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
	} else {
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

// Get an object
func (b *Bucket) Get(key string) (obj *RObject, err error) {
	req := &RpbGetReq{
		Bucket: []byte(b.name),
		Key:    []byte(key),
	}
	err = b.client.request(req, "RpbGetReq")
	if err != nil {
		return nil, err
	}
	resp := &RpbGetResp{}
	err = b.client.response(resp)
	if err != nil {
		return nil, err
	}
	// Create a new object and set the fields
	obj = &RObject{Key: key, Bucket: b, Vclock: resp.Vclock}
	obj.setContent(resp)

	return obj, nil
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

// Reload an object if it has changed (new Vclock)
func (obj *RObject) Reload() (err error) {
	req := &RpbGetReq{
		Bucket:     []byte(obj.Bucket.name),
		Key:        []byte(obj.Key),
		IfModified: obj.Vclock}
	err = obj.Bucket.client.request(req, "RpbGetReq")
	if err != nil {
		return err
	}
	resp := &RpbGetResp{}
	err = obj.Bucket.client.response(resp)
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

// Add a link to another object (does not store the object, must explicitly call "Store()")
func (obj *RObject) LinkTo(target *RObject, tag string) {
	if target.Bucket.name != "" && target.Key != "" {
		obj.Links = append(obj.Links, Link{target.Bucket.name, target.Key, tag})
	}
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

// Run a MapReduce query
func (c *Client) MapReduce(mr string) (resp [][]byte, err error) {
	req := &RpbMapRedReq{
		Request:     []byte(mr),
		ContentType: []byte("application/json"),
	}
	err = c.request(req, "RpbMapRedReq")
	if err != nil {
		return nil, err
	}
	resp, err = c.mr_response()
	if err != nil {
		return nil, err
	}
	return
}
