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
	} else if msgcode == messageCodes["RpbErrorResp"] {
		errResp := &RpbErrorResp{}
		err = proto.Unmarshal(pbmsg, errResp)
		if err == nil {
			err = errors.New(string(errResp.Errmsg))
		} else {
			err = errors.New(string("Cannot deserialize error response from Riak"))
		}
		return nil, err
	} else {
		return nil, err
	}
	return
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
