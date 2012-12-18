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
	conn_count   int
	conns        chan *net.TCPConn
}

// Returns a new Client connection
func New(addr string) *Client {
	return &Client{addr: addr, connected: false, readTimeout: 1e8, writeTimeout: 1e8, conn_count: 1}
}

// Returns a new Client with multiple connections to Riak
func NewPool(addr string, count int) *Client {
	return &Client{addr: addr, connected: false, readTimeout: 1e8, writeTimeout: 1e8, conn_count: count}
}

// Connects to a Riak server.
func (c *Client) Connect() (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	tcpaddr, err := net.ResolveTCPAddr("tcp", c.addr)
	if err != nil {
		return err
	}

	if c.conn_count <= 0 {
		return errors.New("Connection count <= 0")
	} else if c.conn_count == 1 {
		c.conn, err = net.DialTCP("tcp", nil, tcpaddr)
		if err != nil {
			return err
		}
	} else {
		// Create multiple connections to Riak and send these to the conns channel for later use
		c.conns = make(chan *net.TCPConn, c.conn_count)
		for i := 0; i < c.conn_count; i++ {
			newconn, err := net.DialTCP("tcp", nil, tcpaddr)
			if err != nil {
				return err
			}
			c.conns <- newconn
		}
	}
	c.connected = true
	return nil
}

// Close the connection
func (c *Client) Close() {
	if c.conn_count <= 0 {
		return
	} else if c.conn_count == 1 {
		c.conn.Close()
	} else {
		// Close all the connections
		for i := 0; i < c.conn_count; i++ {
			conn := <-c.conns
			conn.Close()
		}
	}
	c.conn_count = 0
}

// Write data to the connection
func (c *Client) write(conn *net.TCPConn, request []byte) (err error) {
	_, err = conn.Write(request)

	return err
}

// Read data from the connection
func (c *Client) read(conn *net.TCPConn, size int) (response []byte, err error) {
	response = make([]byte, size)
	s := 0
	for i := 0; (size > 0) && (i < size); {
		s, err = conn.Read(response[i:size])
		i += s
		if err != nil {
			return
		}
	}
	return
}

// Gets the TCP connection for a client (either the only one, or one from the pool)
func (c *Client) getConn() (err error, conn *net.TCPConn) {
	err = nil
	// Connect if necessary
	if !c.connected {
		err = c.Connect()
		if err != nil {
			return err, nil
		}
	}
	// Select a connection to use 
	if c.conn_count == 1 {
		conn = c.conn
		c.mu.Lock() // Lock until the response is read
	} else {
		conn = <-c.conns
	}
	return err, conn
}

// Releases the TCP connection for use by subsequent requests
func (c *Client) releaseConn(conn *net.TCPConn) {
	if c.conn_count == 1 {
		// Unlock the Mutex so the single shared connection can be re-used
		c.mu.Unlock()
	} else {
		// Return this connection down the channel for re-use
		c.conns <- conn
	}
}

// Request serializes the data (using protobuf), adds the header and sends it to Riak.
func (c *Client) request(req proto.Message, name string) (err error, conn *net.TCPConn) {
	err, conn = c.getConn()
	if err != nil {
		return err, nil
	}
	// Serialize the request using protobuf
	pbmsg, err := proto.Marshal(req)
	if err != nil {
		return err, conn
	}
	// Build message with header: <length:32> <msg_code:8> <pbmsg>
	i := int32(len(pbmsg) + 1)
	msgbuf := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i), messageCodes[name]}
	msgbuf = append(msgbuf, pbmsg...)
	// Send to Riak
	err = c.write(conn, msgbuf)

	return err, conn
}

// Reponse deserializes the data and returns a struct.
func (c *Client) response(conn *net.TCPConn, response proto.Message) (err error) {
	defer c.releaseConn(conn)
	// Read the response from Riak
	msgbuf, err := c.read(conn, 5)
	if err != nil {
		return err
	}
	// Check the length
	if len(msgbuf) < 5 {
		return errors.New("Response length too short")
	}
	// Read the message length, read the rest of the message if necessary
	msglen := int(msgbuf[0])<<24 + int(msgbuf[1])<<16 + int(msgbuf[2])<<8 + int(msgbuf[3])
	pbmsg, err := c.read(conn, msglen-1)
	if err != nil {
		return err
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

// Reponse deserializes the data from a MapReduce response and returns the data, 
// this can come from multiple response messages
func (c *Client) mr_response(conn *net.TCPConn) (response [][]byte, err error) {
	defer c.releaseConn(conn)
	// Read the response from Riak
	msgbuf, err := c.read(conn, 5)
	if err != nil {
		return nil, err
	}
	// Check the length
	if len(msgbuf) < 5 {
		return nil, errors.New("Response length too short")
	}
	// Read the message length, read the rest of the message if necessary
	msglen := int(msgbuf[0])<<24 + int(msgbuf[1])<<16 + int(msgbuf[2])<<8 + int(msgbuf[3])
	pbmsg, err := c.read(conn, msglen-1)
	if err != nil {
		return nil, err
	}

	// Deserialize, by default the calling method should provide the expected RbpXXXResp
	msgcode := msgbuf[4]
	if msgcode == messageCodes["RpbMapRedResp"] {
		partial := &RpbMapRedResp{}
		err = proto.Unmarshal(pbmsg, partial)
		if err != nil {
			return nil, err
		}
		done := partial.Done
		var resp [][]byte = nil
		if partial.Response != nil {
			resp = make([][]byte, 1)
			resp[0] = partial.Response
		}

		for done == nil {
			partial = &RpbMapRedResp{}
			// Read another response
			msgbuf, err = c.read(conn, 5)
			if err != nil {
				return nil, err
			}
			// Check the length
			if len(msgbuf) < 5 {
				return nil, errors.New("Response length too short")
			}
			// Read the message length, read the rest of the message if necessary
			msglen := int(msgbuf[0])<<24 + int(msgbuf[1])<<16 + int(msgbuf[2])<<8 + int(msgbuf[3])
			pbmsg, err := c.read(conn, msglen-1)
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

// Deserializes the data from possibly multiple packets, 
// currently only for RpbListKeysResp.
func (c *Client) mp_response(conn *net.TCPConn) (response [][]byte, err error) {
	defer c.releaseConn(conn)
	var (
		partial *RpbListKeysResp
		msgcode byte
	)

	for {
		// Read the response from Riak
		msgbuf, err := c.read(conn, 5)
		if err != nil {
			return nil, err
		}
		// Check the length
		if len(msgbuf) < 5 {
			return nil, errors.New("Response length too short")
		}
		// Read the message length, read the rest of the message if necessary
		msglen := int(msgbuf[0])<<24 + int(msgbuf[1])<<16 + int(msgbuf[2])<<8 + int(msgbuf[3])
		pbmsg, err := c.read(conn, msglen-1)
		if err != nil {
			return nil, err
		}

		// Deserialize, by default the calling method should provide the expected RbpXXXResp
		msgcode = msgbuf[4]

		if msgcode == messageCodes["RpbListKeysResp"] {
			partial = &RpbListKeysResp{}
			err = proto.Unmarshal(pbmsg, partial)
			if err != nil {
				return nil, err
			}

			response = append(response, partial.Keys...)

			if partial.Done != nil {
				break
			}
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
	}

	return
}

// Ping the server
func (c *Client) Ping() (err error) {
	// Use hardcoded request, no need to serialize
	msg := []byte{0, 0, 0, 1, messageCodes["RpbPingReq"]}
	err, conn := c.getConn()
	if err != nil {
		return err
	}
	c.write(conn, msg)
	// Get response and return error if there was one
	err = c.response(conn, nil)

	return err
}

// Get the client Id
func (c *Client) Id() (id string, err error) {
	// Use hardcoded request, no need to serialize
	msg := []byte{0, 0, 0, 1, messageCodes["RpbGetClientIdReq"]}
	err, conn := c.getConn()
	if err != nil {
		return id, err
	}
	c.write(conn, msg)
	resp := &RpbGetClientIdResp{}
	err = c.response(conn, resp)
	if err == nil {
		id = string(resp.ClientId)
	}
	return id, err
}

// Set the client Id
func (c *Client) SetId(id string) (err error) {
	req := &RpbSetClientIdReq{ClientId: []byte(id)}
	err, conn := c.request(req, "RpbSetClientIdReq")
	if err != nil {
		return err
	}
	err = c.response(conn, req)
	return err
}

// Get the server version
func (c *Client) ServerVersion() (node string, version string, err error) {
	msg := []byte{0, 0, 0, 1, messageCodes["RpbGetServerInfoReq"]}
	err, conn := c.getConn()
	if err != nil {
		return node, version, err
	}
	c.write(conn, msg)
	resp := &RpbGetServerInfoResp{}
	err = c.response(conn, resp)
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
	err, conn := c.request(req, "RpbGetBucketReq")
	if err != nil {
		return nil
	}
	resp := &RpbGetBucketResp{}
	err = c.response(conn, resp)
	if err != nil {
		return nil
	}
	bucket := &Bucket{name: name, client: c, nval: *resp.Props.NVal, allowMult: *resp.Props.AllowMult}
	return bucket
}
