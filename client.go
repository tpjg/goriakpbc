/*
Package riak is a riak-client, inspired by the Ruby riak-client gem and the riakpbc go package from mrb.
It implements a connection to Riak using protobuf.
*/
package riak

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"syscall"
	"time"

	"code.google.com/p/goprotobuf/proto"
	"github.com/tpjg/goriakpbc/pb"
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
	connected    bool
	addr         string
	tcpaddr      *net.TCPAddr
	readTimeout  time.Duration
	writeTimeout time.Duration
	conn_count   int
	conns        chan *net.TCPConn
	chanWait     time.Duration
}

/*
Options for storing and retrieving data, only a few are defined, different
values can be supplied by creating a map in the application, for example:
  bucket.Get("key", map[string]int{"r":2})
*/
var (
	R1  = map[string]uint32{"r": 1}
	PR1 = map[string]uint32{"pr": 1}
	W1  = map[string]uint32{"w": 1}
	DW1 = map[string]uint32{"dw": 1}
	PW1 = map[string]uint32{"pw": 1}
)

// Protobuf symbolic quorum integer values
const (
	QuorumOne      = uint32(math.MaxUint32 - 1)
	QuorumMajority = uint32(math.MaxUint32 - 2)
	QuorumAll      = uint32(math.MaxUint32 - 3)
	QuorumDefault  = uint32(math.MaxUint32 - 4)
)

// Error definitions
var (
	BadNumberOfConnections = errors.New("Connection count <= 0")
	BadResponseLength      = errors.New("Response length too short")
	NoBucketName           = errors.New("No bucket name")
	BadMapReduceInputs     = errors.New("MapReduce inputs should be either a (single) index or bucket,key pairs - not both at")
	ChanWaitTimeout        = errors.New("Waiting for an available connection timed out")
)

// Returns a new Client connection
func NewClient(addr string) *Client {
	return &Client{addr: addr, connected: false, readTimeout: 1e8, writeTimeout: 1e8, conn_count: 1}
}

// Returns a new Client connection. DEPRECATED, use NewClient instead
func New(addr string) *Client {
	return NewClient(addr)
}

// Returns a new Client with multiple connections to Riak
func NewClientPool(addr string, count int) *Client {
	return &Client{addr: addr, connected: false, readTimeout: 1e8, writeTimeout: 1e8, conn_count: count}
}

// Returns a new Client with multiple connections to Riak. DEPRECATED, use NewClientPool instead
func NewPool(addr string, count int) *Client {
	return NewClientPool(addr, count)
}

// Connects to a Riak server.
func (c *Client) Connect() error {
	d := new(net.Dialer)
	return c.tcpConnect(d)
}

func (c *Client) ConnectTimeout(timeout time.Duration) error {
	d := &net.Dialer{Timeout: timeout}
	return c.tcpConnect(d)
}

func (c *Client) tcpConnect(dialer *net.Dialer) (err error) {
	tcpaddr, err := net.ResolveTCPAddr("tcp", c.addr)
	if err != nil {
		return err
	}
	c.tcpaddr = tcpaddr

	if c.conn_count <= 0 {
		return BadNumberOfConnections
	} else if !c.connected {
		// Create multiple connections to Riak and send these to the conns channel for later use
		c.conns = make(chan *net.TCPConn, c.conn_count)
		for i := 0; i < c.conn_count; i++ {
			conn, err := dialer.Dial("tcp", tcpaddr.String())
			if err != nil {
				return err
			}
			c.conns <- conn.(*net.TCPConn)
		}
	}
	c.connected = true
	return nil
}

// Close the connection
func (c *Client) Close() {
	if !c.connected {
		return
	}

	// Close all the connections
	for i := 0; i < c.conn_count; i++ {
		conn := <-c.conns
		conn.Close()
	}
	c.connected = false
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
	if c.chanWait > 0 {
		select {
		case conn = <-c.conns:
			break
		case <-time.After(c.chanWait):
			err = ChanWaitTimeout
			break
		}
	} else {
		conn = <-c.conns
	}
	return err, conn
}

// Releases the TCP connection for use by subsequent requests
func (c *Client) releaseConn(conn *net.TCPConn) {
	// Return this connection down the channel for re-use
	c.conns <- conn
}

// Request serializes the data (using protobuf), adds the header and sends it to Riak.
func (c *Client) request(req proto.Message, code byte) (err error, conn *net.TCPConn) {
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
	msgbuf := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i), code}
	msgbuf = append(msgbuf, pbmsg...)
	// Send to Riak
	err = c.write(conn, msgbuf)
	// If an error occurred when sending request
	if err != nil {
		// Make sure connection will be released in the end
		defer c.releaseConn(conn)

		var errno syscall.Errno

		// If the error is not recoverable like a broken pipe, close all connections,
		// so next time when getConn() is called it will Connect() again
		if operr, ok := err.(*net.OpError); ok {
			if errno, ok = operr.Err.(syscall.Errno); ok {
				if errno == syscall.EPIPE {
					c.Close()
				}
			}
		}
	}
	return err, conn
}

// Reponse deserializes the data and returns a struct.
func (c *Client) response(conn *net.TCPConn, response proto.Message) (err error) {
	// Read the response from Riak
	msgbuf, err := c.read(conn, 5)
	if err != nil {
		if err == io.EOF {
			// Connection was closed, try to re-open the connection so subsequent
			// i/o can succeed. Does report the error for this response.
			conn, _ = net.DialTCP("tcp", nil, c.tcpaddr)
		}
		c.releaseConn(conn)
		return err
	}
	defer c.releaseConn(conn)

	// Check the length
	if len(msgbuf) < 5 {
		return BadResponseLength
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
	case rpbErrorResp:
		errResp := &pb.RpbErrorResp{}
		err = proto.Unmarshal(pbmsg, errResp)
		if err == nil {
			err = errors.New(string(errResp.Errmsg))
		}
	case rpbPingResp, rpbSetClientIdResp, rpbSetBucketResp, rpbDelResp:
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
		return nil, BadResponseLength
	}
	// Read the message length, read the rest of the message if necessary
	msglen := int(msgbuf[0])<<24 + int(msgbuf[1])<<16 + int(msgbuf[2])<<8 + int(msgbuf[3])
	pbmsg, err := c.read(conn, msglen-1)
	if err != nil {
		return nil, err
	}

	// Deserialize, by default the calling method should provide the expected RbpXXXResp
	msgcode := msgbuf[4]
	if msgcode == rpbMapRedResp {
		partial := &pb.RpbMapRedResp{}
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
			partial = &pb.RpbMapRedResp{}
			// Read another response
			msgbuf, err = c.read(conn, 5)
			if err != nil {
				return nil, err
			}
			// Check the length
			if len(msgbuf) < 5 {
				return nil, BadResponseLength
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
	} else if msgcode == rpbErrorResp {
		errResp := &pb.RpbErrorResp{}
		err = proto.Unmarshal(pbmsg, errResp)
		if err == nil {
			err = errors.New(string(errResp.Errmsg))
		} else {
			err = fmt.Errorf("Cannot deserialize error response from Riak - %v", err)
		}
		return nil, err
	} else {
		return nil, err
	}
	return
}

// Deserializes the data from possibly multiple packets,
// currently only for pb.RpbListKeysResp.
func (c *Client) mp_response(conn *net.TCPConn) (response [][]byte, err error) {
	defer c.releaseConn(conn)
	var (
		partial *pb.RpbListKeysResp
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
			return nil, BadResponseLength
		}
		// Read the message length, read the rest of the message if necessary
		msglen := int(msgbuf[0])<<24 + int(msgbuf[1])<<16 + int(msgbuf[2])<<8 + int(msgbuf[3])
		pbmsg, err := c.read(conn, msglen-1)
		if err != nil {
			return nil, err
		}

		// Deserialize, by default the calling method should provide the expected RbpXXXResp
		msgcode = msgbuf[4]

		if msgcode == rpbListKeysResp {
			partial = &pb.RpbListKeysResp{}
			err = proto.Unmarshal(pbmsg, partial)
			if err != nil {
				return nil, err
			}

			response = append(response, partial.Keys...)

			if partial.Done != nil {
				break
			}
		} else if msgcode == rpbErrorResp {
			errResp := &pb.RpbErrorResp{}
			err = proto.Unmarshal(pbmsg, errResp)
			if err == nil {
				err = errors.New(string(errResp.Errmsg))
			} else {
				err = fmt.Errorf("Cannot deserialize error response from Riak - %v", err)
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
	msg := []byte{0, 0, 0, 1, rpbPingReq}
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
	msg := []byte{0, 0, 0, 1, rpbGetClientIdReq}
	err, conn := c.getConn()
	if err != nil {
		return id, err
	}
	c.write(conn, msg)
	resp := &pb.RpbGetClientIdResp{}
	err = c.response(conn, resp)
	if err == nil {
		id = string(resp.ClientId)
	}
	return id, err
}

// Set the client Id
func (c *Client) SetId(id string) (err error) {
	req := &pb.RpbSetClientIdReq{ClientId: []byte(id)}
	err, conn := c.request(req, rpbSetClientIdReq)
	if err != nil {
		return err
	}
	err = c.response(conn, req)
	return err
}

// Get the server version
func (c *Client) ServerVersion() (node string, version string, err error) {
	msg := []byte{0, 0, 0, 1, rpbGetServerInfoReq}
	err, conn := c.getConn()
	if err != nil {
		return node, version, err
	}
	c.write(conn, msg)
	resp := &pb.RpbGetServerInfoResp{}
	err = c.response(conn, resp)
	if err == nil {
		node = string(resp.Node)
		version = string(resp.ServerVersion)
	}
	return node, version, err
}

// Set the maximum time to wait for a connection to
// be available in the pool. By default getConn() will wait forever.
func (c *Client) SetChanWaitTimeout(waitTimeout time.Duration) {
	c.chanWait = waitTimeout
}
